import os
import sys
import asyncio

src_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(src_path)

from src.dbs.pgsql_helper import PgsqlHelper  # noqa
from src.parsers.operation_parser import parse_send_content  # noqa
from src.processors.common import (query_token_by_tick_and_tick_id,
                                   genarate_transaction_json,
                                   generate_balance_json,
                                   save_invalid_transaction_task,
                                   update_to_balance_in_send)  # noqa


async def handle_transfer_send(pgsql: PgsqlHelper, event, content):

    transaction = genarate_transaction_json(event, "transfer")

    send_content, _ = parse_send_content(content)
    if send_content is None:
        return save_invalid_transaction_task(pgsql, transaction, "parse send content error")
    if "amt" in send_content:
        transaction["quantity"] = send_content["amt"]

    token_info = await query_token_by_tick_and_tick_id(
        pgsql, send_content["tick"], send_content["tick_id"]
    )
    if token_info is None:
        return save_invalid_transaction_task(pgsql, transaction, "token not exists")

    transaction["token_id"] = token_info.id

    from_balance_id = f"{event['from']}-{token_info.id}"
    from_balance_infos = await pgsql.get_balance_by_id(from_balance_id)
    if not from_balance_infos:
        from_balance_info = generate_balance_json(
            event, token_info, from_balance_id, 0)
        return save_invalid_transaction_task(
            pgsql, transaction, "send inscription is invalid", from_balance_info
        )

    # check if the send inscription can be transferred
    from_balance_info = from_balance_infos[0]
    received_send_pool = from_balance_info.received_send_pool
    available_send_pool = from_balance_info.available_send_pool
    pending_send_pool = from_balance_info.pending_send_pool

    send_source = None
    target_send_transaction = None

    # find the send transaction from different pool
    for send_transaction in received_send_pool:
        if send_transaction["inscription_id"] == event.inscription_id:
            send_source = "received"
            target_send_transaction = send_transaction
            break

    if target_send_transaction is None:
        for send_transaction in available_send_pool:
            if send_transaction["inscription_id"] == event.inscription_id:
                send_source = "available"
                target_send_transaction = send_transaction
                break

    if target_send_transaction is None:
        for send_transaction in pending_send_pool:
            if send_transaction["inscription_id"] == event.inscription_id:
                send_source = "pending"
                target_send_transaction = send_transaction
                break

    if target_send_transaction is None:
        return save_invalid_transaction_task(
            pgsql, transaction, "send inscription is invalid")

    tasks = []
    transaction["quantity"] = target_send_transaction["amt"]

    # if the send inscription is not completed, wait for remaining inscription to complete
    if send_source == "pending":
        sent_send_pool = from_balance_info.sent_send_pool
        target_send_transaction["transaction_id"] = event.id
        sent_send_pool.append(target_send_transaction)

        tasks.append(asyncio.create_task(
            pgsql.update_balance_info(
                from_balance_id,
                {"sent_send_pool": sent_send_pool}
            )))

        tasks.extend(
            save_invalid_transaction_task(
                pgsql, transaction,
                "transaction is not completed, wait for remaining inscription to complete"
            ))

        return tasks

    # save the transfer transaction
    transaction["valid"] = True
    tasks.append(asyncio.create_task(
        pgsql.save_transaction_info(transaction)
    ))

    # update sender info
    sender_update_info = dict()

    # if the send inscription is from available pool, just remove it
    if send_source == "available":
        available_send_pool.remove(target_send_transaction)
        sender_update_info["available_send_pool"] = available_send_pool

    # if the send inscription is from received pool, just remove it
    elif send_source == "received":
        received_send_pool.remove(target_send_transaction)
        sender_update_info["received_send_pool"] = received_send_pool

    # update balance
    sender_balance = float(from_balance_info.balance)
    if event["from"] != event["to"]:
        sender_balance -= target_send_transaction["amt"]
    sender_update_info["balance"] = sender_balance

    tasks.append(asyncio.create_task(
        pgsql.update_balance_info(from_balance_id, sender_update_info)
    ))

    # update receiver info
    update_to_task = await update_to_balance_in_send(
        pgsql, event, token_info, target_send_transaction["amt"], target_send_transaction
    )
    tasks.extend(update_to_task)

    return tasks
