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
                                   update_to_balance_in_send,
                                   generate_pool_send_json)  # noqa


async def handle_inscribe_send(pgsql: PgsqlHelper, event, content):

    transaction = genarate_transaction_json(event, "inscribe-send")

    send_content, _ = parse_send_content(content)
    if send_content is None:
        return save_invalid_transaction_task(pgsql, transaction, "invalid send content")

    transaction["quantity"] = send_content.get("amt", 0)

    token_info = await query_token_by_tick_and_tick_id(pgsql, send_content["tick"], send_content["tick_id"], event.block_height)
    if token_info is None:
        return save_invalid_transaction_task(pgsql, transaction, "token not exists")

    transaction["token_id"] = token_info.id

    is_inscribe_remaining = "amt" not in send_content
    transaction_method = "inscribe-remaining" if is_inscribe_remaining else "inscribe-send"
    transaction["method"] = transaction_method

    balance_id = f"{event.to}-{token_info.id}"
    balance_infos = await pgsql.get_balance_by_id(balance_id)
    if not balance_infos:
        balance_info = generate_balance_json(event, token_info, balance_id, 0)
    else:
        balance_info = balance_infos[0]

    # if the nonce is repeated, then the transaction is invalid
    if "pending_send_pool" in balance_info and \
            send_content["n"] in [item["nonce"] for item in balance_info["pending_send_pool"]]:
        return save_invalid_transaction_task(
            pgsql, transaction, "repeated nonce", balance_info)

    if not is_inscribe_remaining:
        return handle_send_transaction(
            pgsql, event, send_content, balance_info, transaction, token_info.id)

    return await handle_remaining_transaction(pgsql, event, send_content, balance_info, transaction)


def handle_send_transaction(pgsql: PgsqlHelper, event, send_content, balance_info, transaction, token_id):

    tasks = []
    transaction["valid"] = True

    # save inscribe send transaction
    tasks.append(asyncio.ensure_future(
        pgsql.save_transaction_info(transaction)))

    # add inscribe send transaction to pending pool and wait for inscribe remaining transaction
    if "pending_send_pool" not in balance_info:
        balance_info["pending_send_pool"] = [
            generate_pool_send_json(event, send_content)]
        tasks.append(asyncio.ensure_future(
            pgsql.save_balance_info(balance_info)
        ))
    else:
        pending_send_pool = balance_info["pending_send_pool"]
        pending_send_pool.append(generate_pool_send_json(event, send_content))
        tasks.append(asyncio.ensure_future(
            pgsql.update_balance_info(
                balance_info.id, {"pending_send_pool": pending_send_pool}
            )
        ))

    return tasks


async def handle_remaining_transaction(pgsql: PgsqlHelper, event, send_content, balance_info, transaction):

    if "pending_send_pool" not in balance_info:
        return save_invalid_transaction_task(pgsql, transaction, "no pending send", balance_info)

    pending_send_pool = balance_info["pending_send_pool"]
    if len(pending_send_pool) == 0:
        return save_invalid_transaction_task(pgsql, transaction, "no pending send")

    total_balance = float(balance_info.balance)
    pending_send_balance = sum([item["amt"] for item in pending_send_pool])

    tasks = []
    if pending_send_balance > total_balance:
        # set all pending send to invalid
        for item in pending_send_pool:
            pending_transactions = await pgsql.get_transaction_by_id(item["transaction_id"])
            if not pending_transactions:
                continue
            pending_transaction = pending_transactions[0]
            tasks.append(asyncio.ensure_future(
                pgsql.update_transaction_info(
                    pending_transaction.id, {"valid": False, "invalid_reason": "insufficient balance"})
            ))

        # the current transaction is invalid
        tasks.extend(save_invalid_transaction_task(
            pgsql, transaction, "insufficient available balance"))

        # set pending send pool and sent pool to empty
        tasks.append(asyncio.ensure_future(pgsql.update_balance_info(balance_info.id, {
            "pending_send_pool": [],
            "sent_send_pool": [],
        })))

        return tasks

    # save inscribe remaining transaction
    remaining_balance = total_balance - pending_send_balance
    transaction["valid"] = True
    transaction["quantity"] = remaining_balance
    tasks.append(asyncio.ensure_future(
        pgsql.save_transaction_info(transaction)
    ))

    send_content["amt"] = remaining_balance
    tasks.extend(await handle_pending_send(pgsql, event, send_content, balance_info))

    return tasks


async def handle_pending_send(pgsql, event, send_content, balance_info):

    tasks = []

    pending_send_pool = balance_info.pending_send_pool
    available_send_pool = balance_info.available_send_pool
    sent_send_pool = balance_info.sent_send_pool
    balance = float(balance_info.balance)

    # the transaction is completed, set all available send or mint to invalid
    for item in available_send_pool:
        transactions = await pgsql.get_transaction_by_id(item["transaction_id"])
        if not transactions:
            continue
        transaction = transactions[0]
        tasks.append(asyncio.ensure_future(pgsql.update_transaction_info(transaction.id, {
            "valid": False,
            "invalid_reason": "not sent before new transaction"
        })))

    # set the remaining to available send pool, and make it sendable
    available_send_pool = [generate_pool_send_json(event, send_content)]

    # move the pending send which has not been sent out to available send pool
    sent_ins_list = [item["inscription_id"] for item in sent_send_pool]
    for item in pending_send_pool:
        if item["inscription_id"] in sent_ins_list:
            continue
        available_send_pool.append(item)

    # make the sent send pool transaction valid
    for item in sent_send_pool:
        amount = item["amt"]

        transactions = await pgsql.get_transaction_by_id(item["transaction_id"])
        if not transactions:
            continue
        transaction = transactions[0]

        if transaction["from"] != transaction["to"]:
            balance -= amount

        token_infos = await pgsql.get_token_by_id(transaction.token_id)
        if not token_infos:
            continue
        token_info = token_infos[0]

        # add the amount to the balance of the receiver
        update_balance_task = await update_to_balance_in_send(
            pgsql, transaction, token_info, amount, item
        )
        tasks.extend(update_balance_task)

        # make the transaction valid
        tasks.append(asyncio.ensure_future(
            pgsql.update_transaction_info(transaction.id, {"valid": True})
        ))

    # update the balance info
    tasks.append(asyncio.ensure_future(pgsql.update_balance_info(balance_info.id, {
        "balance": balance,
        "pending_send_pool": [],
        "sent_send_pool": [],
        "received_mint_pool": [],
        "received_send_pool": [],
        "available_send_pool": available_send_pool
    })))

    return tasks
