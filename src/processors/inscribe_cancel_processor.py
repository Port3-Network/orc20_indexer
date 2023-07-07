import os
import sys
import asyncio

src_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(src_path)

from src.dbs.pgsql_helper import PgsqlHelper  # noqa
from src.parsers.operation_parser import parse_cancel_content  # noqa
from src.processors.common import (query_token_by_tick_and_tick_id,
                                   generate_balance_json,
                                   genarate_transaction_json,
                                   save_invalid_transaction_task)  # noqa


async def handle_inscribe_cancel(pgsql: PgsqlHelper, event, content):

    transaction = genarate_transaction_json(event, "inscribe-cancel")

    cancel_content, _ = parse_cancel_content(content)
    if cancel_content is None:
        return save_invalid_transaction_task(pgsql, transaction, "invalid cancel content")

    token_info = await query_token_by_tick_and_tick_id(pgsql, cancel_content["tick"], cancel_content["tick_id"], event.block_height)
    if token_info is None:
        return save_invalid_transaction_task(pgsql, transaction, "token not exists")

    transaction["token_id"] = token_info.id

    balance_id = f"{event.to}-{token_info.id}"
    balance_infos = await pgsql.get_balance_by_id(balance_id)
    if not balance_infos:
        balance_info = generate_balance_json(event, token_info, balance_id, 0)
        return save_invalid_transaction_task(pgsql, transaction, "no pending send to cancel", balance_info)

    balance_info = balance_infos[0]
    pending_send_pool = balance_info.pending_send_pool
    sent_send_pool = balance_info.sent_send_pool

    # check if the nonce to cancel is in pending_send_pool or sent_send_pool
    canceled_send = []
    for nonce in cancel_content["n"]:
        for item in pending_send_pool:
            if item["nonce"] != nonce:
                continue
            pending_send_pool.remove(item)
            canceled_send.append(item)
            break
        for item in sent_send_pool:
            if item["nonce"] != nonce:
                continue
            sent_send_pool.remove(item)
            canceled_send.append(item)
            break

    # if the nonce to cancel is not fount, the cancel is invalid
    if len(canceled_send) == 0:
        return save_invalid_transaction_task(pgsql, transaction, "cancel nonce not found")

    tasks = []
    # save transaction info
    transaction["valid"] = True
    tasks.append(asyncio.ensure_future(
        pgsql.save_transaction_info(transaction)
    ))

    # update balance info
    tasks.append(asyncio.ensure_future(
        pgsql.update_balance_info(balance_id, {
            "pending_send_pool": pending_send_pool,
            "sent_send_pool": sent_send_pool
        })
    ))

    # set canceled transaction to invalid
    for item in canceled_send:
        transactions = await pgsql.get_transaction_by_id(item["transaction_id"])
        if not transactions:
            continue
        tasks.append(asyncio.ensure_future(
            pgsql.update_transaction_info(transactions[0].id, {
                "valid": False,
                "invalid_reason": "canceled by inscribe-cancel: {}".format(event.inscription_id)
            })
        ))

    return tasks
