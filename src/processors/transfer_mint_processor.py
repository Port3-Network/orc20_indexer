import os
import sys
import asyncio

src_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(src_path)

from src.dbs.pgsql_helper import PgsqlHelper  # noqa
from src.parsers.operation_parser import parse_mint_content  # noqa
from src.processors.common import (query_token_by_tick_and_tick_id,
                                   genarate_transaction_json,
                                   generate_balance_json,
                                   save_invalid_transaction_task,
                                   update_to_balance_in_mint)  # noqa


async def handle_transfer_mint(pgsql: PgsqlHelper, event, content):

    transaction = genarate_transaction_json(event, "transfer")

    mint_content, _ = parse_mint_content(content)
    if mint_content is None:
        return save_invalid_transaction_task(pgsql, transaction, "parse mint content error")

    transaction["quantity"] = mint_content["amt"]

    token_info = await query_token_by_tick_and_tick_id(pgsql, mint_content["tick"], mint_content["tick_id"])
    if token_info is None:
        return save_invalid_transaction_task(pgsql, transaction, "token not exists")

    transaction["token_id"] = token_info.id

    from_balance_id = f"{event['from']}-{token_info.id}"
    from_balance_infos = await pgsql.get_balance_by_id(from_balance_id)
    if not from_balance_infos:
        from_balance_info = generate_balance_json(
            event, token_info, from_balance_id, 0)
        return save_invalid_transaction_task(pgsql, transaction, "mint inscription is invalid", from_balance_info)

    # check if the mint inscription can be transferred
    from_balance_info = from_balance_infos[0]
    received_mint_pool = from_balance_info.received_mint_pool
    target_mint_transaction = None
    for mint_transaction in received_mint_pool:
        if mint_transaction["inscription_id"] == event.inscription_id:
            target_mint_transaction = mint_transaction
            break

    if target_mint_transaction is None:
        return save_invalid_transaction_task(pgsql, transaction, "mint inscription is invalid")

    tasks = []
    # save the transfer transaction
    transaction["valid"] = True
    tasks = [asyncio.ensure_future(
        pgsql.save_transaction_info(transaction)
    )]

    # if the sender is the receiver, do nothing
    if transaction["from"] == transaction["to"]:
        return tasks

    # update sender balance info
    received_mint_pool.remove(target_mint_transaction)
    sender_balance = float(from_balance_info.balance) - mint_content["amt"]
    sender_update_info = {
        "balance": sender_balance,
        "received_mint_pool": received_mint_pool
    }
    tasks.append(asyncio.create_task(
        pgsql.update_balance_info(from_balance_id, sender_update_info)
    ))

    # update receiver balance info
    update_to_task = await update_to_balance_in_mint(
        pgsql, event, token_info, mint_content["amt"], target_mint_transaction)
    tasks.extend(update_to_task)

    return tasks
