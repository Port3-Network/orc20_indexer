
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


async def handle_inscribe_mint(pgsql: PgsqlHelper, event, content):

    transaction = genarate_transaction_json(event, "inscribe-mint")

    mint_content, _ = parse_mint_content(content)
    if mint_content is None:
        return save_invalid_transaction_task(pgsql, transaction, "invalid mint content")

    token_info = await query_token_by_tick_and_tick_id(pgsql, mint_content["tick"], mint_content["tick_id"], event.block_height)
    if token_info is None:
        return save_invalid_transaction_task(pgsql, transaction, "token not exists")
    transaction["token_id"] = token_info.id

    balance_info = None
    balance_id = f"{event.to}-{token_info.id}"
    balance_infos = await pgsql.get_balance_by_id(balance_id)
    if not balance_infos:
        balance_info = generate_balance_json(event, token_info, balance_id, 0)

    # if amt is float, precision should not exceed dec
    origin_amt = str(content['amt'])
    if "." in origin_amt and len(origin_amt.split(".")[1]) > token_info.dec:
        return save_invalid_transaction_task(pgsql, transaction, "amount precision error", balance_info)

    amount = mint_content["amt"]
    transaction["quantity"] = amount

    # if mint amount id greater than limit, not allowed to mint
    if amount > float(token_info.lim):
        return save_invalid_transaction_task(pgsql, transaction, "amount > limit", balance_info)

    # if end_number is not None, mint is ended, not allowed to mint
    if token_info.end_number is not None:
        return save_invalid_transaction_task(pgsql, transaction, "mint ended", balance_info)

    # if mint amount added to minted amount is greater than max, not allowed to mint
    minted = float(token_info.minted) if token_info.minted is not None else 0
    if minted + amount > float(token_info.max):
        return save_invalid_transaction_task(pgsql, transaction, "exceed max", balance_info)

    tasks = []
    # now the transaction is valid, save transaction info
    transaction["valid"] = True
    tasks.append(asyncio.ensure_future(
        pgsql.save_transaction_info(transaction)
    ))

    # update token info
    token_update_info = update_token_info(event, token_info, minted + amount)
    tasks.append(asyncio.ensure_future(
        pgsql.update_token_info(token_info.id, token_update_info)
    ))

    # update balance of holder
    update_balance_task = await update_to_balance_in_mint(pgsql, event, token_info, amount)
    tasks.extend(update_balance_task)

    return tasks


def update_token_info(event, token_info, minted):
    # update token info
    token_update_info = dict()

    # update token minted amount
    token_update_info["minted"] = minted

    # if start_time is None, it means the first mint, update start_time and start_number
    if token_info.start_time is None:
        token_update_info["start_time"] = event.time
        token_update_info["start_number"] = event.inscription_number

    # if minted == max, it means mint is ended, update end_time and end_number
    if token_update_info["minted"] == float(token_info.max):
        token_update_info["end_time"] = event.time
        token_update_info["end_number"] = event.inscription_number

    return token_update_info
