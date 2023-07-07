

import os
import sys
import asyncio

src_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(src_path)

from src.dbs.pgsql_helper import PgsqlHelper  # noqa
from src.parsers.operation_parser import parse_upgrade_content, parse_upgrade_tick  # noqa
from src.processors.common import query_token_by_tick_and_tick_id  # noqa
from src.processors.common import (genarate_transaction_json,
                                   save_invalid_transaction_task,
                                   generate_balance_json)  # noqa


async def handle_inscribe_upgrade(pgsql: PgsqlHelper, event, content):

    transaction = genarate_transaction_json(event, "inscribe-upgrade")

    upgrade_tick_content, _ = parse_upgrade_tick(content)
    if upgrade_tick_content is None:
        return save_invalid_transaction_task(pgsql, transaction, "invalid upgrade content")

    token_info = await query_token_by_tick_and_tick_id(pgsql, upgrade_tick_content["tick"], upgrade_tick_content["tick_id"], event.block_height)
    if token_info is None:
        return save_invalid_transaction_task(pgsql, transaction, "token not exists")

    balance_id = f"{event.to}-{token_info.id}"
    balance_info = generate_balance_json(event, token_info, balance_id, 0)

    # if token is not upgradable, not allowed to upgrade
    if token_info.ug is False:
        return save_invalid_transaction_task(pgsql, transaction, "token is not upgradable", balance_info)

    # only deployer can upgrade
    if event.to != token_info.deployer:
        return save_invalid_transaction_task(pgsql, transaction, "only deployer can upgrade", balance_info)

    # parse upgrade content
    upgrade_content, _ = parse_upgrade_content(content, token_info.dec)
    if upgrade_content is None:
        return save_invalid_transaction_task(pgsql, transaction, "invalid upgrade content", balance_info)

    # can not set max less than minted amount
    if "max" in upgrade_content and \
        token_info.minted is not None and \
            float(upgrade_content["max"]) < float(token_info.minted):
        return save_invalid_transaction_task(pgsql, transaction, "can not set max less than minted", balance_info)

    tasks = []
    # add upgrade info to pending list, wait for transfering to confirm
    upgrade_pending_list = token_info.upgrade_pending
    upgrade_transaction = {
        "inscription_index": event.id,
        "inscription_time": event.time,
        "inscription_block_height": event.block_height,
        "inscription_id": event.inscription_id,
        "inscription_number": event.inscription_number,
        "content": upgrade_content
    }
    upgrade_pending_list.append(upgrade_transaction)
    tasks.append(asyncio.ensure_future(
        pgsql.update_token_info(
            token_info.id, {"upgrade_pending": upgrade_pending_list})
    ))

    # save transaction info
    transaction["valid"] = True
    transaction["token_id"] = token_info["id"]
    tasks.append(asyncio.ensure_future(
        pgsql.save_transaction_info(transaction)
    ))

    # save user balance info if user not exists
    tasks.append(asyncio.ensure_future(
        pgsql.save_balance_info(balance_info)
    ))

    return tasks
