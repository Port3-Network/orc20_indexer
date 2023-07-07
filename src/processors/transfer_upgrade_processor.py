from src.processors.common import query_token_by_tick_and_tick_id
import os
import sys
import asyncio
import copy

src_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(src_path)

from src.dbs.pgsql_helper import PgsqlHelper  # noqa
from src.parsers.operation_parser import parse_upgrade_tick  # noqa
from src.processors.common import (genarate_transaction_json,
                                   save_invalid_transaction_task)  # noqa


UPGRADE_RECEIVER_ADDRESS = "bc1pgha2vs4m4d70aw82qzrhmg98yea4fuxtnf7lpguez3z9cjtukpssrhakhl"


async def handle_transfer_upgrade(pgsql: PgsqlHelper, event, content):

    tasks = []

    transaction = genarate_transaction_json(event, "transfer-upgrade")

    upgrade_tick_content, _ = parse_upgrade_tick(content)
    if upgrade_tick_content is None:
        return save_invalid_transaction_task(pgsql, transaction, "invalid upgrade content")

    token_info = await query_token_by_tick_and_tick_id(pgsql, upgrade_tick_content["tick"], upgrade_tick_content["tick_id"])
    if token_info is None:
        return save_invalid_transaction_task(pgsql, transaction, "token not exists")

    upgrade_pending_list = []
    upgrade_transaction = None
    for upgrade_pending in token_info.upgrade_pending:
        if upgrade_pending["inscription_id"] == event.inscription_id:
            upgrade_transaction = upgrade_pending
            continue
        upgrade_pending_list.append(upgrade_pending)

    if upgrade_transaction is None:
        return save_invalid_transaction_task(pgsql, transaction, "invalid upgrade inscription")

    # Only deployer can transfer upgrade inscription to upgrade receiver address
    if event["from"] != token_info.deployer or event["to"] != UPGRADE_RECEIVER_ADDRESS:
        return save_invalid_transaction_task(pgsql, transaction, "only deployer can transfer upgrade")

    upgrade_transaction["effective_index"] = event.id
    upgrade_transaction["effective_time"] = event.time
    upgrade_transaction["effective_block_height"] = event.block_height

    upgrade_history_list = token_info.upgrade_history
    upgrade_history_list.append(upgrade_transaction)

    update_content = copy.deepcopy(upgrade_transaction["content"])
    update_content["upgrade_history"] = upgrade_history_list
    update_content["upgrade_pending"] = upgrade_pending_list
    update_content["upgrade_time"] = event.time

    tasks.append(asyncio.ensure_future(
        pgsql.update_token_info(
            token_info.id, update_content)
    ))

    transaction["token_id"] = token_info["id"]
    transaction["valid"] = True
    tasks.append(asyncio.ensure_future(
        pgsql.save_transaction_info(transaction)
    ))

    return tasks
