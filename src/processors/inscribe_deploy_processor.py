import os
import sys
import asyncio

src_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(src_path)

from src.dbs.pgsql_helper import PgsqlHelper  # noqa
from src.parsers.operation_parser import parse_deploy_content  # noqa
from src.processors.common import (genarate_transaction_json,
                                   save_invalid_transaction_task,
                                   generate_balance_json)  # noqa


async def handle_inscribe_deploy(pgsql: PgsqlHelper, event, content):

    transaction = genarate_transaction_json(event, "inscribe-deploy")

    deploy_content, _ = parse_deploy_content(
        content, event.block_height, event.inscription_number)
    if deploy_content is None:
        return save_invalid_transaction_task(pgsql, transaction, "invalid deploy content")

    token_id = f'{deploy_content["tick"]}-{deploy_content["tick_id"]}'
    tokens = await pgsql.get_token_by_id(token_id)
    if tokens:
        return save_invalid_transaction_task(pgsql, transaction, "token exists")

    tasks = []
    # save token info
    token_info = {
        "id": token_id,
        "inscription_id": event.inscription_id,
        "inscription_number": event.inscription_number,
        "deployer": event.to,
        "deploy_time": event.time,
    }
    token_info.update(deploy_content)
    tasks.append(asyncio.ensure_future(
        pgsql.save_token_info(token_info)
    ))

    # save deploy transaction info
    transaction["token_id"] = token_info["id"]
    transaction["valid"] = True
    tasks.append(asyncio.ensure_future(
        pgsql.save_transaction_info(transaction)
    ))

    # save user balance info
    balance_id = f"{event.to}-{token_info['id']}"
    balance_json = generate_balance_json(event, token_info, balance_id, 0)
    tasks.append(asyncio.ensure_future(pgsql.save_balance_info(balance_json)))

    return tasks
