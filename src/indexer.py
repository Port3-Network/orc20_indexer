import os
import sys
import time
import signal
import asyncio
from loguru import logger

src_path = os.path.dirname(os.path.dirname(__file__))
sys.path.append(src_path)

from src.dbs.pgsql_helper import PgsqlHelper  # noqa
from src.dbs.redis_helper import RedisHelper  # noqa
from src.parsers.operation_parser import *  # noqa
from src.processors.inscribe_deploy_processor import handle_inscribe_deploy  # noqa
from src.processors.inscribe_upgrade_processor import handle_inscribe_upgrade  # noqa
from src.processors.inscribe_mint_processor import handle_inscribe_mint  # noqa
from src.processors.inscribe_cancel_processor import handle_inscribe_cancel  # noqa
from src.processors.inscribe_send_processor import handle_inscribe_send  # noqa
from src.processors.transfer_mint_processor import handle_transfer_mint  # noqa
from src.processors.transfer_send_processor import handle_transfer_send  # noqa
from src.processors.transfer_upgrade_processor import handle_transfer_upgrade  # noqa
from src.utils.orc20 import is_orc20  # noqa


class Indexer:
    def __init__(self, db_version="A"):
        self.set_signal()

        logger.add(
            f"./logs/{os.path.basename(__file__).split('.')[0]}.log",
            level="INFO",
            rotation="500 MB",
            enqueue=True
        )

        self.logger = logger
        self.stop_flag = False
        self.orc20_start_height = 787606

        self.pgsql = PgsqlHelper(self.logger, db_version)
        self.redis = RedisHelper(self.logger, db_version)

    def set_signal(self):
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGHUP, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def stop(self, signal_num, frame):
        if self.stop_flag == True:
            sys.exit(0)
        self.stop_flag = True

    async def init(self):
        await self.pgsql.init()

    async def close(self):
        await self.redis.close()
        await self.pgsql.close()

    async def handle_inscribe_event(self, event, content):

        tasks = []
        operation = content["op"].lower()
        if operation == "deploy":
            tasks = await handle_inscribe_deploy(self.pgsql, event, content)
        elif operation == "mint":
            tasks = await handle_inscribe_mint(self.pgsql, event, content)
        elif operation == "send" or operation == "transfer":
            tasks = await handle_inscribe_send(self.pgsql, event, content)
        elif operation == "cancel":
            tasks = await handle_inscribe_cancel(self.pgsql, event, content)
        elif operation == "upgrade":
            tasks = await handle_inscribe_upgrade(self.pgsql, event, content)

        try:
            result = await asyncio.gather(*tasks)
            if False in result:
                return False
        except Exception as e:
            return False

        return True

    async def handle_transfer_event(self, event, content):

        tasks = []
        operation = content["op"].lower()
        if operation == "mint":
            tasks = await handle_transfer_mint(self.pgsql, event, content)
        elif operation == "send" or operation == "transfer":
            tasks = await handle_transfer_send(self.pgsql, event, content)
        elif operation == "upgrade":
            tasks = await handle_transfer_upgrade(self.pgsql, event, content)

        result = await asyncio.gather(*tasks)
        if False in result:
            return False

        return True

    async def handle_block(self, block_height):
        start = time.time()
        events = await self.pgsql.get_event_by_block_height(block_height)
        if not events:
            return True

        for event in events:

            if await self.redis.is_event_exists(event.id):
                continue

            content = is_orc20(event.content)
            if content is None:
                continue

            if event.event == "inscribe":
                success = await self.handle_inscribe_event(event, content)
                if success is False:
                    return False
            elif event.event == "transfer":
                success = await self.handle_transfer_event(event, content)
                if success is False:
                    return False

            await self.redis.set_handled_event(event.id)

        end = time.time()
        self.logger.info(
            f"handle block: {block_height} successfully, events: {len(events)}, cost: {end-start} s")

        return True

    async def run(self, start_height=None):
        await self.init()

        current_block_height = start_height if start_height is not None else self.orc20_start_height
        while not self.stop_flag:

            latest_block_height = await self.redis.get_current_block_from_redis()
            while not self.stop_flag and \
                    current_block_height <= int(latest_block_height):

                success = await self.handle_block(current_block_height)
                if success is False:
                    self.logger.error(
                        f"handle block: {current_block_height} failed")
                    break
                current_block_height += 1

            if not self.stop_flag:
                await asyncio.sleep(60)

        await self.close()


if __name__ == '__main__':

    indexer = Indexer()
    asyncio.run(indexer.run())
