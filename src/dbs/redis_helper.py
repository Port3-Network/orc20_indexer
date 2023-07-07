import asyncio
import aioredis
from environs import Env


class RedisHelper:

    def __init__(self, logger, db_version="A"):

        self.logger = logger
        self.semaphore = asyncio.Semaphore(1000)

        env = Env()
        env.read_env()
        redis_url = env.str("ORC_REDIS_URL")

        self.pool = aioredis.ConnectionPool.from_url(
            redis_url, decode_responses=True
        )
        self.redis = aioredis.Redis(
            connection_pool=self.pool,  health_check_interval=30)

        self.redis_key_output = "event_output"
        self.redis_key_current_block = "current_block"
        self.redis_key_handled_event = f"indexer_handled_event_{db_version}"

    async def close(self):
        await self.pool.disconnect()

    async def get_output_from_redis(self, output_id):
        async with self.semaphore:
            try:
                return await self.redis.hget(self.redis_key_output, output_id)
            except Exception as e:
                self.logger.error(f"get output from redis error: {e}")
                return False

    async def get_current_block_from_redis(self):
        async with self.semaphore:
            try:
                return await self.redis.get(self.redis_key_current_block)
            except Exception as e:
                self.logger.error(f"get current block from redis error: {e}")
                return False

    async def set_handled_event(self, event_id):
        async with self.semaphore:
            try:
                await self.redis.hset(self.redis_key_handled_event, event_id, "")
                return True
            except Exception as e:
                self.logger.error(f"set handled event error: {e}")
                return False

    async def is_event_exists(self, event_id):
        async with self.semaphore:
            try:
                return await self.redis.hexists(self.redis_key_handled_event, event_id)
            except Exception as e:
                self.logger.error(f"query exist of handled event error: {e}")
                return False

    async def del_handled_event_db(self):
        async with self.semaphore:
            try:
                return await self.redis.delete(self.redis_key_handled_event)
            except Exception as e:
                self.logger.error(f"del handled event db error: {e}")
                return False
