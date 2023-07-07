import os
import sys
from loguru import logger

src_path = os.path.dirname(os.path.dirname(os.path.dirname(__file__)))
sys.path.append(src_path)

from src.dbs.redis_helper import RedisHelper  # noqa
from src.dbs.pgsql_helper import PgsqlHelper  # noqa


logger.add(
    f"./logs/{os.path.basename(__file__).split('.')[0]}.log",
    level="INFO",
    rotation="500 MB",
    enqueue=True
)


async def clean_db(db_version="a"):
    pgsql = PgsqlHelper(logger, db_version)
    redis = RedisHelper(logger, db_version)

    await pgsql.init()

    await redis.del_handled_event_db()
    await pgsql.create_token_table()
    await pgsql.create_balance_table()
    await pgsql.create_transaction_table()

    await redis.close()
    await pgsql.close()


if __name__ == '__main__':
    import asyncio
    asyncio.run(clean_db())
