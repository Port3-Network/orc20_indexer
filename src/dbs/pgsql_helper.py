import asyncio
import sqlalchemy as sa
from sqlalchemy.sql.ddl import CreateTable
from sqlalchemy.dialects.postgresql import ENUM, JSON, ARRAY
from sqlalchemy.dialects.postgresql import insert
from aiopg.sa import create_engine
from environs import Env


class PgsqlHelper:

    def __init__(self, logger, db_version="A") -> None:
        self.logger = logger
        self.db_version = db_version
        metadata = sa.MetaData()
        self.semaphore = asyncio.Semaphore(100)

        self.inscription = sa.Table(
            "inscription",
            metadata,
            sa.Column("id", sa.String(255), primary_key=True, unique=True),
            sa.Column("inscription_number", sa.BigInteger),
            sa.Column("address", sa.String(255)),
            sa.Column("output_value", sa.BigInteger),
            sa.Column("sat", sa.BigInteger),
            sa.Column("content_length", sa.String(65)),
            sa.Column("content_type", sa.String(65)),
            sa.Column("timestamp", sa.String(65)),
            sa.Column("genesis_height", sa.Integer),
            sa.Column("genesis_fee", sa.BigInteger),
            sa.Column("genesis_transaction", sa.String(65)),
            sa.Column("content", JSON),
        )

        self.event = sa.Table(
            "event",
            metadata,
            sa.Column("id", sa.BigInteger, primary_key=True),
            sa.Column("inscription_id", sa.String(255)),
            sa.Column("inscription_number", sa.BigInteger),
            sa.Column("block_height", sa.BigInteger),
            sa.Column("event", ENUM(
                "transfer", "inscribe", name="event_enum")),
            sa.Column("from", sa.String(255)),
            sa.Column("to", sa.String(255)),
            sa.Column("time", sa.String(65)),
            sa.Column("value", sa.BigInteger),
            sa.Column("content", JSON),
            sa.Column("spent", sa.Boolean),
        )

        self.token = sa.Table(
            f"token_{db_version}",
            metadata,
            # tick-inscription_number | tick-tick_id
            sa.Column("id", sa.String, primary_key=True),
            sa.Column("tick", sa.String),
            sa.Column("tick_id", sa.String),
            sa.Column("name", sa.String),
            sa.Column("max", sa.String),
            sa.Column("lim", sa.String),
            sa.Column("dec", sa.Integer),
            sa.Column("ug", sa.Boolean),
            sa.Column("wp", sa.Boolean),
            sa.Column("v", sa.String),
            sa.Column("msg", sa.String),

            sa.Column("inscription_id", sa.String(255)),
            sa.Column("inscription_number", sa.BigInteger),
            sa.Column("deployer", sa.String(255)),
            sa.Column("deploy_time", sa.BigInteger),

            sa.Column("minted", sa.String),
            sa.Column("start_number", sa.BigInteger),
            sa.Column("end_number", sa.BigInteger),
            sa.Column("start_time", sa.BigInteger),
            sa.Column("end_time", sa.BigInteger),
            sa.Column("upgrade_time", sa.BigInteger),

            sa.Column("upgrade_pending", ARRAY(JSON), default=[]),
            sa.Column("upgrade_history", ARRAY(JSON), default=[]),
        )

        self.balance = sa.Table(
            f"balance_{db_version}",
            metadata,
            # address + inscription_number | address + tick + tick_id
            sa.Column("id", sa.String, primary_key=True),
            sa.Column("address", sa.String(255)),
            sa.Column("tick", sa.String),
            sa.Column("tick_id", sa.String, default=""),
            sa.Column("inscription_id", sa.String(255)),
            sa.Column("inscription_number", sa.BigInteger),
            sa.Column("balance", sa.String, default=0),
            sa.Column("available_balance", sa.String, default=0),
            sa.Column("pending_send_pool", ARRAY(JSON), default=[]),
            sa.Column("available_send_pool", ARRAY(JSON), default=[]),
            sa.Column("sent_send_pool", ARRAY(JSON), default=[]),
            sa.Column("received_send_pool", ARRAY(JSON), default=[]),
            sa.Column("received_mint_pool", ARRAY(JSON), default=[]),
        )

        self.transaction = sa.Table(
            f"transaction_{db_version}",
            metadata,
            # event id
            sa.Column("id", sa.BigInteger, primary_key=True),
            sa.Column("block_height", sa.BigInteger),
            sa.Column("inscription_id", sa.String(255)),
            sa.Column("inscription_number", sa.BigInteger),
            sa.Column("method", ENUM("inscribe-mint", "inscribe-send", "inscribe-remaining",
                                     "inscribe-cancel", "inscribe-upgrade", "inscribe-deploy",
                                     "transfer-upgrade", "transfer", name=f"transaction_enum_{db_version}")),
            sa.Column("token_id", sa.String),
            sa.Column("quantity", sa.String),
            sa.Column("from", sa.String(255)),
            sa.Column("to", sa.String(255)),
            sa.Column("time", sa.String(65)),
            sa.Column("valid", sa.Boolean),
            sa.Column("invalid_reason", sa.String),
        )

    async def init(self):
        env = Env()
        env.read_env()
        self.engine = await create_engine(
            maxsize=env.int("ORC_PGSQL_CONNECTION_POOL_SIZE"),
            user=env.str("ORC_PGSQL_USER"),
            password=env.str("ORC_PGSQL_PASSWD"),
            database=env.str("ORC_PGSQL_DB"),
            host=env.str("ORC_PGSQL_HOST"),
        )

    async def close(self):
        self.engine.close()
        await self.engine.wait_closed()

    async def create_token_table(self):
        try:
            async with self.engine.acquire() as conn:
                await conn.execute(f"DROP TABLE IF EXISTS token_{self.db_version} CASCADE")
                await conn.execute(CreateTable(self.token))
            return True
        except Exception as e:
            self.logger.error(f"create token table error: {e}")
            return False

    async def create_balance_table(self):
        try:
            async with self.engine.acquire() as conn:
                await conn.execute(f"DROP TABLE IF EXISTS balance_{self.db_version} CASCADE")
                await conn.execute(CreateTable(self.balance))
            return True
        except Exception as e:
            self.logger.error(f"create balance table error: {e}")
            return False

    async def create_transaction_table(self):
        try:
            async with self.engine.acquire() as conn:
                await conn.execute(f"DROP TABLE IF EXISTS transaction_{self.db_version} CASCADE")
                await conn.execute(f"DROP TYPE IF EXISTS transaction_enum_{self.db_version} CASCADE")
                await conn.execute(f"CREATE TYPE transaction_enum_{self.db_version} AS ENUM ('inscribe-mint', 'inscribe-send','inscribe-cancel', 'inscribe-remaining', 'transfer', 'inscribe-upgrade', 'inscribe-deploy','transfer-upgrade')")
                await conn.execute(CreateTable(self.transaction))
            return True
        except Exception as e:
            self.logger.error(f"create transaction table error: {e}")
            return False

    async def get_event_by_block_height(self, block_height):
        async with self.semaphore:
            try:
                async with self.engine.acquire() as conn:
                    query = self.event.select().where(self.event.c.block_height ==
                                                      block_height).order_by(self.event.c.id)
                    result = await conn.execute(query)
                    return await result.fetchall()
            except Exception as e:
                self.logger.error(f"get event by block height error: {e}")
                return None

    async def get_event_by_id(self, event_id):
        async with self.semaphore:
            try:
                async with self.engine.acquire() as conn:
                    query = self.event.select().where(self.event.c.id == event_id)
                    result = await conn.execute(query)
                    return await result.fetchall()
            except Exception as e:
                self.logger.error(f"get event by id error: {e}")
                return None

    async def get_token_by_id(self, token_id):
        async with self.semaphore:
            try:
                async with self.engine.acquire() as conn:
                    query = self.token.select().where(self.token.c.id == token_id)
                    result = await conn.execute(query)
                    token = await result.fetchall()
                    return token
            except Exception as e:
                self.logger.error(f"is token exist error: {e}")
                return None

    async def get_token_by_inscription_number(self, inscription_number, tick):
        async with self.semaphore:
            try:
                async with self.engine.acquire() as conn:
                    query = self.token.select().where(
                        self.token.c.inscription_number == inscription_number).where(self.token.c.tick == tick)
                    result = await conn.execute(query)
                    token = await result.fetchall()
                    return token
            except Exception as e:
                self.logger.error(
                    f"get token by inscription number failed: {e}")
                return None

    async def save_token_info(self, value):
        async with self.semaphore:
            try:
                async with self.engine.acquire() as conn:
                    await conn.execute(insert(self.token).values(value).on_conflict_do_nothing())
                return True
            except Exception as e:
                self.logger.error(f"save token info error: {e}")
                return False

    async def update_token_info(self, token_id, value):
        async with self.semaphore:
            try:
                async with self.engine.acquire() as conn:
                    await conn.execute(self.token.update().where(self.token.c.id == token_id).values(value))
                return True
            except Exception as e:
                self.logger.error(f"update token info error: {e}")
                return False

    async def get_balance_by_id(self, id):
        async with self.semaphore:
            try:
                async with self.engine.acquire() as conn:
                    query = self.balance.select().where(
                        self.balance.c.id == id)
                    result = await conn.execute(query)
                    balance = await result.fetchall()
                    return balance
            except Exception as e:
                self.logger.error(f"get balance by id error: {e}")
                return None

    async def get_balance_by_inscription_number(self, inscription_number, tick, address):
        async with self.semaphore:
            try:
                async with self.engine.acquire() as conn:
                    query = self.balance.select().where(
                        self.balance.c.inscription_number == inscription_number).where(
                        self.balance.c.tick == tick).where(
                        self.balance.c.address == address)
                    result = await conn.execute(query)
                    balance = await result.fetchall()
                    return balance
            except Exception as e:
                self.logger.error(
                    f"get balance by inscription number failed: {e}")
                return None

    async def save_balance_info(self, value):
        async with self.semaphore:
            try:
                async with self.engine.acquire() as conn:
                    await conn.execute(insert(self.balance).values(value).on_conflict_do_nothing())
                return True
            except Exception as e:
                self.logger.error(f"save balance info error: {e}")
                return False

    async def update_balance_info(self, balance_id, value):
        async with self.semaphore:
            try:
                async with self.engine.acquire() as conn:
                    await conn.execute(self.balance.update().where(self.balance.c.id == balance_id).values(value))
                return True
            except Exception as e:
                self.logger.error(f"update balance info error: {e}")
                return False

    async def save_transaction_info(self, value):
        async with self.semaphore:
            try:
                async with self.engine.acquire() as conn:
                    await conn.execute(insert(self.transaction).values(value).on_conflict_do_nothing())
                return True
            except Exception as e:
                self.logger.error(f"save transaction info error: {e}")
                return False

    async def update_transaction_info(self, transaction_id, value):
        async with self.semaphore:
            try:
                async with self.engine.acquire() as conn:
                    await conn.execute(self.transaction.update().where(self.transaction.c.id == transaction_id).values(value))
                return True
            except Exception as e:
                self.logger.error(f"update transaction info error: {e}")
                return False

    async def get_transaction_by_id(self, id):
        async with self.semaphore:
            try:
                async with self.engine.acquire() as conn:
                    query = self.transaction.select().where(
                        self.transaction.c.id == id)
                    result = await conn.execute(query)
                    transaction = await result.fetchall()
                    return transaction
            except Exception as e:
                self.logger.error(f"get transaction by id error: {e}")
                return None


if __name__ == "__main__":

    import os
    from loguru import logger
    logger.add(
        f"./logs/{os.path.basename(__file__).split('.')[0]}.log",
        level="INFO",
        rotation="500 MB",
        enqueue=True
    )

    pg = PgsqlHelper(logger)

    # async def create_table():
    #     await pg.init()
    #     await pg.create_token_table()
    #     await pg.create_balance_table()
    #     await pg.create_transaction_table()
    #     await pg.close()
    # asyncio.run(create_table())
