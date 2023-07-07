import os
import sys
import asyncio
import argparse

src_path = os.path.dirname(os.path.dirname(__file__))
sys.path.append(src_path)

from src.utils.clean_db import clean_db  # noqa
from src.indexer import Indexer  # noqa


def parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='fetch inscriptions and blocks')

    parser.add_argument('-d', '--db_version',  type=str,
                        choices=['A', 'B'], default='A', help='db version')

    parser.add_argument('-s', '--start_height', type=int,
                        default=None, help='start block height')

    parser.add_argument('-c', '--clean_db', action='store_true',
                        help='clean dbs before start')

    return parser


if __name__ == '__main__':

    args = parser().parse_args()
    db_version = args.db_version
    start_height = args.start_height
    is_clean_db = args.clean_db

    print(
        f"db_version: {db_version}, start_height: {start_height}, is_clean_db: {is_clean_db}")

    if is_clean_db:
        asyncio.run(clean_db(db_version))

    indexer = Indexer(db_version)
    if start_height is not None:
        start_height = int(start_height)
    asyncio.run(indexer.run(start_height))
