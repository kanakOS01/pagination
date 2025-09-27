import os
from typing import AsyncGenerator

import asyncpg
from dotenv import load_dotenv

load_dotenv()

DB_CONFIG = {
    "user": os.getenv("DB_USER", ""),
    "password": os.getenv("DB_PWD", ""),
    "database": os.getenv("DB_NAME", ""),
    "host": os.getenv("DB_HOST", ""),
    "port": int(os.getenv("DB_PORT", "5432")),
}


async def get_db() -> AsyncGenerator[asyncpg.Connection, None]:
    """
    Open a new DB connection per request and close it afterwards.
    """
    conn = await asyncpg.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        await conn.close()
