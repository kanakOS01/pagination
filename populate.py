import asyncio
import os
import random
import string
from datetime import datetime, timedelta
import time

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


def random_name():
    return "".join(random.choices(string.ascii_letters, k=10))


def random_city():
    cities = ["Delhi", "Mumbai", "London", "New York", "Tokyo", "Berlin"]
    return random.choice(cities)


def random_timestamp():
    """Generate a random datetime as epoch ms between 2010 and now."""
    start = datetime(2010, 1, 1)
    end = datetime.now()
    delta = end - start
    random_sec = random.randint(0, int(delta.total_seconds()))
    dt = start + timedelta(seconds=random_sec)
    # Convert to epoch milliseconds
    return int(dt.timestamp() * 1000)


async def main():
    st = time.time()
    conn = await asyncpg.connect(**DB_CONFIG)

    # Drop + Create table
    await conn.execute("""
    DROP TABLE IF EXISTS pagination_dataset;
    CREATE TABLE pagination_dataset (
        id SERIAL PRIMARY KEY,
        name TEXT,
        age INT,
        city TEXT,
        created_at BIGINT
    );
    """)

    # Generate rows with random epoch ms
    rows = [
        (random_name(), random.randint(18, 80), random_city(), random_timestamp())
        for _ in range(1_000_000)
    ]

    # Use COPY for fast bulk insert
    async with conn.transaction():
        await conn.copy_records_to_table(
            "pagination_dataset",
            records=rows,
            columns=["name", "age", "city", "created_at"]
        )

    await conn.close()
    print("✅ Table created and 1M rows inserted!")
    print(f"Time taken: {time.time() - st} ms")


if __name__ == "__main__":
    asyncio.run(main())
