import asyncpg
import base64
import json
from fastapi import Depends, FastAPI, Query
from fastapi.responses import ORJSONResponse

from api.db import get_db

app = FastAPI(default_response_class=ORJSONResponse)


# ---------- Cursor Helpers ----------
def encode_cursor(created_at: int, id: int) -> str:
    raw = json.dumps({"created_at": created_at, "id": id})
    return base64.urlsafe_b64encode(raw.encode()).decode()


def decode_cursor(cursor: str) -> dict:
    raw = base64.urlsafe_b64decode(cursor.encode()).decode()
    return json.loads(raw)


# ---------- Offset Pagination ----------
@app.get("/api/v1")
async def offset_pagination(
    conn: asyncpg.Connection = Depends(get_db),
    offset: int = Query(0, ge=0),
    limit: int = Query(10, le=100),
):
    rows = await conn.fetch(
        """
        SELECT id, name, age, city, created_at
        FROM pagination_dataset
        ORDER BY id
        OFFSET $1 LIMIT $2
        """,
        offset,
        limit,
    )
    return {"data": rows, "offset": offset, "limit": limit}


# ---------- Page Pagination ----------
@app.get("/api/v2")
async def page_pagination(
    conn: asyncpg.Connection = Depends(get_db),
    page: int = Query(1, ge=1),
    page_size: int = Query(10, le=100),
):
    offset = (page - 1) * page_size
    rows = await conn.fetch(
        """
        SELECT id, name, age, city, created_at
        FROM pagination_dataset
        ORDER BY id
        OFFSET $1 LIMIT $2
        """,
        offset,
        page_size,
    )
    return {"data": rows, "page": page, "page_size": page_size}


# ---------- Cursor Pagination ----------
@app.get("/api/v3")
async def cursor_pagination(
    conn: asyncpg.Connection = Depends(get_db),
    limit: int = Query(10, le=100),
    after: str | None = None,
):
    if after:
        cursor = decode_cursor(after)
        rows = await conn.fetch(
            """
            SELECT id, name, age, city, created_at
            FROM pagination_dataset
            WHERE (created_at, id) < ($1, $2)
            ORDER BY created_at DESC, id DESC
            LIMIT $3
            """,
            cursor["created_at"],
            cursor["id"],
            limit,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT id, name, age, city, created_at
            FROM pagination_dataset
            ORDER BY created_at DESC, id DESC
            LIMIT $1
            """,
            limit,
        )


    end_cursor = None
    has_next_page = False

    if rows:
        last_row = rows[-1]
        end_cursor = encode_cursor(last_row["created_at"], last_row["id"])

        # check if more rows exist
        check = await conn.fetchval(
            f"""
            SELECT COUNT(1) FROM pagination_dataset
            WHERE (created_at, id) < ($1, $2)
            """,
            last_row["created_at"],
            last_row["id"],
        )
        has_next_page = check > 0 if check else False

    return {
        "data": rows,
        "page_info": {"end_cursor": end_cursor, "has_next_page": has_next_page},
    }


# ---------- Keyset Pagination ----------
@app.get("/api/v4")
async def keyset_pagination(
    conn: asyncpg.Connection = Depends(get_db),
    limit: int = Query(10, le=100),
    last_id: int | None = None,
):
    if last_id:
        rows = await conn.fetch(
            """
            SELECT id, name, age, city, created_at
            FROM pagination_dataset
            WHERE id > $1
            ORDER BY id
            LIMIT $2
            """,
            last_id,
            limit,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT id, name, age, city, created_at
            FROM pagination_dataset
            ORDER BY id
            LIMIT $1
            """,
            limit,
        )

    next_last_id = rows[-1]["id"] if rows else None
    return {"data": rows, "next_last_id": next_last_id}
