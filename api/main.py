import asyncpg
from fastapi import Depends, FastAPI, Query
from fastapi.responses import ORJSONResponse
from api.db import get_db

app = FastAPI(default_response_class=ORJSONResponse)


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
    after_id: int | None = None,
):
    if after_id is not None:
        rows = await conn.fetch(
            """
            SELECT id, name, age, city, created_at
            FROM pagination_dataset
            WHERE id > $1
            ORDER BY id
            LIMIT $2
            """,
            after_id,
            limit + 1,
        )
    else:
        rows = await conn.fetch(
            """
            SELECT id, name, age, city, created_at
            FROM pagination_dataset
            ORDER BY id
            LIMIT $1
            """,
            limit + 1,
        )

    has_next_page = len(rows) > limit
    if has_next_page:
        rows = rows[:limit]

    end_cursor = rows[-1]["id"] if rows else None

    return {
        "data": rows,
        "page_info": {"end_cursor": end_cursor, "has_next_page": has_next_page},
    }
