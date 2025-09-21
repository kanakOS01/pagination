from fastapi import FastAPI
from fastapi.responses import ORJSONResponse


app = FastAPI(default_response_class=ORJSONResponse)


@app.get("/api/v1")
async def offset_pagination():
    pass


@app.get("/api/v2")
async def page_pagination():
    pass


@app.get("/api/v3")
async def keyset_pagination():
    pass


@app.get("/api/v4")
async def cursor_pagination():
    pass
