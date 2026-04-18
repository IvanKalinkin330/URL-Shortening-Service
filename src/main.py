from fastapi import FastAPI, Depends, HTTPException
from fastapi.responses import RedirectResponse
from sqlalchemy.ext.asyncio import AsyncSession
from typing import Annotated

from src.schemas import URLCreate, URLResponse, URLStats
from src.core.database import get_db
from src.crud import URLCRUD


app = FastAPI(title="URL Shortening Service")

db_session = Annotated[AsyncSession, Depends(get_db)]


@app.post("/shorten", status_code=201)
async def create_short_url(data: URLCreate, db: db_session) -> URLResponse:
    new_url = await URLCRUD.create_url(db, url=str(data.url))
    return new_url


@app.get("/shorten/{short_code}", status_code=200)
async def retrieve_original_url(short_code: str, db: db_session) -> URLResponse:
    db_url = await URLCRUD.get_url_by_code(db, short_code)

    if not db_url:
        raise HTTPException(status_code=404, detail="Code not found")

    await URLCRUD.increment_access_count(db, db_url)
    return db_url


@app.patch("/shorten/{short_code}", status_code=200)
async def update_short_url(short_code: str, data: URLCreate, db: db_session) -> URLResponse:
    db_url = await URLCRUD.get_url_by_code(db, short_code)

    if not db_url:
        raise HTTPException(status_code=404, detail="Code not found")

    updated_url = await URLCRUD.update_url(db, db_url, str(data.url))
    return updated_url


@app.delete("/shorten/{short_code}", status_code=204)
async def delete_short_url(short_code: str, db: db_session) -> None:
    db_url = await URLCRUD.get_url_by_code(db, short_code)

    if not db_url:
        raise HTTPException(status_code=404, detail="Code not found")

    await URLCRUD.delete_url(db, db_url)


@app.get("/shorten/{short_code}/stats", status_code=200)
async def get_url_statistics(short_code: str, db: db_session) -> URLStats:
    db_url = await URLCRUD.get_url_by_code(db, short_code)

    if not db_url:
        raise HTTPException(status_code=404, detail="Code not found")

    return db_url


@app.get("/{short_code}")
async def redirect_to_original_url(short_code: str, db: db_session) -> RedirectResponse:
    db_url = await URLCRUD.get_url_by_code(db, short_code)

    if not db_url:
        raise HTTPException(status_code=404, detail="Code not found")

    await URLCRUD.increment_access_count(db, db_url)
    return RedirectResponse(url=db_url.url, status_code=307)
