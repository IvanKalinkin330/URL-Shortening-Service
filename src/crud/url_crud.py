from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select, func, Sequence

from src.models import URL
from src.services.shortener import ShortenerService
from src.core.config import config


class URLCRUD:
    @staticmethod
    async def create_url(db: AsyncSession, url: str) -> URL:
        query = select(URL).where(URL.url == url)
        existing_url = await db.scalar(query)

        if existing_url:
            return existing_url

        seq = Sequence("urls_id_seq")
        next_id = await db.scalar(select(func.next_value(seq)))
        short_code = ShortenerService.encode(next_id + config.OFFSET)

        db_url = URL(id=next_id, url=url, short_code=short_code)

        db.add(db_url)
        await db.commit()
        await db.refresh(db_url)

        return db_url

    @staticmethod
    async def get_url_by_code(db: AsyncSession, short_code: str) -> URL | None:
        query = select(URL).where(URL.short_code == short_code)
        return await db.scalar(query)

    @staticmethod
    async def increment_access_count(db: AsyncSession, db_url: URL) -> URL:
        db_url.access_count += 1

        await db.commit()
        await db.refresh(db_url)
        return db_url

    @staticmethod
    async def update_url(db: AsyncSession, db_url: URL, new_url: str) -> URL:
        db_url.url = new_url

        await db.commit()
        await db.refresh(db_url)
        return db_url

    @staticmethod
    async def delete_url(db: AsyncSession, db_url: URL) -> None:
        await db.delete(db_url)
        await db.commit()
