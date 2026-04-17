from sqlalchemy.orm import Session
from sqlalchemy import select

from src.models import URL


class URLCRUD:
    @staticmethod
    def create_url(db: Session, url: str, short_code: str) -> URL:
        db_url = URL(url=url, short_code=short_code)
        db.add(db_url)
        db.commit()
        db.refresh(db_url)
        return db_url
    
    @staticmethod
    def get_url_by_code(db: Session, short_code: str) -> URL | None:
        query = select(URL).where(URL.short_code == short_code)
        return db.scalar(query)
    
    @staticmethod
    def increment_access_count(db: Session, db_url: URL) -> URL:
        db_url.access_count += 1
        db.commit()
        db.refresh(db_url)
        return db_url
    
    @staticmethod
    def update_url(db: Session, db_url: URL, new_url: str) -> URL:
        db_url.url = new_url
        db.commit()
        db.refresh(db_url)
        return db_url
    
    @staticmethod
    def delete_url(db: Session, db_url: URL) -> None:
        db.delete(db_url)
        db.commit()