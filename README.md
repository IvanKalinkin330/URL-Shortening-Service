# URL Shortening Service FastAPI

Async API for a URL shortening service. The API allow users to perform the following operations:
- Create a new short code (using Base62 coding)
- Retrieve an original URL from a short code
- Redirect to original URL from a short code
- Update an existing short code
- Delete an existing short code
- Get statistics on the short code


## Technologies

- Python
- FastAPI
- PostgreSQL
- pydantic
- SQLAlchemy
- alembic


## Install

```bash
git clone https://github.com/pdnxhdm/URL-Shortening-Service
cd URL-Shortening-Service

python -m venv .venv
sourсe .venv/bin/activate   # Windows: .venv\Scripts\activate

pip install -r requirements.txt

alembic upgrade head
uvicorn src.main:app --reload
```


## Database Schema

### **urls**
| Column | Type | Description |
| :--- | :--- | :--- |
| id | Integer | Primary key |
| url | Varchar(2048) | Original url |
| short_code | Varchar(10) | Unique short code |
| created_at | DateTime | Date and time of creation |
| updated_at | DateTime | Date and time of update |
| access_count | Integer | Number of times accessed |


## Environment Variables

```env
DB_HOST: str
DB_PORT: int
DB_USER: str
DB_PASS: str
DB_NAME: str
```

## API Endpoints

### Create Short URL

##### Request
```
POST /shorten
{
  "url": "https://www.example.com/some/long/url"
}
```

##### Response
```json
{
  "id": "1",
  "url": "https://www.example.com/some/long/url",
  "short_code": "abc123",
  "created_at": "2021-09-01T12:00:00Z",
  "updated_at": "2021-09-01T12:00:00Z"
}
```

The endpoint validate the request body and return a **`201 Created`** status code with the newly created short URL or a **`400 Bad Request`** status code with error messages in case of validation errors


### Retrieve Original URL

##### Request
```
GET /shorten/abc123
```

##### Response
```json
{
  "id": "1",
  "url": "https://www.example.com/some/long/url",
  "short_code": "abc123",
  "created_at": "2021-09-01T12:00:00Z",
  "updated_at": "2021-09-01T12:00:00Z"
}
```

The endpoint return a **`200 OK`** status code with the original URL or a **`404 Not Found`** status code if the short URL was not found

### Redirect to Original URL

##### Request
```
GET /abc123
```

The endpoint return a **`307`** status code and redirect to original URL or a **`404 Not Found`** status code if the short URL was not found

### Update Short URL

##### Request
```
PATCH /shorten/abc123
{
  "url": "https://www.example.com/some/updated/url"
}
```

##### Response
```json
{
  "id": "1",
  "url": "https://www.example.com/some/long/url",
  "short_code": "abc123",
  "created_at": "2021-09-01T12:00:00Z",
  "updated_at": "2021-09-01T12:00:00Z"
}
```

The endpoint validate the request body and return a **`200 OK`** status code with the updated short URL or a **`400 Bad Request`** status code with error messages in case of validation errors. It return a **`404 Not Found`** status code if the short URL was not found

### Delete Short URL

##### Request
```
DELETE /shorten/abc123
```

The endpoint return a **`204 No Content`** status code if the short URL was successfully deleted or a **`404 Not Found`** status code if the short URL was not found

### Get URL Statistics

##### Request
```
GET /shorten/abc123/stats
```

##### Response
```json
{
  "id": "1",
  "url": "https://www.example.com/some/long/url",
  "short_code": "abc123",
  "created_at": "2021-09-01T12:00:00Z",
  "updated_at": "2021-09-01T12:00:00Z",
  "accessCount": 10
}
```

The endpoint return a **`200 OK`** status code with the statistics or a **`404 Not Found`** status code if the short URL was not found

## Sample code

```python
class URLCRUD:
    @staticmethod
    async def create_url(db: AsyncSession, url: str) -> URL:
        query = select(URL).where(URL.url == url)
        existing_url = await db.scalar(query)

        if existing_url:
            return existing_url

        # Generating a unique short code using Base62
        seq = Sequence("urls_id_seq")
        next_id = await db.scalar(select(func.next_value(seq)))
        # OFFSET is needed so that the short code does not start with 1
        short_code = ShortenerService.encode(next_id + config.OFFSET)

        db_url = URL(id=next_id, url=url, short_code=short_code)

        db.add(db_url)
        await db.commit()
        await db.refresh(db_url)

        return db_url
```

## Team

Ivan Kalinkin - Python Backend Developer