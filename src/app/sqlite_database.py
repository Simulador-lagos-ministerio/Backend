import sqlalchemy as _sql
import sqlalchemy.ext.declarative as _declarative
import sqlalchemy.orm as _orm
from app.settings import settings


SqliteEngine = _sql.create_engine(settings.sqlite_database_url, connect_args={"check_same_thread": False})
SqliteSessionLocal = _orm.sessionmaker(autocommit=False, autoflush=False, bind=SqliteEngine)
SqliteBase = _declarative.declarative_base()


def create_sqlite_database():
    # Create tables on startup.
    return SqliteBase.metadata.create_all(bind=SqliteEngine)


def get_sqlite_db():
    # FastAPI dependency that yields a scoped session.
    db = SqliteSessionLocal()
    try:
        yield db
    finally:
        db.close()
