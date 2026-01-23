from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, declarative_base
from app.settings import settings

PostgisBase = declarative_base()
PostgisEngine = create_engine(settings.postgis_url, pool_pre_ping=True)
PostgisSessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=PostgisEngine)

def create_postgis_database():
    # Create tables on startup.
    return PostgisBase.metadata.create_all(bind=PostgisEngine)

def get_postgis_db():
    # FastAPI dependency that yields a scoped session.
    db = PostgisSessionLocal()
    try:
        yield db
    finally:
        db.close()