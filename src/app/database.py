import sqlalchemy as _sql
import sqlalchemy.ext.declarative as _declarative
import sqlalchemy.orm as _orm

# Database engine and session helpers.
DATABASE_URL = "sqlite:///./database.db"

engine = _sql.create_engine(DATABASE_URL, connect_args={"check_same_thread": False})

SessionLocal = _orm.sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = _declarative.declarative_base()


def create_database():
    # Create tables on startup.
    return Base.metadata.create_all(bind=engine)


def get_db():
    # FastAPI dependency that yields a scoped session.
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
