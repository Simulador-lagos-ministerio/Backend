import passlib.hash as _hash
import sqlalchemy as _sql

from app import sqlite_database as _database


# User persistence model.
class User(_database.Base):
    __tablename__ = "users"
    id = _sql.Column(_sql.Integer, primary_key=True, index=True)
    email = _sql.Column(_sql.String(255), unique=True, index=True, nullable=False)
    hashed_password = _sql.Column(_sql.String(255), nullable=False)

    def verify_password(self, password: str) -> bool:
        return _hash.bcrypt.verify(password, self.hashed_password)
