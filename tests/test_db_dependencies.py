# tests/test_db_dependencies.py
from app import postgis_database, sqlite_database

class DummySession:
    def __init__(self):
        self.closed = False
    def close(self):
        self.closed = True

def test_get_postgis_db_yields_and_closes(monkeypatch):
    dummy = DummySession()
    monkeypatch.setattr(postgis_database, "PostgisSessionLocal", lambda: dummy)

    gen = postgis_database.get_postgis_db()
    db = next(gen)
    assert db is dummy

    gen.close()
    assert dummy.closed is True

def test_get_sqlite_db_yields_and_closes(monkeypatch):
    dummy = DummySession()
    monkeypatch.setattr(sqlite_database, "SqliteSessionLocal", lambda: dummy)

    gen = sqlite_database.get_sqlite_db()
    db = next(gen)
    assert db is dummy

    gen.close()
    assert dummy.closed is True
