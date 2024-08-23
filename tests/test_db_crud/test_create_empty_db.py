import sys
import os
from unittest import TestCase

home_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

for pth in [
    "..",
    home_dir,
    f'{home_dir}{os.sep}{"test_db_crud"}',
    f'{home_dir}{os.sep}{"test_db_crud"}{os.sep}{"db"}',
]:
    if pth not in sys.path:

        sys.path.append(pth)

from sqlalchemybulk.init_or_update_db import InitDB
from dotenv import load_dotenv

load_dotenv()

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "-1")
ALEMBIC_PATH = f'{os.getenv("ALEMBIC_PATH", "-1")}'


class TestCreateDB(TestCase):
    def test_create_db(self):
        init_db = InitDB()

        assert (
            SQLITE_DB_PATH != "-1"
        ), "Please add the path to the SQLite DB to a .env file. Note that the path should be to the desired SQLite DB, and should include the .db extension"

        assert not os.path.exists(
            SQLITE_DB_PATH
        ), "A database with the same name already exists. Please change the name of the database and try again"

        init_db.create_empty_sqlite_database()

        self.assertTrue(
            expr=os.path.exists(SQLITE_DB_PATH),
            msg="SQLite DB initialization failed",
        )
        print(f"{SQLITE_DB_PATH}{' created'}")

    def test_apply_migration_to_db(self):
        init_db = InitDB()

        assert (
            SQLITE_DB_PATH != "-1"
        ), "Please add the path to the SQLite DB to a .env file. Note that the path should be to the desired SQLite DB, and should include the .db extension"

        assert os.path.exists(
            SQLITE_DB_PATH
        ), "A database with the same name already exists. Please change the name of the database and try again"

        init_db.apply_migrations_to_database(
            RUN_MIGRATION=True,
            CREATE_MIGRATION_FILE=True,
            DROP_ALEMBIC_TABLE_AND_STAMP_HEAD=False,
            IS_SQLITE=True,
            db_uri=SQLITE_DB_PATH,
            alembic_path=ALEMBIC_PATH,
        )

        # print(f"{SQLITE_DB_PATH}{' migrated'}")


if __name__ == "__main__":
    create_db = TestCreateDB()
    create_db.test_create_db()
