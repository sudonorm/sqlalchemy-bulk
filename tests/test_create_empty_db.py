import sys
import os
from unittest import TestCase

home_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(home_dir)

from sqlalchemybulk.init_or_update_db import InitDB
from dotenv import load_dotenv

load_dotenv()

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "-1")


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


if __name__ == "__main__":
    create_db = TestCreateDB()
    create_db.test_create_db()
