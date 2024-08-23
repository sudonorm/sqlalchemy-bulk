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

from db import dataModel

from sqlalchemybulk.crud_helper_funcs import DownloadData, DeleteData
from dotenv import load_dotenv
from sqlalchemy import select, delete

import pandas as pd
import json

from test_create_empty_db import TestCreateDB

load_dotenv()

delete_data = DeleteData(engine=dataModel.engine)
download_data = DownloadData(engine=dataModel.engine)

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "-1")
create_db = TestCreateDB()


class TestDelete(TestCase):

    def test_delete(self):

        assert (
            SQLITE_DB_PATH != "-1"
        ), "Please add the path to the SQLite DB to a .env file. Note that the path should be to the desired SQLite DB, and should include the .db extension"

        assert os.path.exists(
            SQLITE_DB_PATH
        ), "No database with the indicated name exists. Please create a database by running the test_create_empty_db.py script"

        create_db.test_apply_migration_to_db()

        query = delete(dataModel.Address).where(dataModel.Address.postalZip == "15143")
        delete_data.delete_data_on_condition(
            dbTable="dataModel.Address", statement=query
        )

        query = select(dataModel.Address).where(dataModel.Address.postalZip == "15143")
        result = download_data.download_info_using_session(statement=query)
        assert len(result) == 0, "Error in deleting"

        print("Entries deleted")


if __name__ == "__main__":
    clean_db = TestDelete()
    clean_db.test_delete()
