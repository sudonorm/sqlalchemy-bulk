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

from sqlalchemybulk.crud_helper_funcs import DownloadData
from dotenv import load_dotenv
from sqlalchemy import select

import pandas as pd
import json

load_dotenv()

### helper funcs
# from sqlalchemybulk.helper_functions import HelperFunctions
# from sqlalchemybulk.crud_helper_funcs import UploadData, DownloadData, DeleteData

from test_create_empty_db import TestCreateDB

download_data = DownloadData(engine=dataModel.engine)

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "-1")
create_db = TestCreateDB()


class TestQueryDB(TestCase):

    def test_query_full_table(self):

        assert (
            SQLITE_DB_PATH != "-1"
        ), "Please add the path to the SQLite DB to a .env file. Note that the path should be to the desired SQLite DB, and should include the .db extension"

        assert os.path.exists(
            SQLITE_DB_PATH
        ), "No database with the indicated name exists. Please create a database by running the test_create_empty_db.py script"

        create_db.test_apply_migration_to_db()

        ### query full table
        query = select(dataModel.Address)
        result = download_data.download_info_using_session(statement=query)

        assert len(result) > 0, "Nothing was queried"
        print("The length of the dataset is", len(result))
        print("Data queried successfully")

    def test_query_with_filters(self):

        assert (
            SQLITE_DB_PATH != "-1"
        ), "Please add the path to the SQLite DB to a .env file. Note that the path should be to the desired SQLite DB, and should include the .db extension"

        assert os.path.exists(
            SQLITE_DB_PATH
        ), "No database with the indicated name exists. Please create a database by running the test_create_empty_db.py script"

        create_db.test_apply_migration_to_db()

        ### query with filter
        query = select(dataModel.Address).where(dataModel.Address.postalZip == "3778")
        result = download_data.download_info_using_session(statement=query)

        assert len(result) > 0, "Nothing was queried"
        print("The length of the dataset is", len(result))
        print("Data queried successfully")


if __name__ == "__main__":
    read_db = TestQueryDB()
    read_db.test_query_full_table()
    read_db.test_query_with_filters()
