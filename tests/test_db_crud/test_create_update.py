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

from sqlalchemybulk.crud_helper_funcs import UploadData
from dotenv import load_dotenv

import pandas as pd
import json

load_dotenv()

### helper funcs
# from sqlalchemybulk.helper_functions import HelperFunctions
# from sqlalchemybulk.crud_helper_funcs import UploadData, DownloadData, DeleteData

from test_create_empty_db import TestCreateDB

upload_data = UploadData(engine=dataModel.engine)

SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH", "-1")
create_db = TestCreateDB()


class TestCreateUpdate(TestCase):

    def get_data(self):
        with open(
            rf'{home_dir}{os.sep}{"test_db_crud"}{os.sep}{"db_test_data.json"}', "r"
        ) as f:
            data = json.load(f)
            df = pd.DataFrame(data)

        return df

    def test_create_update(self):

        data_df = self.get_data()

        assert (
            SQLITE_DB_PATH != "-1"
        ), "Please add the path to the SQLite DB to a .env file. Note that the path should be to the desired SQLite DB, and should include the .db extension"

        assert os.path.exists(
            SQLITE_DB_PATH
        ), "No database with the indicated name exists. Please create a database by running the test_create_empty_db.py script"

        create_db.test_apply_migration_to_db()

        returned_ids = upload_data.upload_info_atomic(
            dbTable="dataModel.Address",
            df=data_df,
            unique_idx_elements=["name", "postalZip"],
            column_update_fields=[
                "address",
                "country",
                "suptext",
                "numberrange",
                "currency",
                "alphanumeric",
            ],
        )

        assert len(returned_ids) > 0, "Nothing was inserted or updated"
        print("The data was inserted or updated")


if __name__ == "__main__":
    create_update = TestCreateUpdate()
    create_update.test_create_update()
