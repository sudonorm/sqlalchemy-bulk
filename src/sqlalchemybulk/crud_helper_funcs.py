import pandas as pd
import numpy as np
import os
import sys

home_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(home_dir)

from glob import glob

from typing import List, Dict, Union, Tuple, Any
from sqlalchemy import func, select, update, and_, delete, insert
import warnings

warnings.filterwarnings("ignore")

### helper funcs
from .helper_functions import HelperFunctions

## crud
from .crud import BulkUpload


class UploadData(HelperFunctions):

    """The UploadData class is the core of the upload process. In this class, there are functions which are used to upload data
    to different tables in the database
    """

    def __init__(self, engine: Any):
        self.engine = engine

    def upload_info(
        self,
        df: pd.DataFrame = pd.DataFrame(),
        dbTable: str = "",
        cols_dict={"base_url": "base_url"},
        create_pk: bool = True,
        drop_na: bool = True,
        drop_duplicate_entries: bool = False,
    ):
        """
        General upload function which should be able to upload anything thrown at it
        Usage: upload_info(df=gs_df, dbTable = "dataModel.Gs", cols_dict = {"go":"go"})
        """

        dbTable_evl = eval(dbTable)

        for col in list(cols_dict.keys()):
            if drop_na:
                df = df[~df[col].isna()]

            if drop_duplicate_entries:
                df = df[~df[col].duplicated()].reset_index(drop=True)

        df = self.replace_nan_with_none(df)
        bulk = BulkUpload(dbTable, self.engine)
        bulk.upsert_table(dbTable_evl, df, cols_dict, create_pk=create_pk)

    def upload_info_atomic(
        self,
        dbTable: str = "",
        df: pd.DataFrame = pd.DataFrame(),
        unique_idx_elements: list = [],
        column_update_fields: list = [],
    ) -> List[int]:
        """
        General upload function which should be able to upload anything thrown at it
        Usage:
        idx_cols = ["somecolone", "somecoltwo"] -> a list of columns which have a UniqueConstraint and which when taken together give us a unique row in the table
        update_cols = ["someupdatecolone", "someupdatecoltwo"] -> a list of columns we want to update

        row_id_x = upload.upload_info_atomic(
                                dbTable="dataModel.User",
                                df=import_df_unformatted[update_cols + idx_cols],
                                unique_idx_elements=idx_cols,
                                column_update_fields=update_cols,
                            )

        where row_id_x is a list of ids of inserted or updated records

        Note that as of December 2023, this method will not work with a MySQL database, because we cannot return IDs of inserted or updated records

        """

        df = df[unique_idx_elements + column_update_fields]

        for col in unique_idx_elements:
            df = df[~df[col].isna()]

        df = df.drop_duplicates(subset=unique_idx_elements).reset_index(drop=True)

        df = self.replace_nan_with_none(df)
        bulk = BulkUpload(dbTable, self.engine)

        row_ids = bulk.atomic_bulk_upsert(df, unique_idx_elements, column_update_fields)

        return row_ids

    def insert_atomic(
        self,
        dbTable: str = "",
        df: pd.DataFrame = pd.DataFrame(),
    ) -> List[int]:
        """
        General insert function to insert records. This should only be used when you are inserting new records.
        If there's a possibility that an update operation can occur, use the upload_info_atomic() function above

        Usage: insert_atomic(dbTable = "dataModel.TableOne", df=table_df)
        """

        df = self.replace_nan_with_none(df)
        bulk = BulkUpload(dbTable, self.engine)

        bulk.bulk_insert_many(df)

    def get_max_id(self, dbTable: str = "") -> int:
        """
        This function returns the maximum row ID of a table

        Usage: get_max_id(dbTable = "dataModel.TableOne")
        """

        bulk = BulkUpload(dbTable, self.engine)

        id = bulk.get_maximum_row_id()
        return id


class DownloadData(HelperFunctions):

    """The UploadData class is the core of the upload process. In this class, there are functions which are used to upload data
    to different tables in the database
    """

    def __init__(self, engine: Any):
        self.engine = engine

    def download_info(
        self,
        dbTable: str = "",
        fltr_output: bool = True,
        fltr: list = ["_sa_instance_state"],
        include_cols_list: list = [],
        useSubset: bool = False,
        subset_col: str = "",
        subset: list = [],
        subset_multi: dict = {},
    ):
        """
        General download function which should be able to download from the database. It uses the select_table() func.
        Note that it's slower than the download_info_using_session below
        Usage: download_info(dbTable = "dataModel.TableOne", fltr_output = True, fltr = ["_sa_instance_state"], include_cols_list = ['some_column', 'another_column'])

        include_cols_list: filter out the resulting dataframe for just specific columns in the DB table

        """

        bulk = BulkUpload(dbTable, self.engine)
        dbTable_evl = eval(dbTable)
        data = pd.DataFrame(
            bulk.select_table(
                dbTable_evl,
                fltr_output=fltr_output,
                fltr=fltr,
                useSubset=useSubset,
                subset_col=subset_col,
                subset=subset,
                subset_multi=subset_multi,
            )
        )

        if len(data.columns) > 0 and len(data) > 0:
            if len(include_cols_list) > 0:
                data = data[include_cols_list]
        else:
            data = pd.DataFrame()

        return data

    def download_info_using_session(self, statement: Any = ""):
        """
        Faster download function which should be able to download from the database. 
        It uses the a SQL statement and the SQLALchemy "select" and "and_" methods

        Usage: 
        from sqlalchemy import update, select, and_, delete
        query = select(dataModel.TableOne)
        download_info_using_session(statement = query)


        Here are a few examples of how the statement can be constructed using filters:

        1. Join with multiple filters
        query = select(dataModel.TableOne.id, dataModel.TableOne.Key, dataModel.TableTwo.name).\
            join(dataModel.TableTwo, dataModel.TableTwo.idKey == dataModel.TableOne.id). \
            where((and_(dataModel.TableOne.keyType.in_([19,29,37,138,231]), dataModel.TableTwo.lang_id == 2)))
        
        2. Join with one iflter
        query = select(dataModel.TableOne.id, dataModel.TableOne.table_two_id, dataModel.TableOne.name, dataModel.TableOne.order, \
                       dataModel.TableOne.cype, dataModel.TableOne.suppText, dataModel.TableOne.isHighlighted) \
                        .join(dataModel.TableTwo, dataModel.TableTwo.id == dataModel.TableOne.table_two_id) \
                        .where(dataModel.TableTwo.id.in_(listofids))

        3. Just one equality check using a value              
        query = select(dataModel.TableOne).where(dataModel.TableOne.cType == "normal") ## just one equality check

        4. Just one equality check using a list
        query = select(dataModel.TableOne).where(dataModel.TableOne.idV.in_(idvs)) ## just one list membership check where idvs is a list of elements [19,29,37,138,231]

        5. Equaity checks using two values
        query = select(dataModel.TableOne.oName).join(dataModel.TableTwo, dataModel.TableTwo.id == dataModel.TableOne.idV) \
                .where((and_(dataModel.TableOne.outlookName == "out", \
                     dataModel.TableTwo.typeV == "out")))
        
        6. query with multiple joins
        query = select(dataModel.TableOne.id, dataModel.TableThree.id, dataModel.TableThree.idParent, dataModel.TableThree.cKey).\
        join(dataModel.TableTwo, dataModel.TableOne.id == dataModel.TableTwo.idP).\
        join(dataModel.TableThree, dataModel.TableThree.idCh == dataModel.TableTwo.id).\
        where(dataModel.TableOne.id.in_(idPsList))    

        7. query with multiple filters
        query = select(dataModel.TableOne.id).join(dataModel.TableTwo, dataModel.TableTwo.id = dataModel.TableOne.idVg) \
                .where((and_(dataModel.TableTwo.idV == idV, dataModel.TableTwo.idVP == idVP, \
                dataModel.TableOne.idLang == language, dataModel.TableOne.idGeo == id_geo)))


        """
        return pd.read_sql(statement, self.engine)


class DeleteData(HelperFunctions):

    """The UploadData class is the core of the upload process. In this class, there are functions which are used to upload data
    to different tables in the database

    """

    def __init__(self, engine: Any):
        self.engine = engine

    def delete_data_from_table_using_ids(
        self, dbTable: str, pk_col_of_table: str, lst_of_ids: List
    ):
        """Delete data from a table using the primary key column. It has the downside that you need the primary key id first

        Usage: delete_data_from_table_using_ids(dbTable='dataModel.User', pk_col_of_table = 'dataModel.User.id', lst_of_ids = list(user_ids['id']))
            where user_ids is a filtered dataframe of users to be deleted in this instance
        """

        bulk = BulkUpload(dbTable, self.engine)
        bulk.delete_from_table(
            table=dbTable, pk_col_of_table=pk_col_of_table, lst_of_ids=lst_of_ids
        )

    def delete_data_on_condition(self, dbTable: str, statement: Any = ""):
        """A faster way to delete data based on a condition. The condition is included in the statement and you don't need the primary key
            Usage: 
            from sqlalchemy import update, select, and_, delete
            query = delete(dataModel.TableOne).where((and_(dataModel.TableOne.key_id.in_(batch), \
                                                        dataModel.TableOne.geo_id.in_(idGs))))
            delete_data_on_condition(dbTable='dataModel.TableOne', statement = query)
            where batch and idGeos are a list of elements
        """

        bulk = BulkUpload(dbTable, self.engine)
        bulk.use_session(statement=statement)
