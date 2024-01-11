import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import MetaData
from sqlalchemy import func, select, update, and_, delete, insert
from sqlalchemy.dialects.sqlite import insert as sqlite_upsert
import sys
import os
import importlib

home_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(home_dir)

from glob import glob
from typing import List, Dict, Union, Tuple, Any

import warnings

warnings.filterwarnings("ignore")

### helper funcs
# from .helper_functions import HelperFunctions

meta = MetaData()


class BulkUpload:

    """This class is a collection of methods designed to make database operations easier and faster."""

    def __init__(self, dbTable: str, engine: Any, number_of_inserts: int = 19999):
        """dbTable is the name of the Table class e.g., dataModel.Store"""

        self.dbTableStr = dbTable

        dataModel = importlib.import_module(dbTable.split(".")[0])
        self.dbTable = eval(dbTable)
        self.engine = engine
        self.pk = self.get_primary_key(self.dbTable)
        self.number_of_inserts = number_of_inserts

    def get_primary_key(self, model_instance: Any) -> List[str]:
        """This function is used to return a list of primary key columns in a table

        Args:
            model_instance (dataModel): dataModel.table

        Returns:
            list: a list of the primary key columns
        """

        model_columns = model_instance.__mapper__.columns
        return [c.description for c in model_columns if c.primary_key][0]

    def create_string(self, tup: tuple, sep: str = "||") -> str:
        """_summary_

        Args:
            tup (tuple): a tuple of all the strings that should be combined together
            sep (str, optional): the separator to be used to separate the strings. Defaults to "||".

        Returns:
            str: string consisting of all elements in the input tuple separated by a separator
        """

        strng = ""
        for i in range(len(tup)):
            if i == 0:
                strng = tup[i]
            else:
                strng = f"{strng}{sep}{tup[i]}"
        return strng

    def get_or_none(
        self,
        df: pd.DataFrame,
        cols_dict: Dict,
        create_pk: bool = True,
        df_to_use: str = "df",
    ) -> Union[int, None]:
        """This function tries to search for a condition in a database table and if this condition is satisfied, it returns
            the primary key column ids of the rows in the input dataframe, otherwise, it returns None for that row


        Returns:
            pd.DataFrame: a dataframe consisting of matched entries which have a primary key id returned from the database or None
        """

        strng = ""
        quote = "'"
        df_in, df_out = pd.DataFrame(), pd.DataFrame()

        if create_pk:
            col_to_use = self.pk
        else:
            col_to_use = f'{self.pk}{"_tmp"}'

        oString = ",".join(
            x
            for x in [
                f'{df_to_use}{"["}{quote}{ky}{quote}{"]"}'
                for ky in list(cols_dict.keys())
            ]
        )

        tuple_string = f'{"tuple(zip("}{oString}{"))"}'

        df["match_str"] = [self.create_string(x) for x in eval(tuple_string)]

        table_data = pd.DataFrame(self.select_table(self.dbTable, fltr_output=True))

        if len(table_data) != 0:
            df_to_use = "table_data"
            oString = ",".join(
                x
                for x in [
                    f'{df_to_use}{"["}{quote}{ky}{quote}{"]"}'
                    for ky in list(cols_dict.keys())
                ]
            )
            tuple_string = f'{"tuple(zip("}{oString}{"))"}'

            table_data["match_str"] = [
                self.create_string(x) for x in eval(tuple_string)
            ]

            table_data_dict = dict(zip(table_data["match_str"], table_data[self.pk]))

            df_in = df[df["match_str"].isin(list(table_data["match_str"].unique()))]
            df_in[col_to_use] = [table_data_dict[x] for x in list(df_in["match_str"])]

            df_out = df[~df["match_str"].isin(list(table_data["match_str"].unique()))]
        else:
            df_out = df.copy()

        df_out[col_to_use] = None

        return pd.concat([df_in, df_out]).reset_index(drop=True)

    def chunked(self, it: Union[List, Tuple], n: int) -> Dict:
        """This function was lifted verbatim from peewee. It's used to chunk up a large dataset into n-sized chunks

        Args:
            it (Union[List, Tuple]): an iterable such as a list or tuple which contains the records to be batched
            n (int): the batch size to use for chunking

        Returns:
            Dict: a dictionary of records

        Yields:
            Iterator[Dict]: a list containing a dictionary of n-sized records
        """

        import itertools

        marker = object()
        for group in (
            list(g) for g in itertools.zip_longest(*[iter(it)] * n, fillvalue=marker)
        ):
            if group[-1] is marker:
                del group[group.index(marker) :]
            yield group

    def get_maximum_row_id(self, data_model_name: str = "dataModel") -> int:
        """
        This function is used to return the maximum row used in a primary key column of a Table.
        It will only work if the primary key column is numeric

        Returns:
            int: the maximum row in the primary key (pk) column or 0
        """

        tbl = str(self.dbTable)
        dataModel = importlib.import_module(tbl.split(".")[0])
        tbl = tbl.split(".")[-1].replace(">", "").replace("'", "")
        tbl = f'{data_model_name}{"."}{tbl}'

        with self.engine.connect() as connection:
            result = connection.execute(select(func.max(eval(f'{tbl}{"."}{self.pk}'))))
            for res in result:
                max_row_id = res[0]

        if max_row_id is None:
            return 0
        else:
            return max_row_id

    def upsert(
        self, df: pd.DataFrame, create_pk: bool = True, surpress_print: bool = True
    ) -> None:
        """
        The upsert function helps us to update or insert data in bulk. It works by taking a dataframe containing
        data to be uploaded and splitting this into two dataframes: new and update
        It determines what is new and what should be updated by checking is the primary key column is empty
        The primary key column needs to have been added in the dataframe prior to passing it to this function
        For details on how to add the primary key column, see the upsert_table() function

        Args:
            df (pd.DataFrame): dataframe containing data to be uploaded
            create_pk (bool, optional): option to create the primary key or not. This is important because we have tables
            with static IDs which we do not want to autoincrement. By setting this to False, the primary key column is not created rather the
            one present in the dataframe is used. Defaults to True.
        """

        dfCopy = df.copy()

        if create_pk:
            new = dfCopy[dfCopy[self.pk].isna()].drop(columns=[self.pk])
        else:
            new = dfCopy[dfCopy[f'{self.pk}{"_tmp"}'].isna()].drop(
                columns=[f'{self.pk}{"_tmp"}']
            )
            if len(new) > 0:
                new = new.sort_values(by=[self.pk], ascending=True).reset_index(
                    drop=True
                )
                try:
                    new[self.pk] = new[self.pk].map(int)
                except:
                    pass
                # if new.loc[0][self.pk] == 0:
                #     new[self.pk] = new[self.pk] + 1

        if create_pk:
            max_row = self.get_maximum_row_id()

            if not surpress_print:
                print("max row is...", max_row)

            if max_row == 0:
                id_list = list(np.arange(1, len(new) + 1))
            else:
                max_row = max_row + 1
                id_list = list(np.arange(max_row, max_row + len(new)))

            assert len(new) == len(id_list)
            new[self.pk] = id_list

        if create_pk:
            update = (
                dfCopy[~dfCopy[self.pk].isna()]
                .drop_duplicates(subset=[self.pk])
                .reset_index(drop=True)
            )
        else:
            update = (
                dfCopy[~dfCopy[f'{self.pk}{"_tmp"}'].isna()]
                .drop_duplicates(subset=[f'{self.pk}{"_tmp"}'])
                .drop(columns=[f'{self.pk}{"_tmp"}'])
                .reset_index(drop=True)
            )

        if not surpress_print:
            print("new")
            print(new.head(5))

            print("update")
            print(update.head(5))

        if len(update) > 0:
            try:
                update[self.pk] = update[self.pk].map(int)
            except:
                pass

        ## bulk insert new records
        if len(new) > 0:
            new = [u for u in new.to_dict("records")]
            with Session(self.engine) as session, session.begin():
                for batch in self.chunked(new, self.number_of_inserts):
                    print(
                        f'{"Insert for batch of "}{len(batch)}{" is being worked on"}'
                    )
                    session.bulk_insert_mappings(self.dbTable, batch)

        ## bulk update new records
        if len(update) > 0:
            update = [u for u in update.to_dict("records")]
            with Session(self.engine) as session, session.begin():
                for batch in self.chunked(update, self.number_of_inserts):
                    print(
                        f'{"Update for batch of "}{len(batch)}{" is being worked on"}'
                    )
                    session.bulk_update_mappings(self.dbTable, batch)

    def upsert_table(
        self, df: pd.DataFrame, cols_dict={}, create_pk: bool = True
    ) -> None:
        """
        The upsert_table function helps us to update or insert data to a table. it uses the upsert() funtion to achieve this.
        Args:

            df (pd.DataFrame): the dataframe which contains data to be updated or inserted
            cols_dict (Dict): key-value dictionary of UniqueConstraint columns in dataframe as keys and corresponding columns in the database as values.
            create_pk (bool, optional): option to create the primary key or not. This is important because we have tables
            with static IDs which we do not want to autoincrement. By setting this to False, the primary key column is not created rather the
            one present in the dataframe is used. Defaults to True.
        """
        df = self.get_or_none(df=df.copy(), cols_dict=cols_dict, create_pk=create_pk)
        self.upsert(df, create_pk=create_pk)

    def atomic_bulk_upsert(
        self,
        data: pd.DataFrame,
        unique_idx_elements: list,
        column_update_fields: list,
    ) -> List[int]:
        """Bulk upsert records using the sqlite_upsert method. This method has also been tested to work with not just SQLite but also postgresSQL

        Args:
            dbTable (str): a string representation of the Table to be worked on in the form dataModel.testTable
            data (pd.DataFrame): dataframe containing the data to be uploaded. Column names must match column names in the DB
            unique_idx_elements (list): a list of columns which should be searched for, which when considered together are unique. Can also be one unique field.
            A sequence consisting of string column names, _schema.Column objects, or other column expression objects that will be used to infer a target index or unique constraint.
            column_update_fields (list): a list of the columns to be updated.
        """
        dbTable = self.dbTableStr
        dataModel = importlib.import_module(dbTable.split(".")[0])
        all_row_ids = []
        records = [u for u in data.to_dict("records")]
        dbTable_eval = eval(dbTable)
        dbTable_eval_id = eval(f'{self.dbTableStr}{".id"}')

        with Session(self.engine) as session, session.begin():
            for batch in self.chunked(records, self.number_of_inserts):
                print(
                    f'{"Insert or update for batch of "}{len(batch)}{" is being worked on"}'
                )
                stmt = sqlite_upsert(dbTable_eval).values(batch)
                column_dict_fields = {
                    column.name: column
                    for column in stmt.excluded._all_columns
                    if column.name in column_update_fields
                }
                stmt = stmt.on_conflict_do_update(
                    index_elements=unique_idx_elements, set_=column_dict_fields
                )

                result = session.scalars(stmt.returning(dbTable_eval_id))
                row_ids = result.all()
                all_row_ids.extend(row_ids)
                session.execute(stmt)

        return all_row_ids

    def bulk_insert_many(self, data: pd.DataFrame) -> List[int]:
        """Bulk insert records using the insert method.

        Args:
            dbTable (str): a string representation of the Table to be worked on in the form dataModel.testTable
            data (pd.DataFrame): dataframe containing the data to be uploaded. Column names must match column names in the DB
        """
        dbTable = self.dbTableStr
        dataModel = importlib.import_module(dbTable.split(".")[0])
        records = [u for u in data.to_dict("records")]
        dbTable_eval = eval(dbTable)

        with Session(self.engine) as session, session.begin():
            for batch in self.chunked(records, self.number_of_inserts):
                print(
                    f'{"Insert or update for batch of "}{len(batch)}{" is being worked on"}'
                )

                try:
                    session.execute(insert(dbTable_eval), batch)
                except Exception as e:
                    print(e)

    def download_info_using_session(self, statement: Any = "") -> pd.DataFrame:
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

    def select_table(
        self,
        dbTable: object,
        fltr_output: bool = False,
        fltr: List = ["_sa_instance_state"],
        useSubset: bool = False,
        subset_col: str = "",
        subset: list = [],
        subset_multi: dict = {},
    ) -> List[Dict]:
        """
        This function is used to select everything or a filtered output from a Table in the database.

        Args:
            dbTable (object): this refers to a Table class e.g., dataModel.Store
            fltr_output (bool, optional): this indicates if we want the output of the select statement to be filtered or not. Defaults to False.
            fltr (List, optional): whatever is put into this list as an argument will be removed from the results. Defaults to ["_sa_instance_state"].
            subset_col (str): column whcih serves as limiter (only entries matching subset will be returned)
            subset (list): subset of entries to select from subset_col
            subset_multi (dict): a dictionary with at least two entries with a key which is a string and a value which is a list e.g. {'Key': ['key_1', 'key_2'], 'idG': [156, 107, 109, 108, 128]}

        Usage:
            This function can be used in three ways:
            1. query everything in a table and remove a few columns, here, the id column
                select_table(dbTable=dataModel.FData, fltr_output=True, fltr=["_sa_instance_state", "id"])
            2. query a table but use a filter on one column to limit the results. Here, we are onle selecting a few geos from the idGeo column
                select_table(dbTable=dataModel.FData, fltr_output=True, fltr=["_sa_instance_state", "id"], useSubset=True, subset_col = "idG", subset = [156, 109, 137])
            3. query a table but filter multiple columns. Here, we are filtering the kpiKey and idGeo columns using a list of elements
                select_table(dbTable=dataModel.FData, fltr_output=True, fltr=["_sa_instance_state", "id"], useSubset=True, subset_multi = {'Key': ['key_1', 'key_2'], 'idG': [156, 107, 109, 108, 128]})

        Returns:
            List: a list of records from a Table in the database
        """

        if useSubset and subset_multi:
            and_filter = [
                getattr(dbTable, subset_col).in_(subset)
                for subset_col, subset in subset_multi.items()
            ]
            query = select(dbTable).where((and_(*and_filter)))
            res = self.download_info_using_session(statement=query)

        elif useSubset:
            subset = subset + [
                str(x) for x in subset
            ]  ### added in case we pass in values with a different datatype
            query = select(dbTable).where(getattr(dbTable, subset_col).in_(subset))
            res = self.download_info_using_session(statement=query)
        else:
            query = select(dbTable)
            res = self.download_info_using_session(statement=query)

        if fltr_output:
            for col in fltr:
                try:
                    res = res.drop(columns=[col])
                except:
                    pass

        elements = res.to_dict(orient="records")
        return elements

    def use_session(self, statement: Any) -> None:
        """Use a session to execute a query statement in a transaction"""

        with Session(self.engine) as session, session.begin():
            session.execute(statement)

    def delete_from_table(
        self, table: str, pk_col_of_table: str, lst_of_ids: List
    ) -> None:
        """
        This function can be used to delete rows from a database. It works by deleting the rows using the primary key

        Args:
            table (str): the table we want to delete from. This should be passed in as a string. We use eval() to convert it into an object
            pk_col_of_table (str): this should be the primary key column e.g., "dataModel.Categories.id"
            lst_of_ids (List): this should be the list of primary keys to be dropped
        """

        dataModel = importlib.import_module(table.split(".")[0])
        table = eval(table)
        pk_col_of_table = eval(pk_col_of_table)
        with Session(self.engine) as session, session.begin():
            stmt = table.__table__.delete().where(pk_col_of_table.in_(lst_of_ids))
            session.execute(stmt)

    def update_table(
        self, table: str, pk_col_of_table: str, lst_of_ids: List, valuesDict: Dict
    ) -> None:
        """
        This function can be used to update rows in a database. It works by targeting the rows using the primary key
        and updating them using the valuesDict, which should contain records

        Args:
            table (str): the table we want to update. This should be passed in as a string. We use eval() to convert it into an object
            pk_col_of_table (str): this should be the primary key column e.g., "dataModel.Categories.id"
            lst_of_ids (List): this should be the list of primary keys to be updated
            valuesDict (Dict): this should be a dictionary of records which should match the columns of the table
        """

        dataModel = importlib.import_module(table.split(".")[0])
        table = eval(table)
        pk_col_of_table = eval(pk_col_of_table)
        with Session(self.engine) as session, session.begin():
            stmt = (
                table.__table__.update()
                .where(pk_col_of_table.in_(lst_of_ids))
                .values(valuesDict)
            )
            session.execute(stmt)
