# Testing Guide

- Rename the `.env.default` file to `.env`
- Change the three variables appropriately
- Open the `db/dataModel.py` file and:

  - check that `CREATE_MIGRATION_FILE`, `RUN_MIGRATION` are set to `True` (here, if we want to review the migration file before migration, we can set `RUN_MIGRATION` to `False`)
  - check that `IS_SQLITE` is set to `True`, since we will be using a SQLite DB for the test
  - `DROP_ALEMBIC_TABLE_AND_STAMP_HEAD` should be set to False
  - If we have nothing to migrate, we can set `CREATE_MIGRATION_FILE`, `RUN_MIGRATION` to `False`

- Save and Run the `db/dataModel.py` file (this should create a db with the name provided in the .env file. It should also create a migration file in `alembic/versions`)
- Run the test cases in this order:
  - If the db/dataModel.py file has been run, then we can skip running the `test_create_empty_db.py` file
  - Run the `test_create_update.py`
  - Then we can test out wither the `test_read.py` or `test_delete.py` scripts

The .db file created can be opened using `DBBrowser for SQLite`.
