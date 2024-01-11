import sys
import os

home_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(home_dir)

from .manager import Migrate, Connection
from dotenv import load_dotenv

load_dotenv()

DB_URI = os.getenv("DB_URI")


class InitDB:
    def create_empty_sqlite_database(
        self,
    ):
        con = Connection()
        sqlite_file_path = os.getenv(
            "SQLITE_DB_PATH", os.getcwd() + os.sep + "sample.db"
        )

        if not os.path.exists(sqlite_file_path):
            con.create_connection(sqlite_file_path)

        assert sqlite_file_path != ""
        db_uri = f"sqlite:///{sqlite_file_path}"

        return db_uri

    def apply_migrations_to_database(
        self,
        RUN_MIGRATION: bool = True,
        CREATE_MIGRATION_FILE: bool = False,
        DROP_ALEMBIC_TABLE_AND_STAMP_HEAD: bool = False,
        IS_SQLITE: bool = True,
        db_uri: str = DB_URI,
        alembic_path: str = home_dir,
    ):
        if IS_SQLITE:
            SQLITE_DB_PATH = os.getenv("SQLITE_DB_PATH")
            db_uri = f"sqlite:///{SQLITE_DB_PATH}"

        script_location = f'{alembic_path}{os.sep}{"alembic"}'

        migrate = Migrate(
            script_location=script_location,
            uri=db_uri,
            is_sqlite=IS_SQLITE,
            db_file_path=SQLITE_DB_PATH,
            create_migration_file=CREATE_MIGRATION_FILE,
            run_migration=RUN_MIGRATION,
            drop_alembic_stamp_head=DROP_ALEMBIC_TABLE_AND_STAMP_HEAD,
        )

        if migrate.check_for_migrations():
            migrate.init_db()
