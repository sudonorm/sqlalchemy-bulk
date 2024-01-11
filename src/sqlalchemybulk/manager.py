import alembic
from alembic.config import CommandLine, Config
from alembic import command
import sqlite3
import re
import os
import sys

from typing import Any

home_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(home_dir)
import time
import shutil
from glob import glob
from sqlalchemy import create_engine, text

SLSH = os.sep


class Connection:
    def create_connection(self, path_to_db_file: str) -> None:
        """create a database connection to a SQLite database"""
        conn = None
        try:
            conn = sqlite3.connect(path_to_db_file)
        except sqlite3.Error as e:
            print(e)
        finally:
            if conn:
                conn.close()


class Migrate(Connection):
    def __init__(
        self,
        script_location: str,
        uri: str,
        is_sqlite: bool = False,
        db_file_path: str = None,
        create_migration_file: bool = False,
        run_migration: bool = True,
        drop_alembic_stamp_head: bool = False,
    ) -> None:
        self.script_location = script_location
        self.uri = uri
        self.is_sqlite = is_sqlite
        self.db_file_path = db_file_path
        self.run_migration = run_migration
        self.create_migration_file = create_migration_file
        self.drop_alembic_stamp_head = drop_alembic_stamp_head

    def to_archive(self, path_data: str = r"", existingFile: str = r"") -> None:
        """
        Move old files to the archive

        Parameters
        ----------
            fileType (str): this should be the name of the file that is to be moved to the archive

        Notes
        -----
        .. Special attention should be payed to how the "archive" is named.
            Changes might be necessary depending on how this is named or renamed

        Potential improvement
        -----
        ..

        Returns
        -------
        Nothing.
        """

        for f in existingFile:
            if "$" not in f:
                if not os.path.exists(
                    path_data + SLSH + "_archive" + SLSH + f.split(SLSH)[-1]
                ):
                    try:
                        shutil.copy(
                            f, path_data + SLSH + "_archive" + SLSH + f.split(SLSH)[-1]
                        )
                        print("file copied")
                        os.remove(f)
                        print("file removed")
                    except:
                        pass
                else:
                    try:
                        os.remove(f)
                    except:
                        pass

    def drop_table(self, table_name: str = "alembic_version", db_uri: str = "") -> None:
        engine = create_engine(db_uri)
        sql = text(f"DROP TABLE {table_name};")

        try:
            result = engine.execute(sql)
        except:
            pass

    def init_db(self) -> None:
        """This function is used to create migration files and to manage the versioning"""

        vrsns_path = self.script_location + "/versions"

        if self.is_sqlite:
            if not os.path.exists(self.db_file_path):
                self.create_connection(self.db_file_path)

        has_changes = self.check_for_migrations()

        try:
            if not "alembic" in sys.modules["__main__"].__file__:
                if has_changes:
                    if self.create_migration_file and self.run_migration:
                        if (
                            len(
                                [
                                    x
                                    for x in glob(vrsns_path + "/*.py")
                                    if f'{time.strftime("%Y%m%d", time.gmtime())}{"_migration"}'
                                    in x
                                ]
                            )
                            != 0
                        ):
                            print(
                                "Migration file already exists. Please run migration by setting the appropriate parameter!"
                            )
                        else:
                            self.create_migrations(
                                f'{time.strftime("%Y%m%d", time.gmtime())}{" migration"}'
                            )
                        self.run_migrations()

                    elif self.create_migration_file and not self.run_migration:
                        if (
                            len(
                                [
                                    x
                                    for x in glob(vrsns_path + "/*.py")
                                    if f'{time.strftime("%Y%m%d", time.gmtime())}{"_migration"}'
                                    in x
                                ]
                            )
                            != 0
                        ):
                            print(
                                "Migration file already exists. Please run migration by setting the appropriate parameter!"
                            )
                        else:
                            self.create_migrations(
                                f'{time.strftime("%Y%m%d", time.gmtime())}{" migration"}'
                            )

                    elif not self.create_migration_file and self.run_migration:
                        try:
                            self.run_migrations()
                        except Exception as e:
                            print(str(e))
                            print(
                                "The migration file with the revision id",
                                str(e),
                                "is missing. Check all the migration files to ensure one of them is not missing and try again.",
                            )

                    else:
                        pass

        except Exception as e:
            print(str(e))

            if "Target database is not" in str(e):
                # self.to_archive(path_data=vrsns_path, existingFile=glob(vrsns_path+'/*.py'))
                if not "alembic" in sys.modules["__main__"].__file__:
                    if has_changes:
                        if self.create_migration_file and self.run_migration:
                            if (
                                len(
                                    [
                                        x
                                        for x in glob(vrsns_path + "/*.py")
                                        if f'{time.strftime("%Y%m%d", time.gmtime())}{"_migration"}'
                                        in x
                                    ]
                                )
                                != 0
                            ):
                                print(
                                    "Migration file already exists. Please run migration by setting the appropriate parameter!"
                                )
                            else:
                                if self.drop_alembic_stamp_head:
                                    # self.drop_table(table_name="alembic_version", db_uri=f"sqlite:///{self.db_file_path}")
                                    self.stamp_revision_head(
                                        purge=True
                                    )  ### forcefully stamp the head of the last known revision as the downgrade of this upgrade

                                self.create_migrations(
                                    f'{time.strftime("%Y%m%d", time.gmtime())}{" migration"}'
                                )
                            self.run_migrations()

                        elif self.create_migration_file and not self.run_migration:
                            if (
                                len(
                                    [
                                        x
                                        for x in glob(vrsns_path + "/*.py")
                                        if f'{time.strftime("%Y%m%d", time.gmtime())}{"_migration"}'
                                        in x
                                    ]
                                )
                                != 0
                            ):
                                print(
                                    "Migration file already exists. Please run migration by setting the appropriate parameter!"
                                )
                            else:
                                if self.drop_alembic_stamp_head:
                                    # self.drop_table(table_name="alembic_version", db_uri=f"sqlite:///{self.db_file_path}")
                                    self.stamp_revision_head(
                                        purge=True
                                    )  ### forcefully stamp the head of the last known revision as the downgrade of this upgrade

                                self.create_migrations(
                                    f'{time.strftime("%Y%m%d", time.gmtime())}{" migration"}'
                                )

                        elif not self.create_migration_file and self.run_migration:
                            try:
                                self.run_migrations()
                            except Exception as e:
                                print(str(e))
                                print(
                                    "The migration file with the revision id",
                                    str(e),
                                    "is missing. Check all the migration files to ensure one of them is not missing and try again.",
                                )

                        else:
                            pass

    def check_for_migrations(self) -> bool:
        #### This function requires alembic version > 1.9
        config = Config()
        config.set_main_option("sqlalchemy.url", self.uri)
        config.set_main_option("script_location", self.script_location)

        try:
            command.check(config)
            return False
        except:
            return True

    def has_db_revisions(self, path_to_file: str) -> bool:
        with open(path_to_file) as f:
            contents = f.read()

        # Define regular expressions to match the entire function bodies
        upgrade_pattern = re.compile(
            r"def upgrade\(\).*?^\s*# ### end Alembic commands ###",
            re.MULTILINE | re.DOTALL,
        )
        downgrade_pattern = re.compile(
            r"def downgrade\(\).*?^\s*# ### end Alembic commands ###",
            re.MULTILINE | re.DOTALL,
        )

        # Use the regular expressions to extract the function bodies from the file contents
        upgrade_match = upgrade_pattern.search(contents)
        downgrade_match = downgrade_pattern.search(contents)

        if upgrade_match and downgrade_match:
            upgrade_body = upgrade_match.group(0)
            downgrade_body = downgrade_match.group(0)

            if "pass" not in upgrade_body and "pass" not in downgrade_body:
                print("Both functions contain code other than 'pass'.")
                return True
            else:
                print("At least one function contains only 'pass'.")
                return False
        else:
            print("Could not find both functions in the file.")
            return False

    def create_migrations(self, message: str) -> None:
        namespace_revision = CommandLine().parser.parse_args(
            ["revision", "--autogenerate"]
        )
        config = Config(cmd_opts=namespace_revision)
        config.set_main_option("sqlalchemy.url", self.uri)
        config.set_main_option("script_location", self.script_location)
        command.revision(config, message=message, autogenerate=True)

    def stamp_revision_head(self, revision: str = "head", purge: bool = True) -> None:
        ### stamping the head version will update the version_num in the respective database
        config = Config()
        config.set_main_option("sqlalchemy.url", self.uri)
        config.set_main_option("script_location", self.script_location)
        # prepare and run the command
        sql = False
        tag = None
        command.stamp(config, revision, sql=sql, tag=tag, purge=purge)

    def run_migrations(self) -> None:
        # namespace_revision = CommandLine().parser.parse_args(["upgrade", "head"])
        config = Config()
        config.set_main_option("sqlalchemy.url", self.uri)
        config.set_main_option("script_location", self.script_location)
        command.upgrade(config, "head")
