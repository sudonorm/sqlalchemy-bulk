from __future__ import annotations

import inspect as sys_inspect
from sqlalchemy import create_engine, event, inspect, Index, UniqueConstraint
from sqlalchemy.orm import declarative_base, registry, relationship, sessionmaker
from sqlalchemy import (
    Column,
    Integer,
    String,
    ForeignKey,
    Table,
    MetaData,
    Boolean,
    BigInteger,
    DateTime,
    Date,
    Numeric,
    Text,
    Float,
)

from dataclasses import dataclass, field
from typing import List, Optional, Union
from sqlalchemy.dialects.postgresql import UUID

from sqlalchemy import types
from sqlalchemy.dialects.mysql.base import MSBinary
import uuid
import subprocess

import os
import json
import re
import shutil
from pathlib import Path
import builtins
import sys

home_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(home_dir)

from sqlalchemybulk.manager import Migrate, Connection
from sqlalchemybulk.helper_functions import HelperFunctions

from sys import platform as pltfrm_type

from dotenv import load_dotenv

load_dotenv()  # take environment variables from .env.

CREATE_MIGRATION_FILE = True
RUN_MIGRATION = True
DROP_ALEMBIC_TABLE_AND_STAMP_HEAD = False
IS_SQLITE = True

SLSH = os.sep

BASEPATH = home_dir

helper_funcs = HelperFunctions()
con = Connection()

mapper_registry = registry()
Base = mapper_registry.generate_base()
db_uri = os.getenv("DB_URI")
alembic_path = os.getenv("ALEMBIC_PATH")

script_location = f'{alembic_path}{os.sep}{"alembic"}'

if IS_SQLITE:
    db_uri = os.getenv("SQLITE_DB_PATH")
    sqlite_file_path = db_uri
    sqlite_file = db_uri
    db_uri = f"sqlite:///{sqlite_file}"

else:
    sqlite_file_path = ""

engine = create_engine(db_uri, echo=False)

##################################

SessionLocal = sessionmaker(bind=engine)

## replace Tables here


class Address(Base):
    __tablename__ = "address"

    id = Column(Integer, primary_key=True)
    name = Column(Text, nullable=False)
    address = Column(Text, nullable=False)
    postalZip = Column(Text, nullable=False)
    country = Column(Text, nullable=False)
    suptext = Column(Text, nullable=False)
    numberrange = Column(Integer, nullable=False)
    currency = Column(Text, nullable=False)
    alphanumeric = Column(Text, nullable=False)

    __table_args__ = (
        UniqueConstraint(name, postalZip, name="u_name_postalzip"),
        Index("idx_name_zip", "name", "postalZip", unique=True),
    )


####### DO NOT DELETE ######
migrate = Migrate(
    script_location=script_location,
    uri=db_uri,
    is_sqlite=IS_SQLITE,
    db_file_path=sqlite_file_path,
    create_migration_file=CREATE_MIGRATION_FILE,
    run_migration=RUN_MIGRATION,
    drop_alembic_stamp_head=DROP_ALEMBIC_TABLE_AND_STAMP_HEAD,
)
if migrate.check_for_migrations():
    migrate.init_db()
