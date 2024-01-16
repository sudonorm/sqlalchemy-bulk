sqlalchemy-bulk
==================

*Perform Create, Update, Read, and Delete operations easily using the SQLAlchemy orm*

---

Installation
-------------

To install via pip::

    pip install sqlalchemybulk

Or download the source code and install manually::

    git clone https://github.com/sudonorm/sqlalchemy-bulk.git
    cd sqlalchemy-bulk/
    python -m pip install .

Or download the dource code and use the `setup.py` file::

    git clone https://github.com/sudonorm/sqlalchemy-bulk.git
    cd sqlalchemy-bulk/
    python setup.py install

Basic usage
-----------

.. code:: python

    from sqlalchemybulk.crud_helper_funcs import UploadData, DownloadData, DeleteData
    import pandas as pd
    from sqlalchemy import select, delete
    from tests.sample_data.dataModel import engine
    from tests.sample_data import dataModel

    ## Insert (Create) or Update
    upload_data = UploadData(engine=engine)
    returned_ids = upload_data.upload_info_atomic(dbTable='dataModel.Address', df=df, unique_idx_elements=['name', 'postalZip'], column_update_fields=['address', 'country', 'suptext', 'numberrange', 'currency', 'alphanumeric'])

    ## Download (Read)
    download_data = DownloadData(engine=engine)

    ### query full table
    query = select(dataModel.Address)
    result = download_data.download_info_using_session(statement=query)

    ### query with filter
    query = select(dataModel.Address).where(dataModel.Address.postalZip == "3778")
    result = download_data.download_info_using_session(statement=query)

    ## Delete
    delete_data = DeleteData(engine=engine)
    query = delete(dataModel.Address).where(dataModel.Address.postalZip == "15143")
    delete_data.delete_data_on_condition(dbTable="dataModel.Address", statement=query)

Bugs, requests, questions, etc.
-------------------------------

Please create an `issue on GitHub <https://github.com/sudonorm/sqlalchemy-bulk/issues>`_.
