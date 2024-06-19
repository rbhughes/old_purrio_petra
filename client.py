import pandas

# from purr_worker import PurrWorker

import pyodbc
from common.dbisam import make_conn_params

# run like this:
# python -m client

if __name__ == "__main__":
    fs_path = "D:/petra/pet_share/2022/INTERSTATE"
    sql = "select * from well"

    conn = make_conn_params(fs_path)
    # connstr = (
    #     f"DRIVER='DBISAM 4 ODBC Driver'; "
    #     f"CATALOGNAME='D:/petra/pet_share/2022/INTERSTATE/DB'"
    # )

    cnxn = pyodbc.connect(**conn)

    # data = pandas.read_sql(sql, cnxn)
    # print(type(data))

    df = pandas.read_sql_query(sql, cnxn)
    print(type(df))

    print(df.key())
    print(df.)

    # pw = PurrWorker()
    # pw.register_worker()
    # pw.start_queue_processing()
    # pw.listen()
