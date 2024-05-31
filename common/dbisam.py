import pyodbc
import os
import re
from retry import retry
from common.logger import Logger
from common.util import normalize_path, RetryException
from common.typeish import DBISAMConn

from typing import List, Dict, Any

logger = Logger(__name__)

# TODO: make this context aware, use "with..."


# @basic_log
# @retry(RetryException, tries=5)
def db_exec(
        conn: dict | DBISAMConn, sql: List[str] or str
) -> List[Dict[str, Any]] | List[List[Dict[str, Any]]]:

    if type(conn) is DBISAMConn:
        conn = conn.to_dict()

    cursor = None
    try:
        connection = pyodbc.connect(**conn)
        cursor = connection.cursor()

        if isinstance(sql, str):
            results = []
            cursor.execute(sql)
            columns = [col[0] for col in cursor.description]
            for row in cursor.fetchall():
                res = dict(zip(columns, row))
                results.append(res)
            # return {'rowcount': int(cursor.rowcount), 'data': results}
            return results

        if isinstance(sql, list):
            multi = []
            for s in sql:
                results = []
                cursor.execute(s)
                columns = [col[0] for col in cursor.description]
                for row in cursor.fetchall():
                    res = dict(zip(columns, row))
                    results.append(res)
                multi.append(results)

                # multi.append({
                #     'rowcount': int(cursor.rowcount),
                #     'data': results
                # })

            return multi
    # except pyodbc.OperationalError as oe:
    #     if re.search(r"Database name not unique", str(oe)):
    #         logger.exception(oe)
    #         conn.pop("dbf")
    #         raise RetryException from oe
    except Exception as ex:
        logger.exception(ex)
        raise ex

    finally:
        if cursor:
            cursor.close()
        # if connection:
        #     connection.close()


def make_conn_params(repo_path: str) -> dict:
    params = {
        "driver": os.environ.get("PETRA_DRIVER"),
        "catalogname": normalize_path(os.path.join(repo_path, "DB")),
    }
    return params
