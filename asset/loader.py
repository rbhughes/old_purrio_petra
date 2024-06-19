import psycopg2
import psycopg2.extras
from common.logger import Logger
from common.dbisam import db_exec
from common.util import hashify, local_pg_params
from asset.post_processor import doc_post_processor
from asset.xformer import xformer
from typing import List

import json

logger = Logger(__name__)


ASSET_COLUMNS = ["id", "repo_id", "repo_name", "well_id", "suite", "tag", "doc"]


def make_upsert_stmt(table_name, columns) -> str:
    """
    Construct a PostgreSQL "upsert" statement for collected asset data
    :param table_name: The asset/table-name (they match)
    :param columns: Usually just ASSET_COLUMNS
    :return: a SQL string
    """
    cols = columns.copy()
    stmt = [f"INSERT INTO {table_name}"]
    stmt.append(f"({', '.join(columns)})")
    stmt.append("VALUES")
    placeholders = ", ".join(["%s"] * len(columns))
    stmt.append(f"({placeholders})")
    stmt.append("ON CONFLICT (id) DO UPDATE SET")
    cols.pop(0)
    stmt.append(", ".join([f"{col} = EXCLUDED.{col}" for col in columns]))
    return " ".join(stmt)


def pg_upserter(docs, table_name) -> None:
    """
    Upsert asset data to local PostgreSQL database. Each asset type has its own
    table, but the columns are identical.
    :param docs: A list of dicts containing json documents
    :param table_name: A str of the asset/table name (they match)
    :return: TODO
    """
    conn = None
    cursor = None
    try:
        conn = psycopg2.connect(**local_pg_params())
        cursor = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)
        psycopg2.extensions.register_adapter(dict, psycopg2.extras.Json)

        upsert_stmt = make_upsert_stmt(table_name, ASSET_COLUMNS)

        upsert_count = 0
        cursor.execute("BEGIN")

        for doc in docs:
            ordered_data = [doc.get(col) for col in ASSET_COLUMNS]
            cursor.execute(upsert_stmt, ordered_data)
            upsert_count += cursor.rowcount

        conn.commit()

        logger.send_message(
            directive="note",
            # repo_id=repo.id,
            data={"note": f"upsert: {upsert_count} of {len(docs)} {table_name}"},
            workflow="load",
        )

    except (Exception, psycopg2.Error) as error:
        logger.exception(error)
        logger.exception("rolling back pg_upserter transaction after exception")
        conn.rollback()

    finally:
        if conn:
            cursor.close()
            conn.close()


def compose_docs(data, body) -> List[dict]:
    """
    A "document" (doc) is basically a json object defined for each specific
    asset by Supabase edge functions.
    :param data: Basically a list (result set) from SQLAnywhere
    :param body: The LoaderTask body, mostly used for metadata
    :return: List of docs
    """
    docs = []

    for row in data:
        o = {}
        doc = {}

        o["id"] = hashify(
            str(body.repo_id)
            + str(body.asset)
            + str(body.suite)
            + str(body.repo_id)
            + "".join([str(row[k]) for k in body.asset_id_keys])
        )

        o["well_id"] = "-".join([str(row[k]) for k in body.well_id_keys])
        o["repo_id"] = body.repo_id
        o["repo_name"] = body.repo_name
        o["tag"] = body.tag
        o["suite"] = body.suite

        # invoke xformer
        for col, xf in body.xforms.items():
            data_type = xf.get("ts_type")
            func_name = xf.get("xform")
            purr_delimiter = body.purr_delimiter
            purr_null = body.purr_null
            xform_args = (
                func_name,
                row,
                col,
                data_type,
                None,
                purr_delimiter,
                purr_null,
            )
            row[col] = xformer(xform_args)

        # build json based on prefixes
        for prefix, table in body.prefixes.items():
            doc[table] = {}
            for key, val in row.items():
                if key.startswith(prefix):
                    new_key = key.replace(f"{prefix}", "", 1)
                    doc[table][new_key] = val

        o["doc"] = doc
        docs.append(o)

    # print(json.dumps(docs[0], indent=4))

    if body.post_process:
        for doc_proc in body.post_process:
            # TODO: verify that docs get modified in place. 35137004570000
            # doc_post_processor(docs, doc_proc)
            docs = doc_post_processor(docs, doc_proc)

    return docs


def loader(body, repo):
    """
    Main entry point for the loader/upserter
    :param body: An instance of LoaderTask
    :param repo: An instance of Repo
    :return: TODO
    """

    try:

        logger.send_message(
            directive="note",
            repo_id=repo.id,
            data={"note": f"building {body.asset} loader query @ {repo.fs_path}"},
            workflow="load",
        )

        data = db_exec(repo.conn, body.selector)
        docs = compose_docs(data, body)

        logger.send_message(
            directive="note",
            repo_id=repo.id,
            data={"note": f"composed {len(docs)} {body.asset} docs @ {repo.fs_path}"},
            workflow="load",
        )

        # print(json.dumps(docs, indent=2))
        pg_upserter(docs, body.asset)

    except Exception as error:
        logger.exception(error)
