import csv
import json
import os

import psycopg2
import psycopg2.extras
import re

from datetime import datetime
from dotenv import load_dotenv
from psycopg2 import sql
from common.logger import Logger
from common.typeish import SearchTaskBody, ExportTaskBody
from common.util import local_pg_params
from contextlib import closing
from typing import List, Dict

load_dotenv()
logger = Logger(__name__)


def make_asset_fts_queries(body: SearchTaskBody, conn: psycopg2.extensions.connection):
    fts_queries: List[Dict[str, str]] = []

    for asset in body.assets:
        query = sql.SQL(
            "SELECT "
            "repo_id, repo_name, well_id, suite, tag, doc, {field} as asset "
            "FROM {table} a "
            "WHERE "
            "1=1 AND"
        ).format(
            field=sql.Literal(asset),
            table=sql.Identifier(asset),
        )

        # query += sql.SQL(" a.suite IN ({})").format(
        #     sql.SQL(",").join(map(sql.Literal, body.suites))
        # )
        query += sql.SQL(" a.suite in ('petra')")

        # screen out blanks; wildcard chars treated literally here
        is_valid_tag = (
            body.tag and isinstance(body.tag, str) and re.search(r"\S", body.tag)
        )
        if is_valid_tag:
            query += sql.SQL(" AND a.tag = {}").format(sql.Literal(body.tag))

        # screen out terms comprised of only wildcards/spaces
        is_valid_terms = (
            body.terms
            and isinstance(body.terms, str)
            and not re.match(r"^[*?\s]+$", body.terms)
        )
        if is_valid_terms:
            terms = [
                (
                    re.sub(r"\*", ":*", re.sub(r"\?", "_", term))
                    if re.search(r"[*?]", term)
                    else term
                )
                for term in re.split(r"\s+", body.terms)
                if term.strip()
            ]
            if terms:
                tsquery = " & ".join(terms)
                query += sql.SQL(" AND a.ts @@ to_tsquery('english', {})").format(
                    sql.Literal(tsquery)
                )

        fts_queries.append({"sql": query.as_string(conn), "asset": asset})

    # print("=========search q==================================")
    # for q in fts_queries:
    #     print(q)
    #     # s = q["sql"].as_string(conn)  # not kosher, just checking
    #     # print(s)
    # print("===============++++++++============================")
    return fts_queries


def search_local_pg(supabase, body: SearchTaskBody) -> str:
    params = local_pg_params()
    conn: psycopg2.extensions.connection = psycopg2.connect(**params)

    fts_queries: List[Dict[str, str]] = make_asset_fts_queries(body, conn)

    limit = 100
    summary = []

    for q in fts_queries:

        summary.append({"asset": q["asset"], "sql": q["sql"]})

        with conn.cursor() as cur:
            # query = q["sql"] if body.save_to_store else q["sql"] + " LIMIT 100"
            cur.execute(q["sql"] + f" LIMIT {limit}")
            res = cur.fetchall()

            cur.execute(f"SELECT COUNT(*) FROM ({q["sql"]}) AS subquery;")
            total_hits = cur.fetchone()[0]

            for d in summary:
                if d["asset"] == q["asset"]:
                    d["total_hits"] = total_hits

            # if total_hi
            # hits > 100:
            #     print("TTTTTTTTTTTTTTTTTT")
            #     print("more than 100 hits. want to save to file?")
            #     print("TTTTTTTTTTTTTTTTTT")

            # TODO: send to search_results instead of message
            # logger.send_message(
            #     directive="storage_prompt",
            #     data={"note": f"fts search yields: {total_hits} hits. Save?"},
            #     workflow="search",
            # )

        hits = (
            [
                {
                    "search_id": body.search_id,
                    "directive": "search_result",
                    "asset": q["asset"],
                    "active": True,
                    "search_body": body.to_dict(),
                    "sql": q["sql"],
                    "user_id": body.user_id,
                }
            ]
            if cur.rowcount == 0
            else [
                {
                    "search_id": body.search_id,
                    "directive": "search_result",
                    "asset": q["asset"],
                    "active": True,
                    "search_body": body.to_dict(),
                    "sql": q["sql"],
                    "user_id": body.user_id,
                    "repo_id": row[0],
                    "repo_name": row[1],
                    "well_id": row[2],
                    "suite": row[3],
                    "tag": row[4],
                    "doc": row[5],
                }
                for row in res
            ]
        )

        logger.send_message(
            directive="note",
            data={"note": f"fts for " f"{q["asset"]} yields: {len(hits)} hits"},
            workflow="search",
        )

        if int(total_hits) > 0:
            supabase.table("search_result").upsert(hits).execute()

    supabase.table("search_result").insert(
        {
            "search_id": body.search_id,
            "user_id": body.user_id,
            "directive": "storage_prompt",
            "search_body": summary,
        }
    ).execute()

    return "maybe donezo"


def get_output_file(task):
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    file_extension = ".csv" if task.file_format == "csv" else ".json"
    output_file = f"{task.asset}_{timestamp}_{task.user_id}{file_extension}"
    return output_file


def query_to_file(task: ExportTaskBody):
    """
    Execute a SQL query and write the results to a CSV file.

    Args:
        task: ExportTaskBody

    Returns:
        None
    """
    batch_size = 1000
    output_file = get_output_file(task)
    output_path = os.path.join(os.environ.get("EXPORT_DIR"), output_file)

    try:
        params = local_pg_params()
        with closing(psycopg2.connect(**params)) as conn:
            with closing(conn.cursor(cursor_factory=psycopg2.extras.DictCursor)) as cur:
                cur.execute(task.sql)

                first_row = cur.fetchone()
                if not first_row:
                    print("No data returned from query.")
                    return

                # Because we may select more than just doc
                colnames = [desc[0] for desc in cur.description]
                doc_index = colnames.index("doc")

                if task.file_format == "csv":
                    # rows = []

                    first_doc = first_row[doc_index]

                    header_keys = []
                    for key1, value1 in first_doc.items():
                        if isinstance(value1, dict):
                            for key2 in value1.keys():
                                header_keys.append(f"{key1}__{key2}")
                        else:
                            header_keys.append(key1)

                    with open(output_path, mode="w", newline="") as csvfile:
                        csv_writer = csv.writer(csvfile)

                        csv_writer.writerow(header_keys)

                        while True:
                            rows = cur.fetchmany(size=batch_size)
                            if not rows:
                                break
                            for row in rows:
                                csv_row = []
                                doc = row[doc_index]
                                print("--------------")
                                for key in header_keys:
                                    key_parts = key.split("__")
                                    value = doc
                                    for part in key_parts:
                                        if part in value:
                                            value = value[part]
                                            if isinstance(value, list):
                                                value = json.dumps(value)
                                        else:
                                            value = ""
                                    csv_row.append(value)
                                csv_writer.writerow(csv_row)

                        #
                        # # Write the first row to the CSV file
                        # csv_row = (
                        #     [json_data.get(key) for key in json_keys]
                        #     if json_data
                        #     else [None] * len(json_keys)
                        # )
                        # csv_writer.writerow(csv_row)
                        #
                        # # Fetch and write in batches

                        ###
                        # while True:
                        #     rows = cur.fetchmany(size=batch_size)
                        #     if not rows:
                        #         break
                        #     for row in rows:
                        #         json_data = row[doc_index]

                        ###
                        #         csv_row = (
                        #             [json_data.get(key) for key in json_keys]
                        #             if json_data
                        #             else [None] * len(json_keys)
                        #         )
                        #         csv_writer.writerow(csv_row)

                elif task.file_format == "json":
                    data = []

                    while True:
                        rows = cur.fetchmany(size=batch_size)
                        if not rows:
                            break
                        for row in rows:
                            json_data = row[doc_index]
                            data.append(json_data if json_data else {})

                    with open(output_path, mode="w") as jsonfile:
                        json.dump(data, jsonfile, indent=4)

        print(f"Data successfully written to {output_path}")

    except Exception as e:
        import traceback

        print(f"An error occurred: {e}")
        traceback.print_exc()
