import simplejson as json
import re
import time
from common.logger import Logger
from common.dbisam import db_exec
from common.util import hashify, hostname
from typing import List

# from common.debugger import debugger

logger = Logger(__name__)


def dotify_columns(prefixes, where_clause):
    prefix_pattern = r"^(" + r"|".join(key for key in prefixes) + r")"
    regex = re.compile(prefix_pattern)

    def swap_dot(match):
        prefix = match.group()
        return prefix.replace("_", ".", 1)

    tokens = (regex.sub(swap_dot, token) for token in where_clause.split())
    return " ".join(tokens)


def make_where_clause(body):
    where_parts = ["WHERE 1=1"]
    if len(body.where_clause.strip()) > 0:
        where_parts.append(body.where_clause)
    if body.recency > 0:
        now = time.time() / 86400 + 25569  # ("excel" date)
        where_parts.append(f"w.chgdate >= {now - body.recency} AND w.chgdate < 1E30")
    where_clause = " AND ".join(where_parts)
    return where_clause


# @debugger
def fetch_id_list(repo, id_sql):
    """
    :param repo: The target project/repo
    :param id_sql: SQL to collect just ids from the asset
    :return: Results will be either be a single "keylist"
    [{keylist: ["1-62", "1-82", "2-83", "2-84"]}]
    or a list of key ids
    [{key: "1-62"}, {key: "1-82"}, {key: "2-83"}, {key: "2-84"}]
    Force int() or str(); the typical case is a list of int
    """

    def int_or_string(obj):
        try:
            return int(obj)
        except ValueError:
            return f"'{str(obj).strip()}'"

    res = db_exec(repo.conn, id_sql)

    ids = []
    if "keylist" in res[0]:
        ids = res[0]["keylist"].split(",")
    elif "key" in res[0]:
        ids = [x["key"] for x in res]
    else:
        print("key or keylist missing; cannot make id list")

    return [int_or_string(i) for i in ids]


def chunk_ids(ids, chunk):
    """
    [621, 826, 831, 834, 835, 838, 846, 847, 848]
    ...with chunk=4...
    [[621, 826, 831, 834], [835, 838, 846, 847], [848]]

    ["1-62", "1-82", "2-83", "2-83", "2-83", "2-83", "2-84", "3-84", "4-84"]
    ...with chunk=4...
    [
        ['1-62', '1-82'],
        ['2-83', '2-83', '2-83', '2-83', '2-84'],
        ['3-84', '4-84']
    ]
    Note how the group of 2's is kept together, even if it exceeds chunk=4

    :param ids: This is usually a list of wsn ints: [11, 22, 33, 44] but may
        also be "compound" str : ['1-11', '1-22', '1-33', '2-22', '2-44'].
    :param chunk: The preferred batch size to process in a single query
    :return: List of id lists
    """
    id_groups = {}

    for item in ids:
        left = str(item).split("-")[0]
        if left not in id_groups:
            id_groups[left] = []
        id_groups[left].append(item)

    result = []
    current_subarray = []

    for group in id_groups.values():
        if len(current_subarray) + len(group) <= chunk:
            current_subarray.extend(group)
        else:
            result.append(current_subarray)
            current_subarray = group[:]

    if current_subarray:
        result.append(current_subarray)

    return result


def make_id_in_clauses(identifier_keys, ids):
    if len(identifier_keys) == 1 and str(ids[0]).replace("'", "").isdigit():
        no_quotes = ",".join(str(i).replace("'", "") for i in ids)
        # print(f"{identifier_keys[0]} IN ({no_quotes})")
        return f"{identifier_keys[0]} IN ({no_quotes})"
    else:
        idc = " || '-' || ".join(f"CAST({i} AS VARCHAR(10))" for i in identifier_keys)
        return f"{idc} IN ({','.join(ids)})"


def batcher(body, dna, repo) -> List[dict]:
    """
    Due to limitations of DBISAM, we cannot use schemes like "OFFSET...FETCH"
    or "SELECT TOP x START AT y" to select subsets. Instead, we group the ID
    fields and use an IN clause.

    :param body:
    :param dna:
    :param repo:
    :return:
    """
    logger.send_message(
        directive="note",
        repo_id=repo.id,
        data={"note": f"define batcher tasks: {body.asset} @ {repo.fs_path}"},
        workflow="load",
    )

    # dna...
    select: str = dna.get("select")
    identifier: str = dna.get("identifier")
    purr_where: str = dna.get("purr_where")
    prefixes: list = dna.get("prefixes").keys()
    order: str = dna.get("order")

    # construct where clause with recency if applicable
    where = make_where_clause(body)
    # swap underscores to dots in aliases
    where = dotify_columns(prefixes, where)

    # make id sub-lists
    id_sql = identifier.replace(purr_where, where)
    ids = fetch_id_list(repo, id_sql)
    chunked_ids = chunk_ids(ids, body.chunk)

    # define chunks of selectors from id sub-lists
    selectors = []
    for c in chunked_ids:
        in_clause = make_id_in_clauses(dna.get("identifier_keys"), c)
        chunk_where = where + " AND " + in_clause
        chunk_select = select.replace(purr_where, chunk_where) + " " + order
        selectors.append(chunk_select)

    # define tasks
    tasks = []
    for selector in selectors:
        task_body = {
            "asset": body.asset,
            "tag": body.tag,
            "asset_id_keys": dna.get("asset_id_keys"),
            "batch_id": hashify(json.dumps(body.to_dict()).lower()),
            "conn": repo.conn.to_dict(),
            "suite": repo.suite,
            "post_process": dna.get("post_process"),
            "prefixes": dna.get("prefixes"),
            "purr_delimiter": dna.get("purr_delimiter"),
            "purr_null": dna.get("purr_null"),
            "repo_id": repo.id,
            "repo_name": repo.name,
            "selector": selector,
            "well_id_keys": dna.get("well_id_keys"),
            "xforms": dna.get("xforms"),
        }
        tasks.append(
            {
                "worker": hostname(),
                "directive": "loader",
                "status": "PENDING",
                "body": task_body,
            }
        )

    return tasks
