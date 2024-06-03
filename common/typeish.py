import re
from common.util import hostname, SUITE
from dataclasses import dataclass, field, asdict
from typing import Dict, List, Any, Optional


@dataclass
class DBISAMConn:
    driver: str
    catalogname: str

    def to_dict(self):
        return asdict(self)


@dataclass
class Message:
    user_id: str
    worker: str
    data: Optional[str] = None  # actually JSON
    directive: Optional[str] = None
    repo_id: Optional[str] = None
    workflow: Optional[str] = None

    def to_dict(self):
        return asdict(self)


# BATCHER #####################################################################


@dataclass
class BatcherTaskBody:
    asset: str
    chunk: int
    cron: str
    id: int
    recency: int
    repo_fs_path: str
    repo_id: str
    repo_name: str
    suite: str
    tag: str
    where_clause: str

    def to_dict(self):
        return asdict(self)


@dataclass
class BatcherTask:
    body: BatcherTaskBody
    directive: str
    id: int
    status: str
    worker: str

    def to_dict(self):
        task_dict = asdict(self)
        task_dict["body"] = self.body.to_dict()
        return task_dict


# LOADER ######################################################################


@dataclass
class LoaderTaskBody:
    asset: str
    asset_id_keys: List[str]
    batch_id: str
    conn: DBISAMConn
    prefixes: Dict[str, str]
    purr_delimiter: Optional[str]
    purr_null: Optional[str]
    repo_id: str
    repo_name: str
    selector: str
    suite: str
    tag: str
    well_id_keys: List[str]
    xforms: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self):
        body_dict = asdict(self)
        body_dict["conn"] = self.conn.to_dict()
        return body_dict


@dataclass
class LoaderTask:
    body: LoaderTaskBody
    directive: str
    id: int
    status: str
    worker: str

    def to_dict(self):
        task_dict = asdict(self)
        task_dict["body"] = self.body.to_dict()
        return task_dict


# RECON #######################################################################


@dataclass
class ReconTaskBody:
    # ggx_host: str
    recon_root: str
    suite: str

    def to_dict(self):
        return asdict(self)


@dataclass
class ReconTask:
    body: ReconTaskBody
    directive: str
    id: int
    status: str
    worker: str

    def to_dict(self):
        task_dict = asdict(self)
        task_dict["body"] = self.body.to_dict()
        return task_dict


# REPO ########################################################################
# @dataclass
# class ConnAux:
#     ggx_host: str
#
#     def to_dict(self):
#         return asdict(self)


@dataclass
class Repo:
    id: str
    name: str
    fs_path: str
    conn: DBISAMConn
    # conn_aux: ConnAux
    suite: str
    well_count: int
    # wells_with_completion: int
    wells_with_core: int
    wells_with_dst: int
    wells_with_formation: int
    wells_with_ip: int
    wells_with_perforation: int
    wells_with_production: int
    wells_with_raster_log: int
    wells_with_survey: int
    wells_with_vector_log: int
    wells_with_zone: int
    storage_epsg: int
    storage_name: str
    display_epsg: int
    display_name: str
    files: int
    directories: int
    bytes: int
    repo_mod: str
    outline: List[List[float]] = field(default_factory=list)
    # active: Optional[bool] = True

    def to_dict(self):
        repo_dict = asdict(self)
        repo_dict["conn"] = self.conn.to_dict()
        # repo_dict["conn_aux"] = self.conn_aux.to_dict()
        return repo_dict


# SEARCH ######################################################################
@dataclass
class SearchTaskBody:
    tag: str
    terms: str
    assets: List[str]
    suites: List[str]
    user_id: str
    search_id: int

    def to_dict(self):
        return asdict(self)


@dataclass
class SearchTask:
    body: SearchTaskBody
    directive: str
    id: int
    status: str
    worker: str

    def to_dict(self):
        task_dict = asdict(self)
        task_dict["body"] = self.body.to_dict()
        return task_dict


# EXPORT ######################################################################
@dataclass
class ExportTaskBody:
    asset: str
    file_format: str
    sql: str
    total_hits: int
    user_id: str

    def to_dict(self):
        return asdict(self)


@dataclass
class ExportTask:
    body: ExportTaskBody
    directive: str
    id: int
    status: str
    worker: str

    def to_dict(self):
        task_dict = asdict(self)
        task_dict["body"] = self.body.to_dict()
        return task_dict


# #############################################################################


def validate_repo(payload: dict):
    # remove supabase audit columns
    unwanted_keys = ["active", "created_at", "touched_at", "updated_at"]
    for key in unwanted_keys:
        if key in payload:
            payload.pop(key)

    return Repo(
        id=payload["id"],
        name=payload["name"],
        fs_path=payload["fs_path"],
        conn=DBISAMConn(**payload["conn"]),
        # conn_aux=ConnAux(**payload["conn_aux"]),
        suite=payload["suite"],
        well_count=payload["well_count"],
        # wells_with_completion=payload["wells_with_completion"],
        wells_with_core=payload["wells_with_core"],
        wells_with_dst=payload["wells_with_dst"],
        wells_with_formation=payload["wells_with_formation"],
        wells_with_ip=payload["wells_with_ip"],
        wells_with_perforation=payload["wells_with_perforation"],
        wells_with_production=payload["wells_with_production"],
        wells_with_raster_log=payload["wells_with_raster_log"],
        wells_with_survey=payload["wells_with_survey"],
        wells_with_vector_log=payload["wells_with_vector_log"],
        wells_with_zone=payload["wells_with_zone"],
        storage_epsg=payload["storage_epsg"],
        storage_name=payload["storage_name"],
        display_epsg=payload["display_epsg"],
        display_name=payload["display_name"],
        files=payload["files"],
        directories=payload["directories"],
        bytes=payload["bytes"],
        repo_mod=payload["repo_mod"],
        outline=payload["outline"],
    )


def is_valid_status(status: str):
    valid_statuses = ["PENDING", "PROCESSING", "FAILED"]
    return status.upper() in valid_statuses


def validate_message(payload: dict):
    try:
        return Message(
            user_id=payload["user_id"],
            worker=payload["worker"],
            data=payload["data"],
            directive=payload["directive"],
            repo_id=payload["repo_id"],
            workflow=payload["workflow"],
        )

    except KeyError as ke:
        print(ke)
        return None


def validate_task(payload: dict):
    """
    Convert json payloads into their dataclass analog
    :param payload:
    :return:
    """
    try:
        if payload["record"]:
            if "status" in payload["record"] and not is_valid_status(
                payload["record"]["status"]
            ):
                raise Exception("Unexpected status in payload")

            if (
                payload["record"]["worker"] == hostname()
                and payload["record"]["status"] == "PENDING"
                and (
                    (
                        "suite" in payload["record"]["body"]
                        and payload["record"]["body"]["suite"] == SUITE
                    )
                    or (
                        "suites" in payload["record"]["body"]
                        and SUITE in payload["record"]["body"]["suites"]
                    )
                    or (
                        "sql" in payload["record"]["body"]
                        and re.search(SUITE, payload["record"]["body"]["sql"])
                    )
                )
            ):
                task = payload["record"]

                if task.get("directive") == "batcher":
                    return BatcherTask(
                        body=BatcherTaskBody(**task["body"]),
                        directive=task["directive"],
                        id=task["id"],
                        status=task["status"],
                        worker=task["worker"],
                    )

                if task.get("directive") == "loader":
                    return LoaderTask(
                        body=LoaderTaskBody(**task["body"]),
                        directive=task["directive"],
                        id=task["id"],
                        status=task["status"],
                        worker=task["worker"],
                    )

                if task.get("directive") == "recon":
                    return ReconTask(
                        body=ReconTaskBody(**task["body"]),
                        directive=task["directive"],
                        id=task["id"],
                        status=task["status"],
                        worker=task["worker"],
                    )

                if task.get("directive") == "search":
                    # NOTE: task.body.search_id = task.id
                    task["body"]["search_id"] = task["id"]
                    return SearchTask(
                        body=SearchTaskBody(**task["body"]),
                        directive=task["directive"],
                        id=task["id"],
                        status=task["status"],
                        worker=task["worker"],
                    )

                if task.get("directive") == "export":
                    return ExportTask(
                        body=ExportTaskBody(**task["body"]),
                        directive=task["directive"],
                        id=task["id"],
                        status=task["status"],
                        worker=task["worker"],
                    )

    except KeyError as ke:
        print(ke)
        return None
    except Exception as e:
        print(e)
