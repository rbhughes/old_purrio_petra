from common.dbisam import db_exec
from common.logger import Logger
from concave_hull import concave_hull

# from common.debugger import debugger
# DBISAM cannot do COUNT(DISTINCT *) and suggests using memory tables as an
# alternative. Unfortunately, they also want you to "unprepare" statements to
# release the memory locking scheme. The ODBC driver does not support this
# unprepare concept, so we get "11013 Access denied" errors everywhere.
# For example, this does not work:
#
#   DROP TABLE IF EXISTS memory\\temp;
#   SELECT DISTINCT(w.wsn) INTO memory\\temp FROM well w;
#   SELECT COUNT(wsn) AS tally from memory\\temp;
#
# We use the ODBC result's rowCount attribute as a (horrible) alternative.


NOTNULL_LONLAT = (
    "SELECT s.lon, s.lat FROM well w"
    "LEFT JOIN locat s ON s.wsn = w.wsn"
    "WHERE s.lon IS NOT NULL AND s.lat IS NOT NULL"
)

HULL_CONCAVITY = 2

##########

WELLS = "SELECT DISTINCT(wsn) AS tally FROM well"

# WELLS_WITH_COMPLETION = "SELECT COUNT(DISTINCT uwi) AS tally FROM well_completion"
# TODO: figure out how to deal with completion/perforation


# Select | Wells By Data Criteria | Mechanical | Cored Intervals | Any Cores
WELLS_WITH_CORE = (
    "SELECT DISTINCT(w.wsn) AS tally FROM well w "
    "JOIN cores c ON c.wsn = w.wsn"
)

# NOTE: Regarding DST: within Petra, doing
# [ Select | Wells By Data Criteria | Tests | Any Formation Tests (DST) ]
# yields a slightly higher count. It doesn't use f.testtype = 'D' clause?
WELLS_WITH_DST = (
    "SELECT DISTINCT(w.wsn) AS tally FROM well w "
    "JOIN fmtest f ON f.wsn = w.wsn AND f.testtype = 'D'"
)

# Select | Wells By Data Criteria | Geology (Tops) | Tops Data |
# (pick All formations on left)
# When ANY of the Selected Tops Meet the Requirements
# Requirements: If Top is Present in the Database
# ...otherwise you get a bunch of tops with no data.
WELLS_WITH_FORMATION = (
    "SELECT DISTINCT(w.wsn) AS tally FROM zflddef f "
    "JOIN zdata z ON f.fid = z.fid "
    "AND f.kind = 'T' "
    "AND z.zid = 1 "
    "AND z.z < 1E30 "
    "AND z.z IS NOT NULL "
    "JOIN well w ON z.wsn = w.wsn"
)

# Select | Wells By Data Criteria | Tests | Any Production Tests (IP)
WELLS_WITH_IP = (
    "SELECT DISTINCT(w.wsn) AS tally FROM well w "
    "JOIN pdtest p ON p.wsn = w.wsn"
)

# Select | Wells By Data Criteria | Mechanical | Any Perfs
WELLS_WITH_PERFORATION = (
    "SELECT DISTINCT(w.wsn) AS tally FROM well w "
    "JOIN perfs p ON p.wsn = w.wsn"
)

# Select | Wells By Data Criteria | Production |
# (one-doc-per-mopddef.fid)
WELLS_WITH_PRODUCTION = (
    "SELECT DISTINCT(w.wsn) AS tally FROM well w "
    "JOIN mopddata a ON a.wsn = w.wsn"
)

# TODO: confirm counts (there was variance in test proj 6669 vs 6672)
# Select | Wells By Data Criteria | Logs | Raster Logs | Calibrated Rasters
# (Find Wells With ANY Rasters)
WELLS_WITH_RASTER_LOG = (
    "SELECT DISTINCT(w.wsn) AS tally FROM well w "
    "JOIN logimage i ON w.wsn = i.wsn"
)

# Select | Wells By Data Criteria | Locations | Wells with Directional Survey
# When ANY Condition is Met
WELLS_WITH_SURVEY = (
    "SELECT DISTINCT(w.wsn) AS tally FROM well w "
    "JOIN dirsurvdata d ON d.wsn = w.wsn "
    "JOIN dirsurvdef f ON f.survrecid = d.survrecid "
    "GROUP BY w.wsn"
)

# Select | Wells By Data Criteria | Logs | Digtial Logs | Log Curves
# Any Curves At All
WELLS_WITH_VECTOR_LOG = (
    "SELECT DISTINCT(w.wsn) AS tally FROM well w "
    "JOIN logdata a ON w.wsn = a.wsn "
    "JOIN logdef f ON a.lsn = f.lsn "
    "JOIN logdatax x ON a.wsn = x.wsn AND a.lsn = x.lsn AND a.ldsn = x.ldsn"
)

# Select | Wells By Data Criteria | Zones | Zone or Tops Data
# When ANY Condition Is Met...and then this is comparable* to picking every
# Zone and Item individually
# * Probably. I tested a few, but it's too much of a pain.
WELLS_WITH_ZONE = (
    "SELECT DISTINCT(w.wsn) AS tally FROM WELL w "
    "JOIN zdata z ON z.wsn = w.wsn "
    "JOIN zonedef n ON n.zid = z.zid AND n.kind > 2 "
    "JOIN zflddef f ON f.zid = n.zid AND f.fid = z.fid"
)


"""
WELLS_WITH_CORE = "SELECT COUNT(DISTINCT uwi) AS tally FROM well_core"

WELLS_WITH_DST = (
    "SELECT COUNT(DISTINCT uwi) AS tally FROM well_test WHERE test_type = 'DST'"
)

WELLS_WITH_FORMATION = "SELECT COUNT(DISTINCT uwi) AS tally FROM well_formation"

# to match the selector for IP:
# SELECT count(DISTINCT uwi || source || run_number)
#   from well_test WHERE test_type ='IP'

WELLS_WITH_IP = (
    "SELECT COUNT(DISTINCT uwi) AS tally FROM well_test WHERE test_type = 'IP'"
)

WELLS_WITH_PERFORATION = "SELECT COUNT(DISTINCT uwi) AS tally FROM well_perforation"

WELLS_WITH_PRODUCTION = (
    "SELECT COUNT(DISTINCT uwi) AS tally FROM well_cumulative_production"
)

WELLS_WITH_RASTER_LOG = (
    "SELECT COUNT(DISTINCT(w.uwi)) AS tally FROM well w "
    "JOIN log_image_reg_log_section r ON r.well_id = w.uwi"
)

WELLS_WITH_SURVEY = (
    "SELECT COUNT(DISTINCT uwi) AS tally FROM ( "
    "SELECT uwi FROM well_dir_srvy_station "
    "UNION "
    "SELECT uwi FROM well_dir_proposed_srvy_station "
    ") x"
)

WELLS_WITH_VECTOR_LOG = "SELECT COUNT(DISTINCT wellid) AS tally FROM gx_well_curve"

WELLS_WITH_ZONE = "SELECT COUNT(DISTINCT uwi) AS tally FROM well_zone_interval"
"""

logger = Logger(__name__)


def well_counts(repo_base) -> dict:
    """
    Run a bunch of SQL counts for wells having each data type. Note that this
    is well-centric. For example, it's wells with raster logs, not a count of
    raster logs.
    :param repo_base: A stub repo dict. We just use the fs_path
    :return: dict with each count, named after the keys below
    """
    logger.send_message(
        directive="note",
        repo_id=repo_base["id"],
        data={"note": f"collecting well counts @ {repo_base["fs_path"]}"},
        workflow="recon",
    )

    counter_sql = {
        "well_count": WELLS,
        # "wells_with_completion": WELLS_WITH_COMPLETION,
        # "wells_with_core": WELLS_WITH_CORE,
        # "wells_with_dst": WELLS_WITH_DST,
        # "wells_with_formation": WELLS_WITH_FORMATION,
        # "wells_with_ip": WELLS_WITH_IP,
        # "wells_with_perforation": WELLS_WITH_PERFORATION,
        # "wells_with_production": WELLS_WITH_PRODUCTION,
        # "wells_with_raster_log": WELLS_WITH_RASTER_LOG,
        # "wells_with_survey": WELLS_WITH_SURVEY,
        # "wells_with_vector_log": WELLS_WITH_VECTOR_LOG,
        # "wells_with_zone": WELLS_WITH_ZONE,
    }

    res = db_exec(repo_base["conn"], list(counter_sql.values()))

    counts = {}

    for i, k in enumerate(counter_sql.keys()):
        counts[k] = res[i][0]["tally"] or 0

    return counts


def hull_outline(repo_base) -> dict:
    """
    https://concave-hull.readthedocs.io/en/latest/
    Note: we add a point to connect the last dot
    :param repo_base: A stub repo dict. We just use the fs_path
    :return: dict with hull (List of points)
    """
    logger.send_message(
        directive="note",
        repo_id=repo_base["id"],
        data={"note": f"building hull outline @ {repo_base["fs_path"]}"},
        workflow="recon",
    )

    res = db_exec(repo_base["conn"], NOTNULL_LONLAT)
    points = [[r["surface_longitude"], r["surface_latitude"]] for r in res]

    if len(points) < 3:
        print(f"Too few valid Lon/Lat points for polygon: {repo_base["name"]}")
        return {"outline": None}

    hull = concave_hull(points, concavity=HULL_CONCAVITY)
    first_point = hull[0]
    hull.append(first_point)
    return {"outline": hull}
