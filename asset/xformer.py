import re
import math
import struct
import simplejson as json
from datetime import datetime, timedelta

from common.debugger import debugger


def xformer(xform_args):
    func_name, row, col, data_type, arg, purr_delimiter, purr_null = xform_args

    def memo_to_string(x):
        return ensure_type("string", bytes(x, "latin-1").decode("utf-8"))

    def excel_date(x):
        if re.match(r"1[eE]\+?30", str(x), re.IGNORECASE):
            return None
        return (datetime(1970, 1, 1) + timedelta(x - 25569)).isoformat()

    def ensure_type(dtype, val):
        if val is None:
            return None
        elif dtype == "object":
            print("UNEXPECTED OBJECT TYPE! (needs xformer)")
            print(val)
            return None
        elif dtype == "string":
            return re.sub(r"[\u0000-\u001F\u007F-\u009F]", "", str(val))
        elif dtype == "number":
            if str(val).replace(" ", "") == "":
                return None
            try:
                n = float(val)
                return n if not math.isnan(n) else None
            except ValueError:
                return None
        elif dtype == "date":
            try:
                return datetime.fromisoformat(str(val)).isoformat()
            except (ValueError, TypeError):
                return None
        else:
            print(f"ENSURE TYPE SOMETHING ELSE (xformer): {type}")
            return "XFORM ME"

    if row.get(col) is None:
        return None

    ##################################################

    if func_name == "blob_to_hex":
        return row[col].hex()

    if func_name == "delimited_array_with_nulls":
        return [
            ensure_type(data_type, v) if v != purr_null else None
            for v in row[col].split(purr_delimiter)
        ]

    if func_name == "delimited_array_of_memo":
        return [
            memo_to_string(v) if v != purr_null else None
            for v in row[col].split(purr_delimiter)
        ]

    if func_name == "delimited_array_of_hex":
        return [
            bytes(v).hex() if v != purr_null else None
            for v in row[col].split(purr_delimiter)
        ]

    if func_name == "delimited_array_of_excel_dates":
        return [
            excel_date(v) if v != purr_null else None
            for v in row[col].split(purr_delimiter)
        ]

    if func_name == "memo_to_string":
        return memo_to_string(row[col])

    if func_name == "excel_date":
        return excel_date(row[col])

    if func_name == "parse_congressional":
        b = bytes(row[col])
        cong = {
            "township": b[4:6].decode().split("\x00")[0],
            "township_ns": b[71:72].decode().split("\x00")[0],
            "range": b[21:23].decode().split("\x00")[0],
            "range_ew": b[70:71].decode().split("\x00")[0],
            "section": b[38:54].decode().split("\x00")[0],
            "section_suffix": b[54:70].decode().split("\x00")[0],
            "meridian": b[153:155].decode().split("\x00")[0],
            "footage_ref": b[137:152].decode().split("\x00")[0],
            "spot": b[96:136].decode().split("\x00")[0],
            "footage_call_ns": struct.unpack("<d", b[88:96])[0],
            "footage_call_ns_ref": struct.unpack("<h", b[76:78])[0],
            "footage_call_ew": struct.unpack("<d", b[80:88])[0],
            "footage_call_ew_ref": struct.unpack("<h", b[72:74])[0],
            "remarks": b[156:412].decode().split("\x00")[0],
        }
        return cong

    if func_name == "fmtest_recovery":
        buf = bytes(row[col])
        recoveries = [
            {
                "amount": struct.unpack("<d", buf[i : i + 8])[0],
                "units": buf[i + 8 : i + 15].decode().split("\x00")[0],
                "descriptions": buf[i + 15 : i + 36].decode().split("\x00")[0],
            }
            for i in range(0, len(buf), 36)
        ]
        return recoveries

    if func_name == "parse_zztops":
        buf = bytes(row[col])
        repeat_tops = [
            struct.unpack("<d", buf[i : i + 8])[0] for i in range(4, len(buf), 28)
        ]
        return repeat_tops

    if func_name == "pdtest_treatment":
        buf = bytes(row[col])
        treatments = [
            {
                "type": buf[i : i + 9].decode().split("\x00")[0],
                "top": struct.unpack("<d", buf[i + 9 : i + 17])[0],
                "base": struct.unpack("<d", buf[i + 17 : i + 25])[0],
                "amount1": struct.unpack("<d", buf[i + 25 : i + 33])[0],
                "units1": buf[i + 61 : i + 65].decode().split("\x00")[0],
                "desc": buf[i + 68 : i + 89].decode().split("\x00")[0],
                "agent": buf[i + 89 : i + 96].decode().split("\x00")[0],
                "amount2": struct.unpack("<d", buf[i + 33 : i + 41])[0],
                "units2": buf[i + 96 : i + 100].decode().split("\x00")[0],
                "fmbrk": struct.unpack("<d", buf[i + 41 : i + 49])[0],
                "num_stages": struct.unpack("<i", buf[i + 57 : i + 61])[0],
                "additive": buf[i + 103 : i + 110].decode().split("\x00")[0],
                "inj_rate": struct.unpack("<d", buf[i + 49 : i + 57])[0],
            }
            for i in range(0, len(buf), 110)
        ]
        return treatments

    if func_name == "logdata_digits":
        b = bytes(row[col])
        digits = [struct.unpack("<d", b[i : i + 8])[0] for i in range(0, len(b), 8)]
        return digits

    if func_name == "loglas_lashdr":
        b = [
            re.sub(r'^"|"$', "", r) for r in bytes(row[col]).decode("utf-8").split(";")
        ]
        return ensure_type("string", "\n".join(b))

    else:
        if data_type not in ("object", "string", "number", "date"):
            print("--------NEED TO ADD XFORM-------->", data_type)
        return ensure_type(data_type, row[col])
