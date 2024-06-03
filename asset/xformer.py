import re
import math
import struct
from datetime import datetime


def xformer(xform_args):
    func_name, row, col, data_type, arg, purr_delimiter, purr_null = xform_args

    # purr_delimiter = "__purrDELIMITER__"
    # purr_null = "__purrNULL__"

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
        # try:
        #     return row[col].hex()
        # except (AttributeError, TypeError):
        #     print("ERROR")
        #     return None
        return row[col].hex()

    if func_name == "delimited_array_with_nulls":

        values = row[col].split(purr_delimiter)
        return [ensure_type(data_type, v) if v != purr_null else None for v in values]

    if func_name == "decode_depth_registration":

        reg_points = []
        buf = bytearray(row[col])
        for i in range(12, len(buf), 28):
            depth_bytes = buf[i : i + 8]  # 64-bit float (double)
            depth = struct.unpack("d", depth_bytes)[0]
            pixel_bytes = buf[i + 12 : i + 16]  # 32-bit integer
            pixel = struct.unpack("i", pixel_bytes)[0]
            reg_points.append({"depth": depth, "pixel": pixel})
        return reg_points

    if func_name == "decode_curve_values":

        curve_vals = []
        buf = bytearray(row[col])
        for i in range(2, len(buf), 4):
            cval_bytes = buf[i : i + 4]  # 32 bit float
            cval = struct.unpack("<f", cval_bytes)[0]
            curve_vals.append(cval)
        return curve_vals

    else:

        if data_type not in ("object", "string", "number", "date"):
            print("--------NEED TO ADD XFORM-------->", data_type)

        return ensure_type(data_type, row[col])


def doc_post_processor():
    print("here is doc post processor")


"""
    case "blob_to_hex":
     return (() => {
       try {
         return Buffer.from(obj[key]).toString("hex");
       } catch (error) {
         console.log("ERROR", error);
         return;
       }
     })();

"""
