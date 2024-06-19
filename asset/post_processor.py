from typing import List


def doc_post_processor(docs: List[dict], func_name: str):
    output_docs = []

    if func_name == "aggregate_fmtest":
        """
        There are multiple DST tests per well. We would normally roll them up
        using LIST aggregation, but each test may contain multiple well
        treatments (FMTEST.recov), which are stored as BLOBs, one per test.
        LIST cannot handle BLOBs. Instead, we collect all tests and then
        aggregate them from the docs.
        """
        for input_doc in docs:
            existing_doc = next(
                (
                    output_doc
                    for output_doc in output_docs
                    if output_doc["doc"]["well"]["wsn"]
                    == input_doc["doc"]["well"]["wsn"]
                ),
                None,
            )
            if existing_doc:
                existing_doc["doc"]["fmtest"].append(input_doc["doc"]["fmtest"])
            else:
                output_doc = {
                    "id": input_doc["id"],
                    "well_id": input_doc["well_id"],
                    "repo_id": input_doc["repo_id"],
                    "repo_name": input_doc["repo_name"],
                    "suite": input_doc["suite"],
                    "tag": input_doc["tag"],
                    "doc": {
                        "fmtest": [input_doc["doc"]["fmtest"]],
                        "well": input_doc["doc"]["well"],
                    },
                }
                output_docs.append(output_doc)

    if func_name == "aggregate_pdtest":
        """
        There are multiple IP tests per well. We would normally roll them up
        using LIST aggregation, but each test may contain multiple well
        treatments (PDTEST.treat), which are stored as BLOBs, one per test.
        LIST cannot handle BLOBs. Instead, we collect all tests and then
        aggregate them from the docs.
        """
        output_docs = []
        for input_doc in docs:
            existing_doc = next(
                (
                    output_doc
                    for output_doc in output_docs
                    if output_doc["doc"]["well"]["wsn"]
                    == input_doc["doc"]["well"]["wsn"]
                ),
                None,
            )
            if existing_doc:
                existing_doc["doc"]["pdtest"].append(input_doc["doc"]["pdtest"])
            else:
                output_doc = {
                    "id": input_doc["id"],
                    "well_id": input_doc["well_id"],
                    "repo_id": input_doc["repo_id"],
                    "repo_name": input_doc["repo_name"],
                    "suite": input_doc["suite"],
                    "tag": input_doc["tag"],
                    "doc": {
                        "pdtest": [input_doc["doc"]["pdtest"]],
                        "well": input_doc["doc"]["well"],
                    },
                }
                output_docs.append(output_doc)
        return output_docs

    if func_name == "aggregate_perfs":
        output_docs = []
        for input_doc in docs:
            existing_doc = next(
                (
                    output_doc
                    for output_doc in output_docs
                    if output_doc["doc"]["well"]["wsn"]
                    == input_doc["doc"]["well"]["wsn"]
                ),
                None,
            )
            if existing_doc:
                existing_doc["doc"]["perfs"].append(input_doc["doc"]["perfs"])
            else:
                output_doc = {
                    "id": input_doc["id"],
                    "well_id": input_doc["well_id"],
                    "repo_id": input_doc["repo_id"],
                    "repo_name": input_doc["repo_name"],
                    "suite": input_doc["suite"],
                    "tag": input_doc["tag"],
                    "doc": {
                        "perfs": [input_doc["doc"]["perfs"]],
                        "well": input_doc["doc"]["well"],
                    },
                }
                output_docs.append(output_doc)
        return output_docs

    else:
        print("no matching post-processing function:", func_name)

    return output_docs


#
# def aggregate_fmtest(docs):
#     output_docs = []
#     for input_doc in docs:
#         existing_doc = next(
#             (
#                 output_doc
#                 for output_doc in output_docs
#                 if output_doc["doc"]["well"]["wsn"] == input_doc["doc"]["well"]["wsn"]
#             ),
#             None,
#         )
#         if existing_doc:
#             existing_doc["doc"]["fmtest"].append(input_doc["doc"]["fmtest"])
#         else:
#             output_doc = {
#                 "id": input_doc["id"],
#                 "well_id": input_doc["well_id"],
#                 "repo_id": input_doc["repo_id"],
#                 "repo_name": input_doc["repo_name"],
#                 "suite": input_doc["suite"],
#                 "tag": input_doc["tag"],
#                 "doc": {
#                     "fmtest": [input_doc["doc"]["fmtest"]],
#                     "well": input_doc["doc"]["well"],
#                 },
#             }
#             output_docs.append(output_doc)
#     return output_docs
