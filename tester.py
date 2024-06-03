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


# ids = [621, 826, 831, 834, 835, 838, 846, 847, 848]
ids = ["1-62", "1-82", "2-83", "2-83", "2-83", "2-83", "2-84", "3-84", "4-84"]


x = chunk_ids(ids, 4)
print(x)
