"""JSON utilities"""


def json2object(jsondict, key):
    """Recursively retrieve the value corresponding to the key"""
    for k in jsondict.keys():
        if key == k:
            return jsondict[k]
        elif isinstance(jsondict[k], dict):
            res = json2object(jsondict[k], key)
            if res:
                return res
