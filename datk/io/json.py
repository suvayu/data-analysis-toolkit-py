"""JSON utilities"""


import json
import ast


def json2object(jsondict, key):
    """Recursively retrieve the value corresponding to the key"""
    for k in jsondict.keys():
        if key == k:
            return jsondict[k]
        elif isinstance(jsondict[k], dict):
            res = json2object(jsondict[k], key)
            if res:
                return res


def jsondecoder(sample_data, loggingfn=print):
    """Return a JSON decoder that works

    The JSON spec requires the properties to be surrounded by double quotes, or
    have no trailing comma after the last entry.  Unfortunately real-world JSON
    documents often violate that.  There is no "safe" way to decode such
    malformed JSON documents, but if you are willing to risk `eval`, this
    method falls back to `ast.literal_eval`.

    Since this is not recommended, a logging function is mandatory.  It is
    simply a callable, so it could be anything: `print`, `logger.warning`, etc.

    """
    decoder = json.loads
    try:
        _ = json.loads(sample_data)
    except json.JSONDecodeError:
        decoder = ast.literal_eval
        loggingfn(f"Malformed JSON document, safe decoder was replaced by {decoder}")
    return decoder
