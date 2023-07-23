import hashlib

# TODO: create router for different data types (i.e. scalar, array, struct, array of struct)

def raw_to_l1_hash(data: str) -> str:
    return hashlib.sha256(data.encode("utf-8")).hexdigest()
