from laminar.transformers.custom import raw_to_l1

def test_raw_to_l1_hash():
    plaintext: str = "dummy"
    actual: str = raw_to_l1.raw_to_l1_hash(plaintext)
    expected: str = "b5a2c96250612366ea272ffac6d9744aaf4b45aacd96aa7cfcb931ee3b558259"
    assert actual == expected