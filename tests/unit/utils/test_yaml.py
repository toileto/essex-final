from laminar.utils.yaml import YAMLUtility
import os

 
def test_YAMLUtility_read_yaml_from_string():
    actual: dict = YAMLUtility.read_yaml_from_string(content="key: value")
    expected: dict = {"key": "value"}
    assert actual == expected

def test_YAMLUtility_read_yaml_from_file():
    file_path: str = os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        "..",
        "resources",
        'dummy_schema.yaml',
    )
    yaml: dict = YAMLUtility.read_yaml_from_file(file_path=file_path)
    assert type(yaml) == dict