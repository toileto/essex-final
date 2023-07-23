import yaml


class YAMLUtility:
    @staticmethod
    def read_yaml_from_string(content: str) -> dict:
        """
        Convert YAML string to dictionary.

        Args:
            content: YAML string content.
        
        Returns:
            Dictionary of the YAML.
        """
        return yaml.safe_load(content)
    
    @staticmethod
    def read_yaml_from_file(file_path: str) -> dict:
        """
        Read a YAML file as a dictionary.

        Args:
            file_path: YAML file path.

        Returns:
            Dictionary of the YAML.
        """
        with open(file_path, "r") as file:
            file_content: str = file.read()
        return YAMLUtility.read_yaml_from_string(content=file_content)
