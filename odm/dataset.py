import os
import json
from copy import deepcopy


def build_directory_tree(root_dir):
    """
    Builds a nested dictionary that represents the folder structure of root_dir.
    Directories are dictionaries, and files are represented by their relative paths.
    """
    tree = {}
    for root, dirs, files in os.walk(root_dir):
        # Get the relative path of the root directory
        rel_path = os.path.relpath(root, root_dir)
        if rel_path == ".":
            # We are at the root directory, handle it specially
            parent = tree
        else:
            # We are in a subdirectory
            parts = rel_path.split(os.sep)
            parent = tree
            for part in parts:
                parent = parent.setdefault(part, {})

        # Add files to the current directory level with their relative paths
        for file in files:
            file_path = os.path.join(rel_path, file) if rel_path != "." else file
            parent[file] = file_path

    return tree


def aggregate_dir_by_key(temp_dict, parent_key=None):
    list_container = []
    keys_to_remove = []

    for key, value in list(temp_dict.items()):
        if parent_key is None and key.isdigit():
            list_container.append({"id": key, **value})  # Add an 'id' field
            keys_to_remove.append(key)

    # Remove the original numeric directory entries from the dictionary
    for key in keys_to_remove:
        del temp_dict[key]

    if list_container:
        if parent_key:
            temp_dict[parent_key] = list_container
        else:
            temp_dict["scans"] = list_container

    # Recursively apply this to all sub-directories
    for key, value in temp_dict.items():
        if isinstance(value, dict):
            aggregate_dir_by_key(value, parent_key=key)  # Recursive call
        if isinstance(value, list):
            [
                aggregate_dir_by_key(item, parent_key=key)
                for item in value
                if isinstance(item, dict)
            ]

    # return temp_dict


def aggregate_numeric_directories(_dict):
    """
    Post-process the directory tree to aggregate numeric-named directories into a list.
    """
    temp_dict = deepcopy(_dict)
    scans = []  # List to hold all numeric directory data
    keys_to_remove = []  # Track numeric keys to remove from the root after aggregation

    # Check each key in the dictionary; if it's numeric, move it to the 'scans' list
    for key, value in list(temp_dict.items()):
        if key.isdigit():  # Check if the directory name is numeric
            scans.append(
                {"id": key, **value}
            )  # Aggregate into scans with an 'id' field
            keys_to_remove.append(key)

    # Remove the original numeric directory entries from the dictionary
    for key in keys_to_remove:
        del temp_dict[key]

    if scans:
        temp_dict["scans"] = scans

    return temp_dict


class DictToObject:
    def __init__(self, data):
        for key, value in data.items():
            if isinstance(value, dict):
                value = DictToObject(value)
            elif isinstance(value, list):
                value = [
                    DictToObject(item) if isinstance(item, dict) else item
                    for item in value
                ]
            setattr(self, key, value)

    def __getattr__(self, item):
        # If the attribute doesn't exist, raise an AttributeError
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{item}'"
        )


def load_json(file):
    with open(file) as f:
        data = json.load(f)
    return data


class Mapping:
    MAPPING_KEY = "mapping"

    def __init__(self, config_file: str):
        self.config = load_json(config_file)

        # Initialize the mapping dictionary
        self.mapping_dict = {}
        self._generate_mapping(self.config)

    def _generate_mapping(self, config, parent_key=""):
        """
        Recursively generate mapping from JSON structure
        """
        for key, value in config.items():
            if isinstance(value, dict):
                # If there is a 'mapping' key, use it; otherwise, continue deeper
                if self.MAPPING_KEY in value:
                    self.mapping_dict[value[self.MAPPING_KEY]] = key
                self._generate_mapping(value, parent_key + key + ".")
            elif isinstance(value, list):
                # Handle list of dicts (assuming similar structure)
                for item in value:
                    if isinstance(item, dict):
                        self._generate_mapping(item, parent_key + key + ".")

    def get_mapping(self, mapping_name):
        return self.mapping_dict.get(mapping_name, None)

    def get_all_mapping(self):
        # Return all mappings
        return self.mapping_dict

    def __str__(self):
        # String representation to print all mappings
        return json.dumps(self.mapping_dict, indent=4)


class Dataset:
    def __init__(self, folder, config_file: json):
        self.folder = folder
        self.config = config_file
        self.mapping = load_json(self.config)

    def scans(self):
        pass

    def list_files(self):
        files = []
        for root, _, filenames in os.walk(self.folder):
            for filename in filenames:
                files.append(os.path.join(root, filename))
        return files


class ODM:
    def __init__(self, datset, config):
        self.dataset = Dataset(datset, config)
        self.mapping = Mapping(config)


if __name__ == "__main__":
    data_root = "/home/szhong/aw13/shenjun/workspace_brukerapi/data/JO_SpruceBub2_25_7_2017_d20231129_one_1_1_20231129_163907"
    tree = build_directory_tree(data_root)
    aggregate_dir_by_key(tree)

    # config_file = (
    #     "/fs04/aw13/shenjun/workspace_brukerapi/xnat-ingest/odm/config/pv360.json"
    # )

    # mapping = Mapping(config_file)
