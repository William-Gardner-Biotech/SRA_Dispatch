import json


def load_config(json_file)-> dict:
    """
    Reads a JSON config file and returns a dictionary object.
    """

    with open(json_file) as file:
        config = json.load(file)
    return config
