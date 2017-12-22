import yaml


def get_config():
    with open("config.yaml") as f:
        return yaml.load(f.read())
