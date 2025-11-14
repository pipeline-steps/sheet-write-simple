from json import load


class Config:
    def __init__(self, billing_project, table_id, if_exists):
        self.billing_project = billing_project
        self.table_id = table_id
        self.if_exists = if_exists


def create_config(config_path: str):
    # read config json into a dict
    with open(config_path) as f:
        config = load(f)

    # client for bq operation
    table_id = config["tableId"]
    billing_project = config["billingProject"]
    if_exists = config.get("ifExists", "append")

    # return the config object
    return Config(billing_project, table_id, if_exists)
