import os
import yaml

current_dir = os.path.dirname(os.path.abspath(__file__))
parent_dir = os.path.abspath(os.path.join(current_dir, os.pardir))

yaml_path = os.path.join(parent_dir, "config.yaml")
with open(yaml_path, 'r',encoding="utf-8") as f:
    config = yaml.safe_load(f)

settings_dict = config