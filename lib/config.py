import os
import yaml
from dotenv import load_dotenv


def load_config(path="config.yml"):
    load_dotenv()
    with open(path) as f:
        cfg = yaml.safe_load(f)
    cfg["google_oauth"] = {
        "client_id": os.getenv("GOOGLE_CLIENT_ID"),
        "client_secret": os.getenv("GOOGLE_CLIENT_SECRET"),
        "token_path": os.getenv("GOOGLE_TOKEN_PATH", ".google_token.json"),
    }
    return cfg
