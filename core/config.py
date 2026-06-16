import os
from pathlib import Path

env_path = Path(__file__).resolve().parent.parent / '.env'

if env_path.exists():
    with open(env_path, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#') and '=' in line:
                key, value = line.split('=', 1)
                clean_value = value.strip().replace('"', '').replace("'", "")
                clean_value = clean_value.replace("\n", "").replace("\r", "").replace(" ", "")
                os.environ[key.strip()] = clean_value

class Settings:
    PROJECT_NAME: str = "Alutech Social Parser"
    VK_TOKEN: str = os.getenv("VK_TOKEN", "")

settings = Settings()