from pathlib import Path

from dotenv import load_dotenv


NOTEBOOKLM_DIR = Path(__file__).resolve().parent
ENV_FILE = NOTEBOOKLM_DIR / ".env"


def load_notebooklm_env(override: bool = False) -> bool:
    """Load the notebooklm-local env file regardless of the current working directory."""
    return load_dotenv(ENV_FILE, override=override)
