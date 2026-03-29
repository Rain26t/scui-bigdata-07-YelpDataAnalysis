from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import quote_plus


_ENV_FILES_LOADED = False
SUPPORTED_SQL_ENGINES = ("hive", "spark")
REQUIRED_LIVE_MODEL_ENV_VARS = ("DEEPSEEK_API_KEY", "DEEPSEEK_MODEL")
REQUIRED_HIVE_ENV_VARS = (
    "YELP_SQL_ENGINE",
    "HIVE_HOST",
    "HIVE_PORT",
    "HIVE_DATABASE",
    "HIVE_AUTH",
)
OPTIONAL_HIVE_ENV_VARS = ("HIVE_USERNAME", "HIVE_PASSWORD")


@dataclass
class AppConfig:
    app_title: str = "Query by SilkByteX"
    page_icon: str = "🍽️"
    default_question: str = "Ask a Yelp analytics question..."
    default_sql_limit: int = 100
    deepseek_api_key: str = ""
    deepseek_model: str = ""
    deepseek_base_url: str = "https://api.deepseek.com"
    debug_mode: bool = False
    database_uri: str = ""


def normalize_sql_engine(engine: str) -> str:
    """Normalize the configured SQL engine and keep the default simple."""
    normalized = engine.strip().lower()
    return normalized or "hive"


def _load_env_file(path: Path) -> None:
    """Load simple KEY=VALUE pairs from one .env-style file.

    This keeps the project beginner-friendly by avoiding an extra dependency.
    Existing shell environment values win over values from the file.
    """
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()

        if not line or line.startswith("#"):
            continue

        if line.startswith("export "):
            line = line[len("export ") :].strip()

        if "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip()

        if not key or key in os.environ:
            continue

        if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
            value = value[1:-1]
        elif " #" in value:
            value = value.split(" #", 1)[0].rstrip()

        os.environ[key] = value


def ensure_environment_loaded() -> None:
    """Load local .env files once so the app works without manual exports.

    Supported locations:
    - project root `.env`
    - `tests/.env` for local test-only values when present
    """
    global _ENV_FILES_LOADED

    if _ENV_FILES_LOADED:
        return

    project_root = Path(__file__).resolve().parent.parent
    _load_env_file(project_root / ".env")
    _load_env_file(project_root / "tests" / ".env")
    _ENV_FILES_LOADED = True


def get_backend_env_template(engine: str) -> str:
    """Return a small .env template for the selected backend engine.

    The template is intentionally explicit and uses TODO placeholders so it is
    easy to copy into a local `.env` file without pretending to know the user's
    real machine values.
    """
    selected_engine = normalize_sql_engine(engine)

    if selected_engine == "spark":
        return "\n".join(
            [
                "YELP_SQL_ENGINE=spark",
                "SPARK_MASTER=local[*]  # TODO: or spark://your-master-host:7077",
                "SPARK_APP_NAME=YelpTextToSQL",
                "SPARK_WAREHOUSE_DIR=  # TODO: optional shared warehouse path",
                "HIVE_METASTORE_URI=  # TODO: optional thrift://your-metastore-host:9083",
                "SPARK_SQL_CATALOG_IMPLEMENTATION=hive",
            ]
        )

    return "\n".join(
        [
            "YELP_SQL_ENGINE=hive",
            "HIVE_HOST=TODO_your_hive_server_ip_or_hostname",
            "HIVE_PORT=10000",
            "HIVE_DATABASE=default",
            "HIVE_AUTH=NONE  # TODO: change if your Hive uses LDAP or Kerberos",
            "HIVE_USERNAME=  # TODO: optional",
            "HIVE_PASSWORD=  # TODO: optional",
        ]
    )


def get_live_hive_env_template() -> str:
    """Return one practical `.env` example for the live Hive path."""
    return "\n".join(
        [
            "# Live model",
            "DEEPSEEK_API_KEY=TODO_your_deepseek_api_key",
            "DEEPSEEK_MODEL=deepseek-chat",
            "DEEPSEEK_BASE_URL=https://api.deepseek.com",
            "",
            "# Hive backend",
            get_backend_env_template("hive"),
        ]
    )


def get_live_hive_setup_message() -> str:
    """Return a short beginner-friendly summary for live Hive setup."""
    required_backend = ", ".join(REQUIRED_HIVE_ENV_VARS)
    optional_backend = ", ".join(OPTIONAL_HIVE_ENV_VARS)
    required_model = ", ".join(REQUIRED_LIVE_MODEL_ENV_VARS)

    return "\n".join(
        [
            "Live Hive setup checklist:",
            f"- Required Hive variables: {required_backend}",
            f"- Optional Hive variables: {optional_backend}",
            f"- Required live model variables: {required_model}",
            "- Install the Hive client with: pip install pyhive",
            "- First connectivity command: python3 yelp_text_to_sql/database.py",
        ]
    )


def get_backend_recommendation(engine: str) -> str:
    """Return a short beginner-friendly recommendation for backend setup."""
    selected_engine = normalize_sql_engine(engine)

    if selected_engine == "spark":
        return (
            "Use the Spark path when this app machine can start Spark directly and "
            "see the same Yelp tables or shared Hive metastore."
        )

    return (
        "Use the Hive path when your Yelp tables live on another machine and "
        "HiveServer2 is the easiest network-accessible service to connect to."
    )


def has_live_model_config(config: AppConfig | None = None) -> bool:
    """Return True when both live-model settings are available."""
    current_config = config or load_config()
    return bool(current_config.deepseek_api_key and current_config.deepseek_model)


def get_live_model_setup_message(config: AppConfig | None = None) -> str:
    """Return a short beginner-friendly setup message for missing config."""
    current_config = config or load_config()
    missing_parts: list[str] = []

    if not current_config.deepseek_api_key:
        missing_parts.append("DEEPSEEK_API_KEY")

    if not current_config.deepseek_model:
        missing_parts.append("DEEPSEEK_MODEL")

    if not missing_parts:
        return "Live model settings are configured."

    missing_text = ", ".join(missing_parts)
    return (
        f"Live SQL generation is not configured yet. Missing: {missing_text}. "
        "Set these environment variables, then restart the Streamlit app. "
        "You can keep using Demo/Mock Mode until live settings are ready."
    )


def _build_database_uri_from_env() -> str:
    """Build one SQLAlchemy database URI from environment variables."""
    explicit_uri = os.getenv("DATABASE_URI", "").strip()
    if explicit_uri:
        return explicit_uri

    engine = normalize_sql_engine(os.getenv("YELP_SQL_ENGINE", "hive"))
    if engine == "hive":
        host = os.getenv("HIVE_HOST", "").strip()
        port = os.getenv("HIVE_PORT", "10000").strip() or "10000"
        database = os.getenv("HIVE_DATABASE", "default").strip() or "default"
        auth = os.getenv("HIVE_AUTH", "NONE").strip().upper() or "NONE"
        username = os.getenv("HIVE_USERNAME", "").strip()
        password = os.getenv("HIVE_PASSWORD", "").strip()

        if not host:
            return ""

        user_part = ""
        if username:
            user_part = quote_plus(username)
            if password:
                user_part += f":{quote_plus(password)}"
            user_part += "@"

        auth_map = {
            "NONE": "NONE",
            "NOSASL": "NOSASL",
            "LDAP": "LDAP",
            "KERBEROS": "KERBEROS",
        }
        auth_value = auth_map.get(auth, auth)
        return f"hive://{user_part}{host}:{port}/{database}?auth={auth_value}"

    return ""


def load_config() -> AppConfig:
    """Load simple app settings from environment variables."""
    ensure_environment_loaded()

    return AppConfig(
        app_title=os.getenv("APP_TITLE", "Query by SilkByteX"),
        page_icon=os.getenv("APP_ICON", "🍽️"),
        default_question=os.getenv(
            "DEFAULT_QUESTION",
            "Ask a Yelp analytics question...",
        ),
        default_sql_limit=max(1, int(os.getenv("DEFAULT_SQL_LIMIT", "100") or "100")),
        deepseek_api_key=os.getenv("DEEPSEEK_API_KEY", "").strip(),
        deepseek_model=os.getenv("DEEPSEEK_MODEL", "").strip(),
        deepseek_base_url=(
            os.getenv("DEEPSEEK_BASE_URL", "https://api.deepseek.com").strip()
            or "https://api.deepseek.com"
        ),
        debug_mode=os.getenv("DEBUG_MODE", "false").lower() == "true",
        database_uri=_build_database_uri_from_env(),
    )
