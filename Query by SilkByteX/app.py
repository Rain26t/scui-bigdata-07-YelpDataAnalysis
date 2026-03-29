import streamlit as st

from yelp_text_to_sql.config import load_config
from yelp_text_to_sql.ui import run_app


def main() -> None:
    """Start the Streamlit app UI."""
    st.set_page_config(page_title="Query by SilkByteX", page_icon="✦", layout="wide")
    config = load_config()
    try:
        run_app(config)
    except Exception:
        st.error(
            "Query by SilkByteX hit a startup problem. Refresh the page or continue in Demo/Mock Mode."
        )
        st.info(
            "The app stayed in a safe presentation state and intentionally hid the Python traceback."
        )


if __name__ == "__main__":
    main()
