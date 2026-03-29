from __future__ import annotations

import streamlit as st

from yelp_text_to_sql.schema_definitions import get_table_schemas


def render_schema_explorer_modal():
    """
    Renders a full-screen modal with a data dictionary for the Yelp schema.
    """
    st.markdown(
        """
        <div class="modal-background">
            <div class="modal-content">
                <h2>Data Dictionary</h2>
                <p>Explore the tables and columns available in the Yelp dataset.</p>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )

    for table_name, table_info in get_table_schemas().items():
        with st.expander(f"Table: {table_name}"):
            st.markdown(f"**Description:** {table_info.get('description', 'N/A')}")
            st.dataframe(table_info.get("columns", []))
