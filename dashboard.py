"""
FakeShop analytics dashboard.

Reads the dbt marts (dim_customers, fct_daily_sales) from the DuckDB warehouse.
Run with:  streamlit run dashboard.py
"""

import duckdb
import plotly.express as px
import streamlit as st

from include.config import duckdb_path

st.set_page_config(page_title="FakeShop Analytics", page_icon="🛒", layout="wide")
st.title("🛒 FakeShop Dashboard")
st.markdown("Analytics over dbt models built by the Airflow + DuckDB pipeline.")


@st.cache_data(ttl=300)
def load_data(db_file: str):
    # Read-only so the dashboard never takes the DuckDB write lock.
    with duckdb.connect(db_file, read_only=True) as conn:
        customers = conn.execute("SELECT * FROM dim_customers").df()
        sales = conn.execute("SELECT * FROM fct_daily_sales").df()
    return customers, sales


db_file = duckdb_path()
if not db_file.exists():
    st.error(f"Warehouse not found at `{db_file}`. Run the pipeline first (see README).")
    st.stop()

try:
    customers_df, sales_df = load_data(str(db_file))
except duckdb.CatalogException:
    st.error("dbt models not found. Run `dbt build` (or the full pipeline) first.")
    st.stop()
except duckdb.IOException as exc:
    st.error(f"Could not open the warehouse (is a pipeline run holding the lock?): {exc}")
    st.stop()

col1, col2, col3, col4 = st.columns(4)
col1.metric("Total Revenue", f"${sales_df['daily_revenue'].sum():,.2f}")
col2.metric("Customers", customers_df["user_id"].nunique())
col3.metric("VIP Customers", int((customers_df["customer_segment"] == "VIP").sum()))
col4.metric("Line Items Sold", int(sales_df["items_sold"].sum()))

st.markdown("---")
chart_col1, chart_col2 = st.columns(2)

with chart_col1:
    st.subheader("Revenue by Category")
    by_category = sales_df.groupby("category", as_index=False)["daily_revenue"].sum()
    st.plotly_chart(
        px.pie(by_category, values="daily_revenue", names="category", hole=0.4),
        use_container_width=True,
    )

with chart_col2:
    st.subheader("Customer Segmentation")
    segments = customers_df["customer_segment"].value_counts().reset_index()
    segments.columns = ["Segment", "Count"]
    st.plotly_chart(
        px.bar(segments, x="Segment", y="Count", color="Segment"), use_container_width=True
    )

st.subheader("Daily Revenue")
daily = sales_df.groupby("transaction_date", as_index=False)["daily_revenue"].sum()
st.plotly_chart(px.bar(daily, x="transaction_date", y="daily_revenue"), use_container_width=True)
