

"""
FakeShop Executive Analytics Dashboard.
Connects directly to our dbt-transformed DuckDB models.
"""

import streamlit as st
import duckdb
import pandas as pd
import plotly.express as px

# 1. Set up the page layout
st.set_page_config(page_title="FakeShop Analytics", page_icon="🛒", layout="wide")
st.title("🛒 FakeShop Executive Dashboard")
st.markdown("Live analytics fueled by Airflow, dbt, and DuckDB.")

# 2. Connect to DuckDB and load our dbt models securely
@st.cache_data
def load_data():
    # Connect in read-only mode to protect our data
    conn = duckdb.connect('fakeshop_warehouse.db', read_only=True)
    
    # Fetch from our dbt-transformed tables!
    customers_df = conn.execute("SELECT * FROM dim_customers").df()
    sales_df = conn.execute("SELECT * FROM fct_daily_sales").df()
    
    conn.close()
    return customers_df, sales_df

try:
    customers_df, sales_df = load_data()

    # --- TOP ROW: Key Performance Indicators (KPIs) ---
    total_revenue = sales_df['daily_revenue'].sum()
    total_customers = customers_df['user_id'].nunique()
    vip_count = customers_df[customers_df['customer_segment'] == 'VIP'].shape[0]

    col1, col2, col3 = st.columns(3)
    col1.metric("Total Revenue", f"${total_revenue:,.2f}")
    col2.metric("Total Customers", total_customers)
    col3.metric("VIP Customers", vip_count)

    st.markdown("---")

    # --- BOTTOM ROW: Interactive Charts ---
    chart_col1, chart_col2 = st.columns(2)

    with chart_col1:
        st.subheader("Revenue by Category")
        category_sales = sales_df.groupby('category')['daily_revenue'].sum().reset_index()
        fig_pie = px.pie(category_sales, values='daily_revenue', names='category', hole=0.4)
        st.plotly_chart(fig_pie, use_container_width=True)

    with chart_col2:
        st.subheader("Customer Segmentation")
        segment_counts = customers_df['customer_segment'].value_counts().reset_index()
        segment_counts.columns = ['Segment', 'Count']
        fig_bar = px.bar(segment_counts, x='Segment', y='Count', color='Segment')
        st.plotly_chart(fig_bar, use_container_width=True)

except duckdb.CatalogException:
    st.error("🚨 Tables not found! Please make sure your Airflow pipeline and dbt models have run successfully.")