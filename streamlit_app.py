import streamlit as st
from snowflake.snowpark.context import get_active_session

st.set_page_config(page_title="Snowflake 14-Day Performance Tracker", layout="wide")

session = get_active_session()

st.title("Snowflake Performance Tracker (Past 14 Days)")
st.caption(
    "Highlights query performance, cost, and operational findings across the last 14 days."
)

DATE_FILTER = "START_TIME >= DATEADD('day', -14, CURRENT_TIMESTAMP())"


@st.cache_data(ttl=900)
def load_query_history():
    return session.sql(
        f"""
        SELECT
            QUERY_ID,
            USER_NAME,
            WAREHOUSE_NAME,
            DATABASE_NAME,
            SCHEMA_NAME,
            QUERY_TEXT,
            TOTAL_ELAPSED_TIME / 1000 AS TOTAL_ELAPSED_SECONDS,
            COALESCE(CREDITS_USED_CLOUD_SERVICES, 0) AS CREDITS_USED_CLOUD_SERVICES,
            START_TIME,
            EXECUTION_STATUS,
            ERROR_CODE,
            ERROR_MESSAGE,
            BYTES_SCANNED
        FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
        WHERE {DATE_FILTER}
          AND QUERY_TYPE != 'SHOW'
        """
    ).to_pandas()


@st.cache_data(ttl=900)
def load_warehouse_metering():
    return session.sql(
        f"""
        SELECT
            TO_DATE(START_TIME) AS USAGE_DATE,
            WAREHOUSE_NAME,
            SUM(CREDITS_USED) AS CREDITS_USED
        FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
        WHERE {DATE_FILTER}
        GROUP BY 1, 2
        """
    ).to_pandas()


query_history = load_query_history()
warehouse_metering = load_warehouse_metering()

query_history["TOTAL_CREDITS"] = query_history["CREDITS_USED_CLOUD_SERVICES"]

slow_queries = query_history.sort_values(
    "TOTAL_ELAPSED_SECONDS", ascending=False
).head(5)
expensive_queries = query_history.sort_values("TOTAL_CREDITS", ascending=False).head(5)

failed_queries = query_history[query_history["EXECUTION_STATUS"] != "SUCCESS"]

warehouse_daily = (
    warehouse_metering.groupby("USAGE_DATE", as_index=False)["CREDITS_USED"].sum()
)
warehouse_average = (
    warehouse_daily["CREDITS_USED"].mean() if not warehouse_daily.empty else 0
)
warehouse_daily["IS_SPIKE"] = warehouse_daily["CREDITS_USED"] > warehouse_average * 1.5
spike_days = warehouse_daily[warehouse_daily["IS_SPIKE"]]

warehouse_totals = (
    warehouse_metering.groupby("WAREHOUSE_NAME", as_index=False)["CREDITS_USED"].sum()
)
warehouse_top = warehouse_totals.sort_values("CREDITS_USED", ascending=False).head(3)

user_totals = (
    query_history.groupby("USER_NAME", as_index=False)["TOTAL_CREDITS"].sum()
)
user_top = user_totals.sort_values("TOTAL_CREDITS", ascending=False).head(3)

st.subheader("1. Critical Findings")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric(
        "Slow Queries (>60s)",
        f"{(query_history['TOTAL_ELAPSED_SECONDS'] > 60).sum():,}",
    )

with col2:
    st.metric(
        "Failed/Cancelled Queries",
        f"{len(failed_queries):,}",
    )

with col3:
    st.metric(
        "Daily Avg Credits",
        f"{warehouse_average:,.2f}",
    )

with col4:
    st.metric("Cost Spike Days", f"{len(spike_days):,}")

st.markdown("**Query Performance Summary**")
if slow_queries.empty:
    st.info("No query history found for the past 14 days.")
else:
    st.write(
        f"Top slow query duration: {slow_queries['TOTAL_ELAPSED_SECONDS'].iloc[0]:,.1f} seconds."
    )
    st.write(
        f"Average query duration: {query_history['TOTAL_ELAPSED_SECONDS'].mean():,.1f} seconds."
    )

st.markdown("**Cost Spikes**")
if spike_days.empty:
    st.success("No daily cost spikes above 1.5x average credits.")
else:
    st.dataframe(spike_days, use_container_width=True)

st.markdown("**Query Issues**")
if failed_queries.empty:
    st.success("No failed or cancelled queries detected.")
else:
    issue_summary = (
        failed_queries.groupby(["ERROR_CODE", "ERROR_MESSAGE"], dropna=False)
        .size()
        .reset_index(name="COUNT")
        .sort_values("COUNT", ascending=False)
        .head(5)
    )
    st.dataframe(issue_summary, use_container_width=True)

st.markdown("**Recommendations**")
st.markdown(
    """
- Review the top slowest and most expensive queries below for tuning opportunities.
- If cost spikes appear, consider warehouse auto-suspend and right-sizing for heavy days.
- Address recurring query errors by validating upstream data sources and permissions.
"""
)

st.divider()

st.subheader("2. Query Performance Analysis")

col5, col6 = st.columns(2)

with col5:
    st.markdown("**Top 5 Slowest Queries**")
    st.dataframe(
        slow_queries[
            [
                "QUERY_ID",
                "USER_NAME",
                "WAREHOUSE_NAME",
                "TOTAL_ELAPSED_SECONDS",
                "START_TIME",
            ]
        ],
        use_container_width=True,
    )

with col6:
    st.markdown("**Top 5 Expensive Queries**")
    st.dataframe(
        expensive_queries[
            [
                "QUERY_ID",
                "USER_NAME",
                "WAREHOUSE_NAME",
                "TOTAL_CREDITS",
                "START_TIME",
            ]
        ],
        use_container_width=True,
    )

st.markdown("**Recommendations**")
st.markdown(
    """
- Target queries with high elapsed time and credits for SQL optimization.
- Consider result caching and clustering keys for frequently scanned tables.
- Evaluate warehouse sizing for workloads with persistent high runtime.
"""
)

st.divider()

st.subheader("3. Cost Analysis")

st.markdown("**Daily Average Credit Consumption**")
st.dataframe(warehouse_daily.sort_values("USAGE_DATE"), use_container_width=True)

col7, col8 = st.columns(2)

with col7:
    st.markdown("**Top 3 Warehouses by Consumption**")
    st.dataframe(warehouse_top, use_container_width=True)

with col8:
    st.markdown("**Top 3 Users by Consumption**")
    st.dataframe(user_top, use_container_width=True)

st.markdown("**Recommendations**")
st.markdown(
    """
- Review warehouse schedules for the top consumers to reduce idle time.
- Track top users and collaborate on query tuning or workload segmentation.
- Use resource monitors to alert on unexpected credit spikes.
"""
)
