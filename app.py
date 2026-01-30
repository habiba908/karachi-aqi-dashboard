import json
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo
from dateutil import parser as dateparser

import pandas as pd
import streamlit as st
import matplotlib.pyplot as plt
from pymongo import MongoClient, DESCENDING, ASCENDING

DB_NAME = "feature_store"
FORECAST_COL = "aqi_forecast_karachi_next72h"

UTC = timezone.utc
PK_TZ = ZoneInfo("Asia/Karachi")

st.set_page_config(page_title="Karachi AQI — Next 72h", layout="wide")


@st.cache_data(ttl=60)
def load_latest_forecast(mongo_uri: str, city: str):
    # Fail fast if connection issues
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")

    col = client[DB_NAME][FORECAST_COL]

    latest = col.find_one({"city": city}, sort=[("base_time", DESCENDING)])
    if not latest:
        return None, []

    base_time = latest["base_time"]

    cursor = col.find(
        {"city": city, "base_time": base_time},
        sort=[("horizon_hours", ASCENDING)]
    )

    rows = []
    for d in cursor:
        d.pop("_id", None)
        rows.append(d)

    return base_time, rows


def to_dt_utc(iso_str: str) -> datetime:
    """Parse ISO string (e.g., '2026-01-26T09:00:00Z') into aware UTC datetime."""
    dt = dateparser.isoparse(iso_str)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def category_badge(cat: str) -> str:
    colors = {
        "Good": "#00e400",
        "Moderate": "#ffff00",
        "Unhealthy for Sensitive Groups": "#ff7e00",
        "Unhealthy": "#ff0000",
        "Very Unhealthy": "#8f3f97",
        "Hazardous": "#7e0023",
    }
    c = colors.get(cat, "#999999")
    # dark text works well for yellow/orange; good enough overall
    return (
        f"<span style='background:{c}; padding:6px 10px; border-radius:10px; "
        f"font-weight:700; color:#000;'>{cat}</span>"
    )


def main():
    st.title("🌫️ Karachi AQI Forecast — Next 72 Hours")

    if "MONGO_URI" not in st.secrets:
        st.error("Missing MONGO_URI in Streamlit secrets. Add it in Streamlit Cloud → Settings → Secrets.")
        st.stop()

    mongo_uri = st.secrets["MONGO_URI"]
    default_city = st.secrets.get("CITY", "Karachi")

    st.sidebar.header("Controls")
    city = st.sidebar.text_input("City", value=default_city)
    if st.sidebar.button("Refresh now"):
        load_latest_forecast.clear()

    base_time, rows = load_latest_forecast(mongo_uri, city)

    if not rows:
        st.warning(f"No forecast found for city={city}")
        st.stop()

    df = pd.DataFrame(rows)

    # Ensure numeric types
    df["horizon_hours"] = pd.to_numeric(df["horizon_hours"], errors="coerce")
    df["predicted_aqi_us"] = pd.to_numeric(df["predicted_aqi_us"], errors="coerce")
    df["predicted_pm2_5"] = pd.to_numeric(df["predicted_pm2_5"], errors="coerce")

    # Parse times
    df["target_dt_utc"] = df["target_time"].apply(to_dt_utc)
    base_dt_utc = to_dt_utc(base_time)
    base_dt_pk = base_dt_utc.astimezone(PK_TZ)

    # Create Pakistan-time columns for display
    df["target_dt_pk"] = pd.to_datetime(df["target_dt_utc"], utc=True).dt.tz_convert(PK_TZ)
    df["target_time_pk"] = df["target_dt_pk"].dt.strftime("%Y-%m-%d %H:%M")

    st.caption(
        f"Latest base_time: **{base_time}** | Pakistan time: **{base_dt_pk.strftime('%Y-%m-%d %H:%M')}** "
        f"| Rows: **{len(df)}**"
    )

    # Tabs
    tab1, tab2 = st.tabs(["📈 72h Charts", "🕒 Pick a Time (Next 72h)"])

    # =========================
    # TAB 1 — Charts
    # =========================
    with tab1:
        current = df.iloc[0]
        worst_idx = df["predicted_aqi_us"].idxmax()
        worst = df.loc[worst_idx]

        c1, c2, c3, c4 = st.columns(4)
        c1.metric("Current AQI (H+1)", int(current["predicted_aqi_us"]), current["category"])
        c2.metric("Current PM2.5 (µg/m³)", round(float(current["predicted_pm2_5"]), 2))
        c3.metric("Worst AQI (Next 72h)", int(worst["predicted_aqi_us"]), f"H+{int(worst['horizon_hours'])} • {worst['category']}")
        c4.metric("Worst PM2.5 (µg/m³)", round(float(df["predicted_pm2_5"].max()), 2))

        # Category badges (nice visual)
        b1, b2 = st.columns(2)
        with b1:
            st.markdown(f"**Current category:** {category_badge(str(current['category']))}", unsafe_allow_html=True)
        with b2:
            st.markdown(f"**Worst category:** {category_badge(str(worst['category']))}", unsafe_allow_html=True)

        left, right = st.columns(2)

        with left:
            st.subheader("AQI Trend (Next 72h)")
            fig = plt.figure()
            plt.plot(df["horizon_hours"], df["predicted_aqi_us"])
            plt.xlabel("Horizon (hours)")
            plt.ylabel("AQI (US EPA)")
            plt.grid(True)
            st.pyplot(fig)

        with right:
            st.subheader("PM2.5 Trend (Next 72h)")
            fig2 = plt.figure()
            plt.plot(df["horizon_hours"], df["predicted_pm2_5"])
            plt.xlabel("Horizon (hours)")
            plt.ylabel("PM2.5 (µg/m³)")
            plt.grid(True)
            st.pyplot(fig2)

        st.subheader("Forecast Table (72 rows) — Pakistan time")
        show_cols = ["horizon_hours", "target_time_pk", "predicted_pm2_5", "predicted_aqi_us", "category"]
        st.dataframe(df[show_cols], use_container_width=True, height=520)

        out = {"ok": True, "city": city, "base_time": base_time, "count": len(rows), "rows": rows}
        st.download_button(
            "⬇️ Download JSON",
            data=json.dumps(out, ensure_ascii=False, indent=2),
            file_name=f"aqi_forecast_{city.lower()}_{base_time.replace(':','-')}.json",
            mime="application/json",
        )

    # =========================
    # TAB 2 — Pick a time (Pakistan time + exact match)
    # =========================
    with tab2:
        st.subheader("Pick a specific time within the next 72 hours (Pakistan time)")

        # Allowed window: from base_time+1h to base_time+72h
        min_dt_utc = base_dt_utc + timedelta(hours=1)
        max_dt_utc = base_dt_utc + timedelta(hours=72)

        min_dt_pk = min_dt_utc.astimezone(PK_TZ)
        max_dt_pk = max_dt_utc.astimezone(PK_TZ)

        st.write(
            f"**Allowed range (Pakistan time):** {min_dt_pk.strftime('%Y-%m-%d %H:%M')} → {max_dt_pk.strftime('%Y-%m-%d %H:%M')}"
        )

        # User selects PK date + hour
        picked_date = st.date_input(
            "Choose date (Pakistan time)",
            value=min_dt_pk.date(),
            min_value=min_dt_pk.date(),
            max_value=max_dt_pk.date(),
        )
        picked_hour = st.selectbox(
            "Choose hour (Pakistan time)",
            list(range(0, 24)),
            index=min_dt_pk.hour
        )

        picked_pk = datetime(
            picked_date.year, picked_date.month, picked_date.day,
            picked_hour, 0, 0,
            tzinfo=PK_TZ
        )
        picked_utc = picked_pk.astimezone(UTC)

        # Guardrails: clamp within allowed range
        if picked_utc < min_dt_utc:
            picked_utc = min_dt_utc
            picked_pk = picked_utc.astimezone(PK_TZ)
        if picked_utc > max_dt_utc:
            picked_utc = max_dt_utc
            picked_pk = picked_utc.astimezone(PK_TZ)

        # Exact match on hourly target_time
        match = df[df["target_dt_utc"] == picked_utc]

        if match.empty:
            st.error("No exact hourly forecast exists for that selected time. Please choose a whole hour within the allowed range.")
        else:
            best = match.iloc[0]

            st.markdown("### Result")
            r1, r2, r3, r4 = st.columns(4)
            r1.metric("Target Time (Pakistan)", best["target_dt_pk"].strftime("%Y-%m-%d %H:%M"))
            r2.metric("AQI (US EPA)", int(best["predicted_aqi_us"]), best["category"])
            r3.metric("PM2.5 (µg/m³)", round(float(best["predicted_pm2_5"]), 2))
            r4.metric("Horizon (hours)", int(best["horizon_hours"]))

            st.markdown(f"**Category badge:** {category_badge(str(best['category']))}", unsafe_allow_html=True)
            st.caption(f"Selected time in UTC: {picked_utc.strftime('%Y-%m-%d %H:%M')}Z")

if __name__ == "__main__":
    main()
