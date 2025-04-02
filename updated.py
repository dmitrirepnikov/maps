import streamlit as st
import folium
from streamlit_folium import st_folium
from google.cloud import bigquery
import google.oauth2.credentials
from datetime import datetime, timedelta
import pytz
from math import sqrt
import pandas as pd

# Page config
st.set_page_config(page_title="Supply-Demand Distribution", layout="wide")

# Initialize BigQuery client
@st.cache_resource
def get_bq_client():
    try:
        credentials_dict = st.secrets["gcp_service_account"]
        credentials = google.oauth2.credentials.Credentials(
            None,
            refresh_token=credentials_dict['refresh_token'],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=credentials_dict['client_id'],
            client_secret=credentials_dict['client_secret']
        )
        return bigquery.Client(project='postmates-x', credentials=credentials)
    except Exception as e:
        st.error(f"Error initializing BigQuery client: {str(e)}")
        raise

bq = get_bq_client()

# Function to calculate square bounds
def get_square_bounds(lat, lon, side_length_meters):
    meters_per_degree = 111000
    degree_delta = (side_length_meters / 2) / meters_per_degree
    return [
        [lat - degree_delta, lon - degree_delta],
        [lat + degree_delta, lon + degree_delta]
    ]

@st.cache_data
def fetch_data(hour, day_offset):
    # Calculate the date based on the offset
    pst = pytz.timezone('America/Los_Angeles')
    current_time = datetime.now(pst)
    refresh_time = current_time.strftime('%Y-%m-%d %H:%M:%S %Z')
    selected_date = current_time.date() + timedelta(days=day_offset)
    
    query = f"""
    DECLARE hour INT64 DEFAULT {hour};
    DECLARE selected_date DATE DEFAULT DATE '{selected_date}';
    CREATE TEMP FUNCTION get_part_of_day(hour INT64) AS (hour);

    WITH hotspots AS (
      SELECT
        label,
        hotspot_location,
        ST_X(hotspot_location) as longitude,
        ST_Y(hotspot_location) as latitude,
        SAFE_CAST(delivery_count AS FLOAT64) as predicted_demand
      FROM `serve-robotics.serve_analytics.stg_delivery_platform__hotspots`
      WHERE date = selected_date
      AND day_of_week = LOWER(FORMAT_DATE("%A", selected_date))
      AND part_of_day = get_part_of_day(hour)
    ),

    hotspot_offers AS (
      SELECT
        h.label AS hotspot_label,
        COUNT(DISTINCT q.partner_job_id) AS num_offers
      FROM hotspots h
      LEFT JOIN `serve-robotics.serve_analytics.quotes` q
        ON ST_DISTANCE(h.hotspot_location, q.pickup_location) <= 400
      WHERE q.cardio_env = 'prod'
      AND q.partner_id = 'uber_eats_api'
      AND TIMESTAMP(q.time) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
      AND EXTRACT(HOUR FROM q.time AT TIME ZONE "America/Los_Angeles") = hour
      AND q.partner_job_id IS NOT NULL
      GROUP BY h.label
    ),

    hotspot_supply_hours AS (
      WITH delivery_times AS (
        SELECT DISTINCT
          robot_id,
          TIMESTAMP(courier_dispatched_datetime_pst) AS start_ts,
          TIMESTAMP(COALESCE(dropoff_complete_datetime_pst, cancel_delivery_datetime_pst)) AS end_ts
        FROM `serve-robotics.serve_analytics.deliveries_wide`
        WHERE cardio_env = 'prod'
        AND partner_id = 'uber_eats_api'
        AND TIMESTAMP(courier_dispatched_datetime_pst) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
      ),

      filtered_rover_state AS (
        SELECT
          r.*,
          dt.robot_id IS NOT NULL AS is_on_delivery
        FROM `serve-robotics.serve_analytics.stg_rover_state` r
        INNER JOIN `serve-robotics.serve_analytics.on_duty_intervals_ts` d
          ON r.robot_id = d.robot_id
          AND r.time_pst BETWEEN d.on_duty_start_datetime_pst AND d.on_duty_end_datetime_pst
        LEFT JOIN delivery_times dt
          ON r.robot_id = dt.robot_id
          AND TIMESTAMP(r.time_pst) BETWEEN dt.start_ts AND dt.end_ts
        WHERE EXTRACT(HOUR FROM r.time_pst) = hour
          AND TIMESTAMP(r.time_pst) >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 24 HOUR)
          AND r.time > CURRENT_TIMESTAMP() - INTERVAL 2 DAY
          AND d.cardio_env = 'prod'
      ),

      point_assignments AS (
        SELECT
          r.robot_id,
          r.time_pst,
          r.is_on_delivery,
          TIMESTAMP_TRUNC(r.time_pst, HOUR) AS time_hour,
          h.label as location
        FROM filtered_rover_state r
        CROSS JOIN hotspots h
        WHERE ST_DISTANCE(h.hotspot_location, ST_GEOGPOINT(r.geo_pose_longitude, r.geo_pose_latitude)) <= 420
        GROUP BY 1, 2, 3, 4, 5
      ),

      bulk AS (
        SELECT
          *,
          LEAD(time_pst) OVER (PARTITION BY robot_id ORDER BY time_pst) AS next_time_pst,
          ROW_NUMBER() OVER (PARTITION BY robot_id, time_pst ORDER BY location) as rn
        FROM point_assignments
      )

      SELECT
        location AS hotspot_label,
        is_on_delivery,
        robot_id,
        SUM(TIMESTAMP_DIFF(next_time_pst, time_pst, SECOND) / 3600.0) AS hours,
        COUNT(DISTINCT robot_id) AS num_robots
      FROM bulk
      WHERE next_time_pst IS NOT NULL
      GROUP BY location, is_on_delivery, robot_id
    )

    SELECT
      h.label AS hotspot_label,
      h.predicted_demand,
      h.latitude,
      h.longitude,
      COALESCE(ho.num_offers, 0) AS num_offers,
      COALESCE(SUM(CASE WHEN hs.is_on_delivery = FALSE THEN hs.hours END), 0) AS on_duty_not_on_delivery_hours,
      COALESCE(SUM(CASE WHEN hs.is_on_delivery = TRUE THEN hs.hours END), 0) AS on_duty_on_delivery_hours,
      COALESCE(SUM(hs.hours), 0) AS net_supply_hours,
      COUNT(DISTINCT hs.robot_id) AS num_robots
    FROM hotspots h
    LEFT JOIN hotspot_offers ho
      ON h.label = ho.hotspot_label
    LEFT JOIN hotspot_supply_hours hs
      ON h.label = hs.hotspot_label
    GROUP BY
      h.label,
      h.predicted_demand,
      h.latitude,
      h.longitude,
      ho.num_offers
    ORDER BY h.predicted_demand DESC;
    """
    
    with st.spinner('Fetching data...'):
        data = bq.query(query).result().to_dataframe()
        data.loc[data['predicted_demand'] <= 0, 'predicted_demand'] = 0
    return data, refresh_time

@st.cache_data
def fetch_previous_hour_data(hour, day_offset):
    prev_hour = hour - 1
    prev_day_offset = day_offset
    
    if prev_hour < 0:
        prev_hour = 23
        prev_day_offset -= 1
        
    prev_data, _ = fetch_data(prev_hour, prev_day_offset)
    return prev_data

def create_map(hour, day_offset):
    # Get data using cached function
    data, refresh_time = fetch_data(hour, day_offset)
    prev_data = fetch_previous_hour_data(hour, day_offset)
    
    # Check if data is empty or contains all NaN values
    if data.empty or data['latitude'].isna().all() or data['longitude'].isna().all():
        st.error("No data available for the selected time period")
        return None, None, refresh_time
    
    # Color scheme
    color_scheme = {
        'High Demand No Supply': '#FF0000',  # Bright red
        'Demand No Supply': '#ff4444',       # Lighter red
        'Demand With Supply': '#44aa44',     # Green
        'Supply No Demand': '#4444ff',       # Blue
        'No Activity': '#888888'             # Gray
    }

    def get_status(row):
        demand = row['predicted_demand']
        supply = row['net_supply_hours']
        has_supply = supply > 0.1
        
        if demand >= 2 and not has_supply:
            return 'High Demand No Supply'
        elif demand > 0 and not has_supply:
            return 'Demand No Supply'
        elif demand > 0 and has_supply:
            if supply > 24:
                st.warning(f"Warning: Unrealistic supply hours ({supply}) for hotspot {row['hotspot_label']}")
            return 'Demand With Supply'
        elif has_supply and demand <= 0:
            return 'Supply No Demand'
        return 'No Activity'
    
    data['status'] = data.apply(get_status, axis=1)

    m = folium.Map(location=[34.0522, -118.2437],  # LA coordinates
              zoom_start=13,
              tiles='cartodbpositron')

    # Add hotspot squares with trend indicators
    for idx, row in data.iterrows():
        if pd.notna(row['hotspot_label']):
            # Get previous hour demand for this hotspot
            prev_demand = prev_data[prev_data['hotspot_label'] == row['hotspot_label']]['predicted_demand'].iloc[0] if len(prev_data[prev_data['hotspot_label'] == row['hotspot_label']]) > 0 else 0
            
            # Calculate trend
            trend = "→"
            if row['predicted_demand'] > prev_demand:
                trend = "↑"
            elif row['predicted_demand'] < prev_demand:
                trend = "↓"
                
            bounds = get_square_bounds(row['latitude'], row['longitude'], 400)
            popup_content = f"""
            <b>Hotspot {row['hotspot_label']}</b><br>
            Predicted Demand: {row['predicted_demand']:.2f} {trend}<br>
            Previous Hour: {prev_demand:.2f}<br>
            Actual Offers: {row['num_offers']}<br>
            Supply Hours: {row['net_supply_hours']:.2f}<br>
            Status: {row['status']}
            """
            folium.Rectangle(
                bounds=bounds,
                color='black',
                weight=1,
                fill=True,
                fillColor=color_scheme[row['status']],
                fillOpacity=0.6,
                popup=popup_content
            ).add_to(m)

    return m, color_scheme, refresh_time

def main():
    st.title("Supply-Demand Distribution")
    
    # Get current time in PST
    pst = pytz.timezone('America/Los_Angeles')
    current_time = datetime.now(pst)
    current_hour = current_time.hour
    
    # Create two columns for the controls
    col1, col2 = st.columns(2)
    
    with col1:
        day_option = st.selectbox(
            "Select Day",
            options=["Today", "Yesterday"],
            index=1 if current_hour == 0 else 0,  # Default to Yesterday if it's midnight
            key="day_select"
        )
        day_offset = 0 if day_option == "Today" else -1
        
    with col2:
        # Calculate max hour based on current time
        max_hour = current_hour - 1 if day_offset == 0 else 23
        
        # Handle case where max_hour would be negative or 0
        if max_hour <= 0 and day_offset == 0:
            st.write("No data available yet for today. Please select yesterday.")
            max_hour = 23
            # Force selection of yesterday
            day_option = "Yesterday"
            day_offset = -1
        
        hour = st.slider(
            "Select Hour (24h)",
            min_value=0,
            max_value=max_hour,
            value=min(max_hour, 18),  # Default to 6PM or latest available hour
            key="hour_select"
        )
    
    # Display current selection
    selected_date = current_time.date() + timedelta(days=day_offset)
    display_time = datetime.combine(selected_date, datetime.min.time().replace(hour=hour))
    st.write(f"Showing Data for: {display_time.strftime('%Y-%m-%d %H:00')} - {(display_time + timedelta(hours=1)).strftime('%H:00')}")
    
    # Create and display map
    m, color_scheme, refresh_time = create_map(hour, day_offset)
    if m is not None and color_scheme is not None:
        st_folium(m, width=1400, height=600)
        
        # Display refresh time
        st.markdown(f"""
            <div style='text-align: right; color: #666; font-size: 0.8em; margin-top: -15px; margin-bottom: 10px;'>
                Last updated: {refresh_time}
            </div>
        """, unsafe_allow_html=True)
        
        st.markdown("""
            <style>
            .legend-container {
                text-align: center;
                padding: 12px;
                background-color: rgba(255, 255, 255, 0.9);
                margin-top: -20px;
                width: 100%;
                border-radius: 2px;
                box-shadow: 0 0 15px rgba(0, 0, 0, 0.1);
            }
            .legend-item {
                display: inline-flex;
                align-items: center;
                margin: 0 20px;
                color: black;
                font-family: system-ui;
            }
            .color-box {
                width: 20px;
                height: 20px;
                margin-right: 8px;
                border: 1px solid black;
            }
            </style>
        """, unsafe_allow_html=True)

        # Add legend items
        legend_items = "".join([
            f"""
            <div class="legend-item">
                <div class="color-box" style="background-color: {color}"></div>
                <span>{status}</span>
            </div>
            """ for status, color in color_scheme.items()
        ])
        
        st.markdown(f"""
            <div class="legend-container">
                {legend_items}
            </div>
        """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
