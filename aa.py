import streamlit as st
import folium
from streamlit_folium import st_folium
from google.cloud import bigquery
from datetime import datetime, timedelta
import pytz
from math import sqrt
import pandas as pd

# Page config
st.set_page_config(page_title="Supply-Demand Distribution", layout="wide")

# Initialize BigQuery client
@st.cache_resource
def get_bq_client():
    credentials_dict = st.secrets["gcp_service_account"]
    credentials = google.oauth2.credentials.Credentials(
        None,
        refresh_token=credentials_dict['refresh_token'],
        token_uri="https://oauth2.googleapis.com/token",
        client_id=credentials_dict['client_id'],
        client_secret=credentials_dict['client_secret']
    )
    return bigquery.Client(project='postmates-x', credentials=credentials)

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
    return data

def create_map(hour, day_offset):
    # Get data using cached function
    data = fetch_data(hour, day_offset)
    
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

    # Create map
    center_lat = data['latitude'].mean()
    center_lon = data['longitude'].mean()
    m = folium.Map(location=[center_lat, center_lon],
                  zoom_start=13,
                  tiles='cartodbpositron')

    # Add hotspot squares
    for idx, row in data.iterrows():
        if pd.notna(row['hotspot_label']):
            bounds = get_square_bounds(row['latitude'], row['longitude'], 400)
            popup_content = f"""
            <b>Hotspot {row['hotspot_label']}</b><br>
            Predicted Demand: {row['predicted_demand']:.2f}<br>
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

    # Add legend directly to the map
    legend_html = """
        <div style="position: fixed; 
                    bottom: 50px; right: 10px; 
                    border:2px solid grey; z-index: 1000;
                    background-color: white;
                    padding: 10px;
                    opacity: 0.8;
                    ">
        <div style="font-size: 16px; font-weight: bold; margin-bottom: 10px;">Legend</div>
    """
    
    for status, color in color_scheme.items():
        legend_html += f"""
            <div style="display: flex; align-items: center; margin-bottom: 5px;">
                <div style="background-color: {color}; 
                            width: 20px; 
                            height: 20px; 
                            margin-right: 5px;
                            border: 1px solid black;">
                </div>
                <div>{status}</div>
            </div>
        """
    
    legend_html += "</div>"
    m.get_root().html.add_child(folium.Element(legend_html))

    return m

def main():
    st.title("Hotspot Demand Map")
    
    # Create two columns for the controls
    col1, col2 = st.columns(2)
    
    with col1:
        day_option = st.selectbox(
            "Select Day",
            options=["Today", "Yesterday"],
            index=0,
            key="day_select"
        )
        day_offset = 0 if day_option == "Today" else -1
        
    with col2:
        hour = st.slider(
            "Select Hour (24h)",
            min_value=0,
            max_value=23,
            value=18,
            key="hour_select"
        )
    
    # Display current selection
    pst = pytz.timezone('America/Los_Angeles')
    current_time = datetime.now(pst)
    selected_date = current_time.date() + timedelta(days=day_offset)
    display_time = datetime.combine(selected_date, datetime.min.time().replace(hour=hour))
    st.write(f"Showing Data for: {display_time.strftime('%Y-%m-%d %H:00')} - {(display_time + timedelta(hours=1)).strftime('%H:00')}")
    
    # Create and display map
    m = create_map(hour, day_offset)
    st_folium(m, width=1400, height=600)

if __name__ == "__main__":
    main()