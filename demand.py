import streamlit as st
import folium
from streamlit_folium import st_folium
from google.cloud import bigquery
from google.oauth2 import credentials
import pytz
from datetime import datetime, timedelta
import branca.colormap as cm

# Page config
st.set_page_config(page_title="Hotspot Demand Map", layout="wide")

# Initialize BigQuery client
@st.cache_resource
def get_bq_client():
    try:
        # Get credentials from Streamlit secrets
        credentials_dict = st.secrets["gcp_service_account"]
        
        # Create credentials object
        credentials_obj = credentials.Credentials(
            token=None,  # Token is handled by refresh
            refresh_token=credentials_dict['refresh_token'],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=credentials_dict['client_id'],
            client_secret=credentials_dict['client_secret']
        )
        
        # Create BigQuery client
        client = bigquery.Client(
            project='postmates-x',
            credentials=credentials_obj
        )
        return client
    except KeyError as e:
        st.error(f"Missing required credential field: {str(e)}")
        st.error("Please ensure all required fields (refresh_token, client_id, client_secret) are present in secrets.")
        raise
    except Exception as e:
        st.error(f"Error initializing BigQuery client: {str(e)}")
        st.error("Please ensure GCP credentials are properly configured in Streamlit secrets.")
        raise

try:
    bq = get_bq_client()
except Exception as e:
    st.error("Failed to initialize BigQuery client. Please check your credentials.")
    st.stop()

@st.cache_data
def fetch_data(hour):
    query = """
    WITH hotspots AS (
      SELECT
        label,
        date,
        part_of_day as hr,
        day_of_week,
        SAFE_CAST(delivery_count AS FLOAT64) as predicted_demand,
        hotspot_location,
        ST_GEOGFROMTEXT(
          CONCAT(
            'POLYGON((',
            ST_X(hotspot_location) - (buffer_size / (COS(ST_Y(hotspot_location) * ACOS(-1) / 180) * 111320)), ' ',
            ST_Y(hotspot_location) - (buffer_size / 111320), ', ',
            ST_X(hotspot_location) + (buffer_size / (COS(ST_Y(hotspot_location) * ACOS(-1) / 180) * 111320)), ' ',
            ST_Y(hotspot_location) - (buffer_size / 111320), ', ',
            ST_X(hotspot_location) + (buffer_size / (COS(ST_Y(hotspot_location) * ACOS(-1) / 180) * 111320)), ' ',
            ST_Y(hotspot_location) + (buffer_size / 111320), ', ',
            ST_X(hotspot_location) - (buffer_size / (COS(ST_Y(hotspot_location) * ACOS(-1) / 180) * 111320)), ' ',
            ST_Y(hotspot_location) + (buffer_size / 111320), ', ',
            ST_X(hotspot_location) - (buffer_size / (COS(ST_Y(hotspot_location) * ACOS(-1) / 180) * 111320)), ' ',
            ST_Y(hotspot_location) - (buffer_size / 111320),
            '))'
          )
        ) AS square_geometry
      FROM `serve-robotics.serve_analytics.stg_delivery_platform__hotspots`,
      UNNEST([325]) AS buffer_size
      WHERE label != '74'
      AND date = DATE(DATETIME(CURRENT_TIMESTAMP(), 'America/Los_Angeles'))
      AND day_of_week = LOWER(FORMAT_DATE('%A', DATE(DATETIME(CURRENT_TIMESTAMP(), 'America/Los_Angeles'))))
    ),

    ranked_hotspots AS (
      SELECT
        label,
        hotspot_location,
        square_geometry,
        hr,
        hotspots.predicted_demand
      FROM hotspots
    )

    SELECT
      label,
      hr,
      ST_ASTEXT(square_geometry) as square_geometry,
      ST_X(hotspot_location) as longitude,
      ST_Y(hotspot_location) as latitude,
      ROUND(CASE WHEN predicted_demand > 0 THEN predicted_demand ELSE 0 END, 1) as uber_eligible_offers
    FROM ranked_hotspots h
    LEFT JOIN `serve-robotics.serve_analytics.neighborhoods` n 
      ON ST_CONTAINS(n.polygon, h.hotspot_location)
    WHERE hr = @hour
    AND predicted_demand > 0
    """
    
    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("hour", "INTEGER", hour),
            ]
        )
        
        with st.spinner('Fetching data...'):
            data = bq.query(query, job_config=job_config).result().to_dataframe()
        return data
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return None

def get_color(demand):
    if demand <= 0.25:
        return '#00CC00'    # Bright green
    elif demand <= 0.5:
        return '#66CC00'    # Light green
    elif demand <= 0.75:
        return '#FFFF00'    # Yellow
    elif demand <= 1.0:
        return '#FF9933'    # Orange
    elif demand <= 2.0:
        return '#FF6666'    # Light red
    else:
        return '#FF0000'    # Bright red

def create_map(hour):
    # Get data using cached function
    data = fetch_data(hour)
    
    if data is None or data.empty:
        st.error("No data available for the selected hour.")
        return None
    
    # Create color scale
    colormap = cm.LinearColormap(
        colors=['#00CC00', '#66CC00', '#FFFF00', '#FF9933', '#FF6666', '#FF0000'],
        vmin=0,
        vmax=2.5,
        caption=f'Eligible Offers (Hour: {hour}:00)',
        index=[0, 0.25, 0.5, 0.75, 1, 2]
    )
    
    # Create base map centered on LA
    m = folium.Map(
        location=[34.0522, -118.2437],
        zoom_start=11,
        tiles='cartodbpositron'
    )
    
    # Add hotspot polygons
    for idx, row in data.iterrows():
        color = get_color(row['uber_eligible_offers'])
        
        folium.GeoJson(
            row['square_geometry'],
            style_function=lambda x, color=color: {
                'fillColor': color,
                'color': 'black',
                'weight': 1,
                'fillOpacity': 0.8
            },
            tooltip=f"Label: {row['label']}<br>Hour: {hour}:00<br>Eligible Offers: {row['uber_eligible_offers']:.2f}"
        ).add_to(m)
    
    # Add color scale
    colormap.add_to(m)
    
    return m

def main():
    st.title("LA Hotspot Demand Map")
    
    # Hour selector
    hour = st.slider(
        "Select Hour (24h)",
        min_value=8,
        max_value=22,
        value=13,
        key="hour_select"
    )
    
    # Display current selection
    pst = pytz.timezone('America/Los_Angeles')
    current_time = datetime.now(pst)
    st.write(f"Showing Data for: {current_time.strftime('%Y-%m-%d')} {hour}:00 - {hour+1}:00")
    
    # Create and display map
    m = create_map(hour)
    if m is not None:
        st_folium(m, width=1400, height=600)

if __name__ == "__main__":
    main()
