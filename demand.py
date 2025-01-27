import streamlit as st
import folium
from streamlit_folium import st_folium
from google.cloud import bigquery
from google.oauth2 import credentials
import pytz
from datetime import datetime, timedelta
import branca.colormap as cm
import json
import re
import pandas as pd
import base64
import io
from folium.plugins import MarkerCluster

# Page config
st.set_page_config(page_title="Hotspot Demand Map", layout="wide")

# Initialize BigQuery client
@st.cache_resource
def get_bq_client():
    try:
        credentials_dict = st.secrets["gcp_service_account"]
        credentials_obj = credentials.Credentials(
            token=None,
            refresh_token=credentials_dict['refresh_token'],
            token_uri="https://oauth2.googleapis.com/token",
            client_id=credentials_dict['client_id'],
            client_secret=credentials_dict['client_secret']
        )
        return bigquery.Client(project='postmates-x', credentials=credentials_obj)
    except Exception as e:
        st.error(f"Error initializing BigQuery client: {str(e)}")
        raise

def download_link(df, filename, text):
    """Generate a link to download the DataFrame as CSV"""
    csv = df.to_csv(index=False)
    b64 = base64.b64encode(csv.encode()).decode()
    href = f'<a href="data:file/csv;base64,{b64}" download="{filename}">{text}</a>'
    return href

def parse_wkt_polygon(wkt_string):
    coords_str = re.search(r'\(\((.*)\)\)', wkt_string).group(1)
    coords_pairs = coords_str.split(',')
    coordinates = []
    for pair in coords_pairs:
        lon, lat = map(float, pair.strip().split())
        coordinates.append([lat, lon])
    return coordinates

@st.cache_data
def fetch_data(start_hour, end_hour):
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
    WHERE hr BETWEEN @start_hour AND @end_hour
    AND predicted_demand > 0
    ORDER BY hr, label
    """
    
    try:
        job_config = bigquery.QueryJobConfig(
            query_parameters=[
                bigquery.ScalarQueryParameter("start_hour", "INTEGER", start_hour),
                bigquery.ScalarQueryParameter("end_hour", "INTEGER", end_hour),
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

def create_map(data, hour=None, use_clustering=False):
    if data is None or data.empty:
        st.error("No data available for the selected time range.")
        return None
    
    # Filter for specific hour if provided
    if hour is not None:
        data = data[data['hr'] == hour]
    
    # Create color scale
    colormap = cm.LinearColormap(
        colors=['#00CC00', '#66CC00', '#FFFF00', '#FF9933', '#FF6666', '#FF0000'],
        vmin=0,
        vmax=2.5,
        caption='Eligible Offers',
        index=[0, 0.25, 0.5, 0.75, 1, 2]
    )
    
    # Create base map centered on LA
    m = folium.Map(
        location=[34.0522, -118.2437],
        zoom_start=11,
        tiles='cartodbpositron'
    )
    
    if use_clustering:
        # Create a marker cluster group
        marker_cluster = MarkerCluster(
            name='Hotspots',
            overlay=True,
            control=True,
            icon_create_function=None
        )
        
        # Add markers to cluster
        for idx, row in data.iterrows():
            color = get_color(row['uber_eligible_offers'])
            
            # Create a circular marker for the cluster view
            folium.CircleMarker(
                location=[row['latitude'], row['longitude']],
                radius=20,
                color='black',
                weight=1,
                fillColor=color,
                fillOpacity=0.8,
                popup=f"""
                <div style='width: 150px'>
                    <b>Hotspot {row['label']}</b><br>
                    Hour: {row['hr']}:00<br>
                    Eligible Offers: {row['uber_eligible_offers']:.2f}
                </div>
                """
            ).add_to(marker_cluster)
        
        marker_cluster.add_to(m)
        
    else:
        # Add individual hotspot polygons (original visualization)
        for idx, row in data.iterrows():
            color = get_color(row['uber_eligible_offers'])
            
            try:
                coordinates = parse_wkt_polygon(row['square_geometry'])
                
                folium.Polygon(
                    locations=coordinates,
                    color='black',
                    weight=1,
                    fillColor=color,
                    fillOpacity=0.8,
                    tooltip=f"Label: {row['label']}<br>Hour: {row['hr']}:00<br>Eligible Offers: {row['uber_eligible_offers']:.2f}"
                ).add_to(m)
            except Exception as e:
                st.warning(f"Error plotting hotspot {row['label']}: {str(e)}")
                continue
    
    # Add color scale
    colormap.add_to(m)
    
    return m

def main():
    st.title("LA Hotspot Demand Map")
    
    # Time range selector
    col1, col2 = st.columns(2)
    with col1:
        start_hour = st.slider(
            "Start Hour (24h)",
            min_value=8,
            max_value=22,
            value=13,
            key="start_hour"
        )
    with col2:
        end_hour = st.slider(
            "End Hour (24h)",
            min_value=start_hour,
            max_value=22,
            value=min(start_hour + 2, 22),
            key="end_hour"
        )
    
    # Fetch data for the entire range
    data = fetch_data(start_hour, end_hour)
    
    if data is not None and not data.empty:
        # Display current selection
        pst = pytz.timezone('America/Los_Angeles')
        current_time = datetime.now(pst)
        st.write(f"Showing Data for: {current_time.strftime('%Y-%m-%d')} {start_hour}:00 - {end_hour}:00")
        
        # Add export button
        st.markdown(download_link(data, 
                                f"hotspot_data_{start_hour}-{end_hour}.csv", 
                                "📥 Download Data as CSV"), 
                   unsafe_allow_html=True)
        
        # Display summary statistics
        st.subheader("Summary Statistics")
        total_offers = data['uber_eligible_offers'].sum()
        avg_offers = data['uber_eligible_offers'].mean()
        num_hotspots = len(data['label'].unique())
        st.write(f"Total Eligible Offers: {total_offers:.1f}")
        st.write(f"Average Offers per Hotspot: {avg_offers:.1f}")
        st.write(f"Number of Active Hotspots: {num_hotspots}")
        
        # Map controls
        col1, col2 = st.columns(2)
        with col1:
            selected_hour = st.slider(
                "Select Hour to View on Map",
                min_value=start_hour,
                max_value=end_hour,
                value=start_hour,
                key="map_hour"
            )
        with col2:
            use_clustering = st.checkbox("Enable Clustering", value=False, 
                                      help="Group nearby hotspots when zoomed out")
        
        # Create and display map for selected hour
        m = create_map(data, selected_hour, use_clustering)
        if m is not None:
            st_folium(m, width=1400, height=600)

if __name__ == "__main__":
    try:
        bq = get_bq_client()
        main()
    except Exception as e:
        st.error("Failed to initialize application. Please check your credentials.")
        st.stop()
