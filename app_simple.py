import streamlit as st
import pandas as pd
import plotly.express as px
import json
import os
from datetime import datetime

st.set_page_config(page_title="Traffic Analytics", layout="wide")

st.title("🚦 Smart Traffic Big Data Analytics")

# Find and load data immediately
data_paths = ["traffic_data_0.json", "data/traffic_data_0.json", "../traffic_data_0.json"]
data_path = None

for path in data_paths:
    if os.path.exists(path):
        data_path = path
        break

if data_path:
    st.success(f"✅ Found data: {data_path}")
    
    # Load data
    with st.spinner("⚡ Loading data..."):
        with open(data_path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        
        df = pd.DataFrame(data)
        
        # Quick processing
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        df['hour'] = df['timestamp'].dt.hour
        df['owner_name'] = df['owner'].apply(lambda x: x.get('name', '') if isinstance(x, dict) else '')
        df['street'] = df['road'].apply(lambda x: x.get('street', '') if isinstance(x, dict) else '')
        df['district'] = df['road'].apply(lambda x: x.get('district', '') if isinstance(x, dict) else '')
        df['latitude'] = df['coordinates'].apply(lambda x: x.get('latitude', 0) if isinstance(x, dict) else 0)
        df['longitude'] = df['coordinates'].apply(lambda x: x.get('longitude', 0) if isinstance(x, dict) else 0)
        df['weather'] = df['weather_condition'].apply(lambda x: x.get('condition', '') if isinstance(x, dict) else '')
        df['congestion_level'] = df['traffic_status'].apply(lambda x: x.get('congestion_level', '') if isinstance(x, dict) else '')
        df['alert_count'] = df['alerts'].apply(lambda x: len(x) if isinstance(x, list) else 0)
    
    st.success(f"✅ Loaded {len(df):,} records")
    
    # KPI Cards
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("🚗 Total Vehicles", f"{len(df):,}")
    with col2:
        st.metric("⚡ Avg Speed", f"{df['speed_kmph'].mean():.1f} km/h")
    with col3:
        st.metric("🔴 High Congestion", len(df[df['congestion_level'] == 'High']))
    with col4:
        st.metric("⛽ Low Fuel", len(df[df['fuel_level_percentage'] < 20]))
    with col5:
        st.metric("🚨 Alerts", df['alert_count'].sum())
    
    st.divider()
    
    # Tabs for different views
    tab1, tab2, tab3, tab4 = st.tabs(["📊 Dashboard", "🗺️ Map", "📈 Charts", "📋 Data"])
    
    with tab1:
        st.subheader("Dashboard Metrics")
        col1, col2 = st.columns(2)
        
        with col1:
            # Vehicle type distribution
            vehicle_counts = df['vehicle_type'].value_counts()
            fig = px.pie(values=vehicle_counts.values, names=vehicle_counts.index, title="Vehicle Types")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # District traffic
            district_counts = df['district'].value_counts().head(10)
            fig = px.bar(x=district_counts.values, y=district_counts.index, orientation='h', title="Top 10 Districts")
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        st.subheader("Traffic Map")
        st.info("Map visualization with vehicle locations")
        
        # Show sample locations
        sample_df = df[['latitude', 'longitude', 'vehicle_id', 'speed_kmph']].head(100)
        st.map(sample_df, latitude='latitude', longitude='longitude')
    
    with tab3:
        st.subheader("Analytics Charts")
        col1, col2 = st.columns(2)
        
        with col1:
            # Speed distribution
            fig = px.histogram(df, x='speed_kmph', nbins=30, title="Speed Distribution")
            st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            # Hourly traffic
            hourly = df.groupby('hour').size()
            fig = px.bar(x=hourly.index, y=hourly.values, title="Traffic by Hour")
            st.plotly_chart(fig, use_container_width=True)
    
    with tab4:
        st.subheader("Raw Data")
        
        # Filters
        col1, col2, col3 = st.columns(3)
        with col1:
            vehicle_type = st.selectbox("Vehicle Type", ["All"] + list(df['vehicle_type'].unique()))
        with col2:
            district = st.selectbox("District", ["All"] + list(df['district'].unique()))
        with col3:
            speed_range = st.slider("Speed Range", 0, int(df['speed_kmph'].max()), (0, int(df['speed_kmph'].max())))
        
        # Apply filters
        filtered_df = df.copy()
        if vehicle_type != "All":
            filtered_df = filtered_df[filtered_df['vehicle_type'] == vehicle_type]
        if district != "All":
            filtered_df = filtered_df[filtered_df['district'] == district]
        filtered_df = filtered_df[(filtered_df['speed_kmph'] >= speed_range[0]) & (filtered_df['speed_kmph'] <= speed_range[1])]
        
        st.info(f"Showing {len(filtered_df):,} records")
        
        # Display table
        display_cols = ['vehicle_id', 'vehicle_type', 'speed_kmph', 'street', 'district', 'fuel_level_percentage', 'congestion_level']
        st.dataframe(filtered_df[display_cols].head(1000), use_container_width=True)
        
        # Download button
        csv = filtered_df.to_csv(index=False)
        st.download_button("📥 Download CSV", csv, "traffic_data.csv", "text/csv")

else:
    st.error("❌ Data file not found!")
    st.info("Looking for: traffic_data_0.json")
    st.write("Current directory:", os.getcwd())
    st.write("Files in current directory:", os.listdir('.'))