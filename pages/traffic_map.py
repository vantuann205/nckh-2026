import streamlit as st
import pandas as pd
import folium
from streamlit_folium import st_folium
import plotly.express as px

def show(df):
    st.markdown('<div class="main-header">🗺️ Interactive Traffic Map</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Real-time Vehicle Locations & Traffic Density</div>', unsafe_allow_html=True)
    
    # Filters
    st.subheader("🔧 Map Filters")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        vehicle_types = ['All'] + list(df['vehicle_type'].unique())
        selected_vehicle_type = st.selectbox("Vehicle Type", vehicle_types)
    
    with col2:
        districts = ['All'] + list(df['district'].unique())
        selected_district = st.selectbox("District", districts)
    
    with col3:
        speed_range = st.slider("Speed Range (km/h)", 0, 120, (0, 120))
    
    with col4:
        max_points = st.slider("Max Points to Display", 100, 5000, 1000)
    
    # Apply filters
    filtered_df = df.copy()
    
    if selected_vehicle_type != 'All':
        filtered_df = filtered_df[filtered_df['vehicle_type'] == selected_vehicle_type]
    
    if selected_district != 'All':
        filtered_df = filtered_df[filtered_df['district'] == selected_district]
    
    filtered_df = filtered_df[
        (filtered_df['speed_kmph'] >= speed_range[0]) & 
        (filtered_df['speed_kmph'] <= speed_range[1])
    ]
    
    # Limit points for performance
    if len(filtered_df) > max_points:
        filtered_df = filtered_df.sample(n=max_points)
    
    st.info(f"📊 Displaying {len(filtered_df):,} vehicles on map")
    
    # Create map
    if not filtered_df.empty:
        # Center map on Ho Chi Minh City
        center_lat = filtered_df['latitude'].mean()
        center_lon = filtered_df['longitude'].mean()
        
        m = folium.Map(
            location=[center_lat, center_lon],
            zoom_start=11,
            tiles='OpenStreetMap'
        )
        
        # Add vehicle markers
        for idx, row in filtered_df.iterrows():
            # Color based on speed
            if row['speed_kmph'] < 20:
                color = 'red'  # Slow/congested
            elif row['speed_kmph'] < 60:
                color = 'orange'  # Normal
            else:
                color = 'green'  # Fast
            
            # Icon based on vehicle type
            if row['vehicle_type'] == 'Motorbike':
                icon = 'motorcycle'
            elif row['vehicle_type'] == 'Car':
                icon = 'car'
            elif row['vehicle_type'] == 'Bus':
                icon = 'bus'
            elif row['vehicle_type'] == 'Truck':
                icon = 'truck'
            else:
                icon = 'circle'
            
            # Create popup content
            popup_content = f"""
            <b>Vehicle ID:</b> {row['vehicle_id']}<br>
            <b>Type:</b> {row['vehicle_type']}<br>
            <b>Speed:</b> {row['speed_kmph']:.1f} km/h<br>
            <b>Location:</b> {row['street']}, {row['district']}<br>
            <b>Passengers:</b> {row['passenger_count']}<br>
            <b>Fuel:</b> {row['fuel_level_percentage']}%<br>
            <b>Congestion:</b> {row['congestion_level']}<br>
            <b>Weather:</b> {row['weather']}
            """
            
            folium.Marker(
                location=[row['latitude'], row['longitude']],
                popup=folium.Popup(popup_content, max_width=300),
                tooltip=f"{row['vehicle_id']} - {row['speed_kmph']:.1f} km/h",
                icon=folium.Icon(color=color, icon=icon, prefix='fa')
            ).add_to(m)
        
        # Add heatmap layer for congestion
        if st.checkbox("Show Congestion Heatmap"):
            from folium.plugins import HeatMap
            
            # Create heatmap data (lat, lon, weight)
            heat_data = []
            for idx, row in filtered_df.iterrows():
                # Weight based on congestion level
                if row['congestion_level'] == 'High':
                    weight = 3
                elif row['congestion_level'] == 'Medium':
                    weight = 2
                else:
                    weight = 1
                
                heat_data.append([row['latitude'], row['longitude'], weight])
            
            if heat_data:
                HeatMap(heat_data, radius=15, blur=10, gradient={
                    0.0: 'green',
                    0.5: 'yellow', 
                    1.0: 'red'
                }).add_to(m)
        
        # Display map
        map_data = st_folium(m, width=1200, height=600)
        
        # Map statistics
        col1, col2, col3 = st.columns(3)
        
        with col1:
            st.metric("🚗 Vehicles on Map", len(filtered_df))
        
        with col2:
            avg_speed_map = filtered_df['speed_kmph'].mean()
            st.metric("⚡ Average Speed", f"{avg_speed_map:.1f} km/h")
        
        with col3:
            congestion_count = len(filtered_df[filtered_df['congestion_level'] == 'High'])
            st.metric("🔴 High Congestion", congestion_count)
    
    else:
        st.warning("No vehicles found matching the selected filters")
    
    # Traffic density by district
    st.subheader("📊 Traffic Density Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # District traffic volume
        district_traffic = filtered_df['district'].value_counts().head(10)
        if not district_traffic.empty:
            fig_district = px.bar(
                x=district_traffic.values,
                y=district_traffic.index,
                orientation='h',
                title="Traffic Volume by District",
                color=district_traffic.values,
                color_continuous_scale='viridis'
            )
            fig_district.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_district, use_container_width=True)
    
    with col2:
        # Speed vs Congestion scatter
        if not filtered_df.empty:
            fig_scatter = px.scatter(
                filtered_df.sample(min(1000, len(filtered_df))),
                x='speed_kmph',
                y='delay_minutes',
                color='congestion_level',
                size='passenger_count',
                hover_data=['vehicle_type', 'district'],
                title="Speed vs Delay by Congestion Level"
            )
            st.plotly_chart(fig_scatter, use_container_width=True)
    
    # Hotspot analysis
    st.subheader("🔥 Traffic Hotspots")
    
    # Find areas with high vehicle density
    if not filtered_df.empty:
        # Group by approximate location (rounded coordinates)
        filtered_df['lat_rounded'] = filtered_df['latitude'].round(3)
        filtered_df['lon_rounded'] = filtered_df['longitude'].round(3)
        
        hotspots = filtered_df.groupby(['lat_rounded', 'lon_rounded', 'street', 'district']).agg({
            'vehicle_id': 'count',
            'speed_kmph': 'mean',
            'congestion_level': lambda x: x.mode().iloc[0] if not x.empty else 'Unknown'
        }).reset_index()
        
        hotspots = hotspots.sort_values('vehicle_id', ascending=False).head(10)
        hotspots.columns = ['Latitude', 'Longitude', 'Street', 'District', 'Vehicle Count', 'Avg Speed', 'Congestion Level']
        
        st.dataframe(hotspots, use_container_width=True)