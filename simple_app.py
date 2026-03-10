import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os
import json

# Page config
st.set_page_config(
    page_title="Smart Traffic Big Data Analytics",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
    }
    .sub-header {
        font-size: 1.2rem;
        color: #666;
        text-align: center;
        margin-bottom: 2rem;
    }
    .kpi-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem;
        border-radius: 10px;
        color: white;
        text-align: center;
    }
    .kpi-value {
        font-size: 2.5rem;
        font-weight: bold;
    }
    .kpi-label {
        font-size: 1rem;
        opacity: 0.9;
    }
</style>
""", unsafe_allow_html=True)

# Initialize session state
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
    st.session_state.df = None

@st.cache_data
def load_data():
    """Load and cache data"""
    try:
        # Check for data file
        data_paths = [
            "data/traffic_data_0.json",
            "../traffic_data_0.json",
            "traffic_data_0.json"
        ]
        
        data_path = None
        for path in data_paths:
            if os.path.exists(path):
                data_path = path
                break
        
        if not data_path:
            st.error("Data file not found. Please ensure traffic_data_0.json is available.")
            return None
        
        with st.spinner("🔄 Loading big data... This may take a moment"):
            # Read JSON data
            with open(data_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
            
            df = pd.DataFrame(data)
            
            # Data preprocessing
            df['timestamp'] = pd.to_datetime(df['timestamp'])
            df['date'] = df['timestamp'].dt.date
            df['hour'] = df['timestamp'].dt.hour
            df['owner_name'] = df['owner'].apply(lambda x: x.get('name', '') if isinstance(x, dict) else '')
            df['license_number'] = df['owner'].apply(lambda x: x.get('license_number', '') if isinstance(x, dict) else '')
            df['street'] = df['road'].apply(lambda x: x.get('street', '') if isinstance(x, dict) else '')
            df['district'] = df['road'].apply(lambda x: x.get('district', '') if isinstance(x, dict) else '')
            df['city'] = df['road'].apply(lambda x: x.get('city', '') if isinstance(x, dict) else '')
            df['latitude'] = df['coordinates'].apply(lambda x: x.get('latitude', 0) if isinstance(x, dict) else 0)
            df['longitude'] = df['coordinates'].apply(lambda x: x.get('longitude', 0) if isinstance(x, dict) else 0)
            df['temperature'] = df['weather_condition'].apply(lambda x: x.get('temperature_celsius', 0) if isinstance(x, dict) else 0)
            df['humidity'] = df['weather_condition'].apply(lambda x: x.get('humidity_percentage', 0) if isinstance(x, dict) else 0)
            df['weather'] = df['weather_condition'].apply(lambda x: x.get('condition', '') if isinstance(x, dict) else '')
            df['congestion_level'] = df['traffic_status'].apply(lambda x: x.get('congestion_level', '') if isinstance(x, dict) else '')
            df['delay_minutes'] = df['traffic_status'].apply(lambda x: x.get('estimated_delay_minutes', 0) if isinstance(x, dict) else 0)
            df['alert_count'] = df['alerts'].apply(lambda x: len(x) if isinstance(x, list) else 0)
            df['has_speeding_alert'] = df['alerts'].apply(
                lambda x: any(alert.get('type') == 'Speeding' for alert in x) if isinstance(x, list) else False
            )
            df['has_fuel_alert'] = df['fuel_level_percentage'] < 20
            
            return df
    except Exception as e:
        st.error(f"Error loading data: {str(e)}")
        return None

# Sidebar
with st.sidebar:
    st.title("🚦 Smart Traffic Analytics")
    
    page = st.radio(
        "Select Page",
        ["📊 Dashboard", "📈 Data Explorer", "🚗 Vehicle Analytics", "⚠️ Alerts & Violations"]
    )
    
    st.divider()
    
    # Load data button
    if st.button("🔄 Load/Reload Data", use_container_width=True):
        st.session_state.df = load_data()
        if st.session_state.df is not None:
            st.session_state.data_loaded = True
            st.success(f"✅ Loaded {len(st.session_state.df):,} records")
    
    if st.session_state.data_loaded:
        st.info(f"📊 Records: {len(st.session_state.df):,}")
        st.info(f"🕐 Last updated: {datetime.now().strftime('%H:%M:%S')}")

# Main content
if not st.session_state.data_loaded:
    st.markdown('<div class="main-header">🚦 Smart Traffic Big Data Analytics</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Realtime Vehicle Monitoring & Lakehouse Architecture</div>', unsafe_allow_html=True)
    
    st.info("👈 Click 'Load/Reload Data' in the sidebar to start analyzing traffic data")
    
    # Architecture diagram
    st.subheader("🏗️ System Architecture")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.markdown("""
        **Data Ingestion**
        - 📥 Batch Upload
        - 🔄 Real-time Stream
        - 📡 IoT Sensors
        """)
    with col2:
        st.markdown("""
        **Processing Layer**
        - ⚡ Apache Spark
        - 🗄️ Delta Lake
        - 💾 MinIO (S3)
        """)
    with col3:
        st.markdown("""
        **Analytics Layer**
        - 📊 Streamlit Dashboard
        - 🗺️ Interactive Maps
        - 🔍 SQL Queries
        """)
    
else:
    df = st.session_state.df
    
    # Route to different pages
    if page == "📊 Dashboard":
        # Dashboard content
        st.markdown('<div class="main-header">📊 Smart Traffic Dashboard</div>', unsafe_allow_html=True)
        
        # KPI Cards
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            total_vehicles = len(df)
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value">{total_vehicles:,}</div>
                <div class="kpi-label">Total Vehicles</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            avg_speed = df['speed_kmph'].mean()
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value">{avg_speed:.1f}</div>
                <div class="kpi-label">Average Speed (km/h)</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            active_vehicles = len(df[df['timestamp'] > df['timestamp'].max() - pd.Timedelta(hours=1)])
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value">{active_vehicles:,}</div>
                <div class="kpi-label">Active Vehicles</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            high_congestion = len(df[df['congestion_level'] == 'High'])
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value">{high_congestion:,}</div>
                <div class="kpi-label">High Congestion Areas</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col5:
            total_alerts = df['alert_count'].sum()
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value">{total_alerts:,}</div>
                <div class="kpi-label">Total Alerts</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🚗 Vehicle Distribution by Type")
            vehicle_counts = df['vehicle_type'].value_counts()
            fig_pie = px.pie(
                values=vehicle_counts.values,
                names=vehicle_counts.index,
                title="Vehicle Types"
            )
            st.plotly_chart(fig_pie, use_container_width=True)
        
        with col2:
            st.subheader("📍 Traffic by District")
            district_counts = df['district'].value_counts().head(10)
            fig_bar = px.bar(
                x=district_counts.values,
                y=district_counts.index,
                orientation='h',
                title="Top 10 Districts by Vehicle Count"
            )
            st.plotly_chart(fig_bar, use_container_width=True)
        
        # Speed Distribution
        st.subheader("⚡ Speed Distribution")
        fig_hist = px.histogram(df, x='speed_kmph', nbins=30, title="Speed Distribution")
        st.plotly_chart(fig_hist, use_container_width=True)
        
    elif page == "📈 Data Explorer":
        st.subheader("📈 Data Explorer")
        
        # Filters
        col1, col2, col3 = st.columns(3)
        
        with col1:
            vehicle_types = st.multiselect(
                "Vehicle Types",
                options=df['vehicle_type'].unique(),
                default=list(df['vehicle_type'].unique())
            )
        
        with col2:
            districts = st.multiselect(
                "Districts",
                options=df['district'].unique(),
                default=list(df['district'].unique())[:5]
            )
        
        with col3:
            speed_range = st.slider(
                "Speed Range (km/h)",
                min_value=0,
                max_value=int(df['speed_kmph'].max()),
                value=(0, int(df['speed_kmph'].max()))
            )
        
        # Apply filters
        filtered_df = df[
            (df['vehicle_type'].isin(vehicle_types)) &
            (df['district'].isin(districts)) &
            (df['speed_kmph'] >= speed_range[0]) &
            (df['speed_kmph'] <= speed_range[1])
        ]
        
        st.info(f"📊 Filtered dataset: {len(filtered_df):,} records")
        
        # Display data
        st.dataframe(filtered_df.head(1000), use_container_width=True)
        
        # Export
        csv = filtered_df.to_csv(index=False)
        st.download_button(
            label="📥 Download as CSV",
            data=csv,
            file_name="traffic_data_export.csv",
            mime="text/csv"
        )
        
    elif page == "🚗 Vehicle Analytics":
        st.subheader("🚗 Vehicle Analytics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Speed by vehicle type
            fig_box = px.box(df, x='vehicle_type', y='speed_kmph', title="Speed by Vehicle Type")
            st.plotly_chart(fig_box, use_container_width=True)
        
        with col2:
            # Fuel level distribution
            fig_fuel = px.histogram(df, x='fuel_level_percentage', title="Fuel Level Distribution")
            st.plotly_chart(fig_fuel, use_container_width=True)
        
        # Vehicle performance table
        st.subheader("📊 Vehicle Performance Summary")
        vehicle_stats = df.groupby('vehicle_type').agg({
            'speed_kmph': 'mean',
            'fuel_level_percentage': 'mean',
            'passenger_count': 'mean',
            'vehicle_id': 'count'
        }).round(2)
        vehicle_stats.columns = ['Avg Speed (km/h)', 'Avg Fuel (%)', 'Avg Passengers', 'Count']
        st.dataframe(vehicle_stats, use_container_width=True)
        
    elif page == "⚠️ Alerts & Violations":
        st.subheader("⚠️ Alerts & Violations")
        
        col1, col2, col3 = st.columns(3)
        
        with col1:
            speeding_alerts = df['has_speeding_alert'].sum()
            st.metric("🏃 Speeding Violations", f"{speeding_alerts:,}")
        
        with col2:
            fuel_alerts = df['has_fuel_alert'].sum()
            st.metric("⛽ Low Fuel Alerts", f"{fuel_alerts:,}")
        
        with col3:
            high_congestion = len(df[df['congestion_level'] == 'High'])
            st.metric("🔴 High Congestion", f"{high_congestion:,}")
        
        # Speeding analysis
        st.subheader("🏃 Speeding Analysis")
        speeding_vehicles = df[df['has_speeding_alert']]
        if not speeding_vehicles.empty:
            fig_speed = px.histogram(speeding_vehicles, x='speed_kmph', title="Speed Distribution of Violating Vehicles")
            st.plotly_chart(fig_speed, use_container_width=True)
            
            # Top speeders
            st.subheader("🏆 Top Speeding Vehicles")
            top_speeders = df.nlargest(20, 'speed_kmph')[
                ['vehicle_id', 'vehicle_type', 'speed_kmph', 'district']
            ]
            st.dataframe(top_speeders, use_container_width=True)
        else:
            st.info("No speeding violations detected")
        
        # Low fuel vehicles
        st.subheader("⛽ Low Fuel Vehicles")
        low_fuel = df[df['has_fuel_alert']]
        if not low_fuel.empty:
            st.dataframe(low_fuel[['vehicle_id', 'vehicle_type', 'fuel_level_percentage', 'district']].head(20), use_container_width=True)
        else:
            st.info("No low fuel alerts")