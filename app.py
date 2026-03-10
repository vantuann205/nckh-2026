import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import os

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

def load_data():
    """Load and cache data"""
    try:
        # Check if running in Docker or local
        data_path = "/data/traffic_data_0.json" if os.path.exists("/data") else "../traffic_data_0.json"
        
        if not os.path.exists(data_path):
            st.error(f"Data file not found at {data_path}")
            return None
        
        with st.spinner("🔄 Loading big data... This may take a moment"):
            df = pd.read_json(data_path, lines=False)
            
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
    st.image("https://img.icons8.com/color/96/000000/traffic-light.png", width=80)
    st.title("🚦 Navigation")
    
    page = st.radio(
        "Select Page",
        ["📊 Dashboard", "🗺️ Traffic Map", "📈 Data Explorer", "🚗 Vehicle Analytics", 
         "⚠️ Alerts & Violations", "🌤️ Weather Impact", "🔍 Query Engine"]
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
        from pages import dashboard
        dashboard.show(df)
    elif page == "🗺️ Traffic Map":
        from pages import traffic_map
        traffic_map.show(df)
    elif page == "📈 Data Explorer":
        from pages import data_explorer
        data_explorer.show(df)
    elif page == "🚗 Vehicle Analytics":
        from pages import vehicle_analytics
        vehicle_analytics.show(df)
    elif page == "⚠️ Alerts & Violations":
        from pages import alerts
        alerts.show(df)
    elif page == "🌤️ Weather Impact":
        from pages import weather_impact
        weather_impact.show(df)
    elif page == "🔍 Query Engine":
        from pages import query_engine
        query_engine.show(df)
