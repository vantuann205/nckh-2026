"""
Streamlit App với Smart Caching - Chỉ refresh khi có thay đổi
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime
import requests
import time

# Page config
st.set_page_config(
    page_title="Smart Traffic Analytics",
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
    .status-badge {
        display: inline-block;
        padding: 4px 12px;
        border-radius: 12px;
        font-size: 12px;
        font-weight: 600;
    }
    .status-online {
        background: #10b981;
        color: white;
    }
    .status-offline {
        background: #ef4444;
        color: white;
    }
</style>
""", unsafe_allow_html=True)

# API Configuration
API_BASE = "http://localhost:8000/api"

# Initialize session state
if 'last_update' not in st.session_state:
    st.session_state.last_update = None
if 'data_loaded' not in st.session_state:
    st.session_state.data_loaded = False
if 'auto_refresh' not in st.session_state:
    st.session_state.auto_refresh = True

def check_api_status():
    """Kiểm tra API có hoạt động không"""
    try:
        response = requests.get(f"{API_BASE}/status", timeout=2)
        return response.status_code == 200, response.json()
    except:
        return False, None

def fetch_summary():
    """Lấy dữ liệu tổng quan"""
    try:
        response = requests.get(f"{API_BASE}/summary", timeout=5)
        return response.json()
    except Exception as e:
        st.error(f"Lỗi khi lấy dữ liệu: {e}")
        return None

def fetch_stats(endpoint):
    """Lấy dữ liệu thống kê"""
    try:
        response = requests.get(f"{API_BASE}/stats/{endpoint}", timeout=5)
        return response.json()
    except:
        return []

def fetch_explorer_data(search="", vtype="", district="", limit=100):
    """Lấy dữ liệu explorer"""
    try:
        params = {"search": search, "vtype": vtype, "district": district, "limit": limit}
        response = requests.get(f"{API_BASE}/explorer", params=params, timeout=10)
        return response.json()
    except:
        return []

# Sidebar
with st.sidebar:
    st.title("🚦 Smart Traffic Analytics")
    
    # API Status
    api_online, status_data = check_api_status()
    
    if api_online:
        st.markdown(f'<span class="status-badge status-online">🟢 API Online</span>', unsafe_allow_html=True)
        if status_data:
            st.info(f"📊 Tổng: {status_data.get('total_records', 0):,} records")
            last_refresh = status_data.get('last_refresh', 'N/A')
            if last_refresh != 'N/A':
                st.info(f"🕐 Cập nhật: {last_refresh[:19]}")
    else:
        st.markdown(f'<span class="status-badge status-offline">🔴 API Offline</span>', unsafe_allow_html=True)
        st.error("⚠️ Không thể kết nối API. Vui lòng chạy: `python smart_server.py`")
    
    st.divider()
    
    # Navigation
    page = st.radio(
        "📍 Chọn trang",
        ["📊 Dashboard", "📈 Data Explorer", "🚗 Vehicle Analytics", "⚠️ Alerts"]
    )
    
    st.divider()
    
    # Auto refresh toggle
    st.session_state.auto_refresh = st.checkbox("🔄 Auto Refresh", value=st.session_state.auto_refresh)
    
    if st.session_state.auto_refresh:
        st.info("✅ Tự động cập nhật khi có thay đổi")
    
    # Manual refresh button
    if st.button("🔄 Làm mới ngay", use_container_width=True):
        try:
            requests.post(f"{API_BASE}/refresh", timeout=2)
            st.success("✅ Đã gửi lệnh refresh!")
            time.sleep(1)
            st.rerun()
        except:
            st.error("❌ Không thể refresh")

# Main content
if not api_online:
    st.markdown('<div class="main-header">🚦 Smart Traffic Analytics</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Hệ thống phân tích giao thông thông minh</div>', unsafe_allow_html=True)
    
    st.error("### ⚠️ API Server chưa chạy")
    st.info("""
    **Để khởi động hệ thống:**
    
    1. Mở terminal mới
    2. Chạy lệnh: `python smart_server.py`
    3. Đợi server khởi động
    4. Refresh trang này
    """)
    st.stop()

# Dashboard Page
if page == "📊 Dashboard":
    st.markdown('<div class="main-header">📊 Dashboard Tổng Quan</div>', unsafe_allow_html=True)
    
    # Fetch data
    summary = fetch_summary()
    
    if summary:
        # KPI Cards
        col1, col2, col3, col4, col5 = st.columns(5)
        
        with col1:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value">{summary['total']:,}</div>
                <div class="kpi-label">Tổng phương tiện</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col2:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value">{summary['avgSpeed']}</div>
                <div class="kpi-label">Tốc độ TB (km/h)</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col3:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value">{summary['active']:,}</div>
                <div class="kpi-label">Đang hoạt động</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col4:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value">{summary['alerts']:,}</div>
                <div class="kpi-label">Cảnh báo</div>
            </div>
            """, unsafe_allow_html=True)
        
        with col5:
            st.markdown(f"""
            <div class="kpi-card">
                <div class="kpi-value">{summary['congested']:,}</div>
                <div class="kpi-label">Khu vực kẹt xe</div>
            </div>
            """, unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("🚗 Phân bố loại xe")
            types_data = fetch_stats("types")
            if types_data:
                df_types = pd.DataFrame(types_data)
                fig = px.pie(df_types, values='count', names='vehicle_type', 
                           title="Loại phương tiện")
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("📍 Top 10 Quận")
            districts_data = fetch_stats("districts")
            if districts_data:
                df_districts = pd.DataFrame(districts_data[:10])
                fig = px.bar(df_districts, x='count', y='district', orientation='h',
                           title="Lưu lượng theo quận")
                st.plotly_chart(fig, use_container_width=True)
        
        # More charts
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("⚡ Phân bố tốc độ")
            speed_data = fetch_stats("speed")
            if speed_data:
                df_speed = pd.DataFrame(speed_data)
                fig = px.bar(df_speed, x='bucket', y='count',
                           title="Phân bố tốc độ (km/h)")
                st.plotly_chart(fig, use_container_width=True)
        
        with col2:
            st.subheader("🕐 Lưu lượng theo giờ")
            flow_data = fetch_stats("flow")
            if flow_data:
                df_flow = pd.DataFrame(flow_data)
                fig = px.line(df_flow, x='hour', y='count', markers=True,
                            title="Lưu lượng trong ngày")
                st.plotly_chart(fig, use_container_width=True)

# Data Explorer Page
elif page == "📈 Data Explorer":
    st.markdown('<div class="main-header">📈 Tra Cứu Dữ Liệu</div>', unsafe_allow_html=True)
    
    # Filters
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        search = st.text_input("🔍 Tìm kiếm", placeholder="ID hoặc tên...")
    
    with col2:
        types_data = fetch_stats("types")
        vehicle_types = [""] + [t['vehicle_type'] for t in types_data]
        vtype = st.selectbox("Loại xe", vehicle_types)
    
    with col3:
        districts_data = fetch_stats("districts")
        districts = [""] + [d['district'] for d in districts_data[:20]]
        district = st.selectbox("Quận", districts)
    
    with col4:
        limit = st.selectbox("Số lượng", [50, 100, 200, 500, 1000], index=1)
    
    # Fetch data
    if st.button("🔍 Tìm kiếm", use_container_width=True):
        with st.spinner("Đang tải dữ liệu..."):
            data = fetch_explorer_data(search, vtype, district, limit)
            
            if data:
                st.success(f"✅ Tìm thấy {len(data)} kết quả")
                
                # Display table
                df = pd.DataFrame(data)
                st.dataframe(df, use_container_width=True, height=600)
                
                # Download button
                csv = df.to_csv(index=False, encoding='utf-8-sig')
                st.download_button(
                    label="📥 Tải xuống CSV",
                    data=csv,
                    file_name=f"traffic_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                    mime="text/csv"
                )
            else:
                st.warning("Không tìm thấy dữ liệu")

# Vehicle Analytics Page
elif page == "🚗 Vehicle Analytics":
    st.markdown('<div class="main-header">🚗 Phân Tích Phương Tiện</div>', unsafe_allow_html=True)
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🌤️ Ảnh hưởng thời tiết")
        weather_data = fetch_stats("weather")
        if weather_data:
            df_weather = pd.DataFrame(weather_data)
            fig = px.pie(df_weather, values='count', names='weather',
                       title="Phân bố theo thời tiết")
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        st.subheader("📊 Thống kê loại xe")
        types_data = fetch_stats("types")
        if types_data:
            df_types = pd.DataFrame(types_data)
            fig = px.bar(df_types, x='vehicle_type', y='count',
                       title="Số lượng theo loại xe")
            st.plotly_chart(fig, use_container_width=True)
    
    # Speed analysis
    st.subheader("⚡ Phân tích tốc độ chi tiết")
    speed_data = fetch_stats("speed")
    if speed_data:
        df_speed = pd.DataFrame(speed_data)
        
        col1, col2 = st.columns(2)
        
        with col1:
            fig = px.bar(df_speed, x='bucket', y='count',
                       title="Phân bố tốc độ",
                       color='count',
                       color_continuous_scale='reds')
  