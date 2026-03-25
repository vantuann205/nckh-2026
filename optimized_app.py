"""
Ứng dụng Streamlit tối ưu với caching thông minh
Chỉ làm mới khi có thay đổi thực sự
"""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import requests
import json
import time
from datetime import datetime
import os
from pathlib import Path

# Cấu hình trang
st.set_page_config(
    page_title="🚦 Smart Traffic Analytics",
    page_icon="🚦",
    layout="wide",
    initial_sidebar_state="expanded"
)

# CSS tùy chỉnh
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        text-align: center;
        margin-bottom: 1rem;
        text-shadow: 2px 2px 4px rgba(0,0,0,0.1);
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
        border-radius: 15px;
        color: white;
        text-align: center;
        box-shadow: 0 8px 32px rgba(0,0,0,0.1);
        backdrop-filter: blur(10px);
        border: 1px solid rgba(255,255,255,0.2);
    }
    .kpi-value {
        font-size: 2.5rem;
        font-weight: bold;
        margin-bottom: 0.5rem;
    }
    .kpi-label {
        font-size: 1rem;
        opacity: 0.9;
    }
    .status-indicator {
        display: inline-block;
        width: 12px;
        height: 12px;
        border-radius: 50%;
        margin-right: 8px;
    }
    .status-healthy { background-color: #10b981; }
    .status-warning { background-color: #f59e0b; }
    .status-error { background-color: #ef4444; }
    
    .metric-container {
        background: white;
        padding: 1rem;
        border-radius: 10px;
        box-shadow: 0 2px 10px rgba(0,0,0,0.1);
        border-left: 4px solid #3b82f6;
    }
</style>
""", unsafe_allow_html=True)

# API Configuration
API_BASE = "http://localhost:8000/api"

# Cache configuration
@st.cache_data(ttl=30)  # Cache 30 giây
def fetch_api_data(endpoint):
    """Fetch data từ API với caching"""
    try:
        response = requests.get(f"{API_BASE}/{endpoint}", timeout=10)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"❌ Lỗi kết nối API: {e}")
        return None

@st.cache_data(ttl=60)  # Cache 1 phút cho dữ liệu lớn
def fetch_explorer_data(search="", vtype="", district="", limit=1000):
    """Fetch dữ liệu explorer với caching"""
    try:
        params = {"limit": limit}
        if search: params["search"] = search
        if vtype: params["vtype"] = vtype
        if district: params["district"] = district
        
        response = requests.get(f"{API_BASE}/explorer", params=params, timeout=15)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        st.error(f"❌ Lỗi tải dữ liệu: {e}")
        return []

def check_api_status():
    """Kiểm tra trạng thái API"""
    try:
        response = requests.get(f"{API_BASE}/status", timeout=5)
        if response.status_code == 200:
            return response.json()
        return None
    except:
        return None

def format_number(num):
    """Format số với dấu phẩy"""
    return f"{num:,}"

def create_kpi_card(title, value, subtitle="", icon="📊"):
    """Tạo KPI card"""
    return f"""
    <div class="kpi-card">
        <div style="font-size: 1.5rem; margin-bottom: 0.5rem;">{icon}</div>
        <div class="kpi-value">{value}</div>
        <div class="kpi-label">{title}</div>
        {f'<div style="font-size: 0.9rem; opacity: 0.8; margin-top: 0.5rem;">{subtitle}</div>' if subtitle else ''}
    </div>
    """

# Sidebar
with st.sidebar:
    st.markdown("### 🚦 Smart Traffic Analytics")
    st.markdown("---")
    
    # API Status
    api_status = check_api_status()
    if api_status:
        status_color = "status-healthy"
        status_text = "🟢 Kết nối thành công"
        st.markdown(f'<div><span class="status-indicator {status_color}"></span>{status_text}</div>', 
                   unsafe_allow_html=True)
        st.info(f"📊 Tổng: {format_number(api_status.get('total_records', 0))} bản ghi")
        if api_status.get('last_refresh'):
            refresh_time = api_status['last_refresh'][:19].replace('T', ' ')
            st.info(f"🕐 Cập nhật: {refresh_time}")
    else:
        st.markdown('<div><span class="status-indicator status-error"></span>🔴 Không kết nối được API</div>', 
                   unsafe_allow_html=True)
        st.error("Vui lòng khởi động backend server")
    
    st.markdown("---")
    
    # Navigation
    page = st.radio(
        "📍 Chọn trang",
        ["📊 Tổng quan", "🗺️ Bản đồ", "📈 Phân tích dữ liệu", "🚗 Phân tích xe", "⚠️ Cảnh báo"],
        key="navigation"
    )
    
    st.markdown("---")
    
    # Manual refresh
    if st.button("🔄 Làm mới dữ liệu", use_container_width=True):
        st.cache_data.clear()
        try:
            requests.post(f"{API_BASE}/refresh", timeout=5)
            st.success("✅ Đã yêu cầu làm mới!")
        except:
            st.warning("⚠️ Không thể kết nối để làm mới")
        st.rerun()

# Main content
if not api_status:
    st.markdown('<div class="main-header">🚦 Smart Traffic Analytics</div>', unsafe_allow_html=True)
    st.error("❌ Không thể kết nối đến backend API. Vui lòng:")
    st.markdown("""
    1. Chạy lệnh: `python smart_server.py`
    2. Hoặc chạy file: `run_simple.bat`
    3. Đảm bảo port 8000 không bị chiếm dụng
    """)
    st.stop()

# Load data based on page
if page == "📊 Tổng quan":
    st.markdown('<div class="main-header">📊 Tổng quan Giao thông</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Dashboard theo thời gian thực</div>', unsafe_allow_html=True)
    
    # Load summary data
    summary = fetch_api_data("summary")
    if not summary:
        st.error("Không thể tải dữ liệu tổng quan")
        st.stop()
    
    # KPI Cards
    col1, col2, col3, col4, col5 = st.columns(5)
    
    with col1:
        st.markdown(create_kpi_card(
            "Tổng số xe", 
            format_number(summary.get('total', 0)),
            "Đang hoạt động",
            "🚗"
        ), unsafe_allow_html=True)
    
    with col2:
        st.markdown(create_kpi_card(
            "Tốc độ TB", 
            f"{summary.get('avgSpeed', 0)} km/h",
            "Trung bình toàn mạng",
            "⚡"
        ), unsafe_allow_html=True)
    
    with col3:
        st.markdown(create_kpi_card(
            "Xe hoạt động", 
            format_number(summary.get('active', 0)),
            "Trong 1 giờ qua",
            "🟢"
        ), unsafe_allow_html=True)
    
    with col4:
        st.markdown(create_kpi_card(
            "Cảnh báo", 
            format_number(summary.get('alerts', 0)),
            "Vi phạm & nhiên liệu",
            "⚠️"
        ), unsafe_allow_html=True)
    
    with col5:
        st.markdown(create_kpi_card(
            "Kẹt xe", 
            format_number(summary.get('congested', 0)),
            "Khu vực tắc nghẽn",
            "🔴"
        ), unsafe_allow_html=True)
    
    st.markdown("<br>", unsafe_allow_html=True)
    
    # Charts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📈 Lưu lượng theo giờ")
        flow_data = fetch_api_data("stats/flow")
        if flow_data:
            df_flow = pd.DataFrame(flow_data)
            fig_flow = px.line(
                df_flow, x='hour', y='count',
                title="Lưu lượng giao thông 24h",
                markers=True
            )
            fig_flow.update_layout(
                xaxis_title="Giờ trong ngày",
                yaxis_title="Số lượng xe",
                showlegend=False
            )
            st.plotly_chart(fig_flow, use_container_width=True)
    
    with col2:
        st.subheader("🚗 Phân bố loại xe")
        type_data = fetch_api_data("stats/types")
        if type_data:
            df_types = pd.DataFrame(type_data)
            fig_types = px.pie(
                df_types, values='count', names='vehicle_type',
                title="Tỷ lệ các loại phương tiện"
            )
            st.plotly_chart(fig_types, use_container_width=True)
    
    # Speed distribution
    st.subheader("⚡ Phân bố tốc độ")
    speed_data = fetch_api_data("stats/speed")
    if speed_data:
        df_speed = pd.DataFrame(speed_data)
        fig_speed = px.bar(
            df_speed, x='bucket', y='count',
            title="Phân bố tốc độ của các phương tiện",
            color='count',
            color_continuous_scale='viridis'
        )
        fig_speed.update_layout(
            xaxis_title="Khoảng tốc độ (km/h)",
            yaxis_title="Số lượng xe"
        )
        st.plotly_chart(fig_speed, use_container_width=True)
    
    # Districts
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("📍 Top 10 quận có nhiều xe nhất")
        district_data = fetch_api_data("stats/districts")
        if district_data:
            df_districts = pd.DataFrame(district_data[:10])
            fig_districts = px.bar(
                df_districts, x='count', y='district',
                orientation='h',
                title="Lưu lượng theo quận/huyện"
            )
            fig_districts.update_layout(yaxis={'categoryorder':'total ascending'})
            st.plotly_chart(fig_districts, use_container_width=True)
    
    with col2:
        st.subheader("🌤️ Điều kiện thời tiết")
        weather_data = fetch_api_data("stats/weather")
        if weather_data:
            df_weather = pd.DataFrame(weather_data)
            fig_weather = px.pie(
                df_weather, values='count', names='weather',
                title="Phân bố thời tiết"
            )
            st.plotly_chart(fig_weather, use_container_width=True)

elif page == "📈 Phân tích dữ liệu":
    st.markdown('<div class="main-header">📈 Phân tích Dữ liệu</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Tra cứu và lọc dữ liệu chi tiết</div>', unsafe_allow_html=True)
    
    # Filters
    st.subheader("🔧 Bộ lọc dữ liệu")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        search_term = st.text_input("🔍 Tìm kiếm", placeholder="ID xe, tên chủ xe...")
    
    with col2:
        # Get vehicle types for filter
        type_data = fetch_api_data("stats/types")
        vehicle_types = [""] + [t['vehicle_type'] for t in type_data] if type_data else [""]
        selected_type = st.selectbox("🚗 Loại xe", vehicle_types)
    
    with col3:
        # Get districts for filter
        district_data = fetch_api_data("stats/districts")
        districts = [""] + [d['district'] for d in district_data[:20]] if district_data else [""]
        selected_district = st.selectbox("📍 Quận/Huyện", districts)
    
    with col4:
        limit = st.selectbox("📊 Số bản ghi", [100, 500, 1000, 2000], index=2)
    
    # Load filtered data
    explorer_data = fetch_explorer_data(search_term, selected_type, selected_district, limit)
    
    if explorer_data:
        st.success(f"✅ Tìm thấy {len(explorer_data)} bản ghi")
        
        # Convert to DataFrame
        df = pd.DataFrame(explorer_data)
        
        # Display table
        st.subheader("📋 Dữ liệu chi tiết")
        
        # Format display
        display_df = df.copy()
        display_df['speed_kmph'] = display_df['speed_kmph'].round(1)
        display_df['fuel_level'] = display_df['fuel_level'].round(1)
        
        st.dataframe(display_df, use_container_width=True, height=400)
        
        # Download
        csv = df.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            label="📥 Tải xuống CSV",
            data=csv,
            file_name=f"traffic_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv"
        )
        
        # Quick stats
        st.subheader("📊 Thống kê nhanh")
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.metric("Tốc độ trung bình", f"{df['speed_kmph'].mean():.1f} km/h")
        
        with col2:
            st.metric("Nhiên liệu TB", f"{df['fuel_level'].mean():.1f}%")
        
        with col3:
            speeding = len(df[df['speed_kmph'] > 80])
            st.metric("Xe chạy quá tốc độ", speeding)
        
        with col4:
            low_fuel = len(df[df['fuel_level'] < 20])
            st.metric("Xe sắp hết xăng", low_fuel)
    
    else:
        st.warning("⚠️ Không tìm thấy dữ liệu phù hợp")

elif page == "🗺️ Bản đồ":
    st.markdown('<div class="main-header">🗺️ Bản đồ Giao thông</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Vị trí xe theo thời gian thực</div>', unsafe_allow_html=True)
    
    # Load map data
    try:
        map_data = requests.get(f"{API_BASE}/map?limit=1000", timeout=10).json()
        
        if map_data:
            df_map = pd.DataFrame(map_data)
            
            # Filter valid coordinates
            df_map = df_map[(df_map['lat'] != 0) & (df_map['lng'] != 0)]
            
            if not df_map.empty:
                st.success(f"📍 Hiển thị {len(df_map)} xe trên bản đồ")
                
                # Map
                st.map(df_map[['lat', 'lng']], zoom=11)
                
                # Map statistics
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("🚗 Xe trên bản đồ", len(df_map))
                
                with col2:
                    avg_speed = df_map['speed_kmph'].mean()
                    st.metric("⚡ Tốc độ TB", f"{avg_speed:.1f} km/h")
                
                with col3:
                    high_speed = len(df_map[df_map['speed_kmph'] > 80])
                    st.metric("🏃 Xe chạy nhanh", high_speed)
                
                # Speed heatmap
                st.subheader("🌡️ Bản đồ nhiệt tốc độ")
                fig_map = px.scatter_mapbox(
                    df_map.sample(min(500, len(df_map))),  # Sample for performance
                    lat="lat", lon="lng",
                    color="speed_kmph",
                    size="speed_kmph",
                    hover_data=["vehicle_id", "vehicle_type"],
                    color_continuous_scale="viridis",
                    mapbox_style="open-street-map",
                    zoom=10,
                    height=600
                )
                st.plotly_chart(fig_map, use_container_width=True)
            else:
                st.warning("⚠️ Không có dữ liệu tọa độ hợp lệ")
        else:
            st.error("❌ Không thể tải dữ liệu bản đồ")
    
    except Exception as e:
        st.error(f"❌ Lỗi tải bản đồ: {e}")

elif page == "⚠️ Cảnh báo":
    st.markdown('<div class="main-header">⚠️ Cảnh báo & Vi phạm</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Giám sát vi phạm giao thông</div>', unsafe_allow_html=True)
    
    # Load data for alerts
    explorer_data = fetch_explorer_data(limit=2000)
    
    if explorer_data:
        df = pd.DataFrame(explorer_data)
        
        # Calculate violations
        speeding = df[df['speed_kmph'] > 80]
        low_fuel = df[df['fuel_level'] < 20]
        critical_fuel = df[df['fuel_level'] < 10]
        
        # Alert metrics
        col1, col2, col3, col4 = st.columns(4)
        
        with col1:
            st.markdown(create_kpi_card(
                "Vi phạm tốc độ", 
                len(speeding),
                f"{len(speeding)/len(df)*100:.1f}% tổng số xe",
                "🏃"
            ), unsafe_allow_html=True)
        
        with col2:
            st.markdown(create_kpi_card(
                "Sắp hết xăng", 
                len(low_fuel),
                "< 20% nhiên liệu",
                "⛽"
            ), unsafe_allow_html=True)
        
        with col3:
            st.markdown(create_kpi_card(
                "Nguy cấp", 
                len(critical_fuel),
                "< 10% nhiên liệu",
                "🚨"
            ), unsafe_allow_html=True)
        
        with col4:
            high_congestion = len(df[df['congestion'] == 'High'])
            st.markdown(create_kpi_card(
                "Kẹt xe cao", 
                high_congestion,
                "Khu vực tắc nghẽn",
                "🔴"
            ), unsafe_allow_html=True)
        
        st.markdown("<br>", unsafe_allow_html=True)
        
        # Violation details
        if not speeding.empty:
            st.subheader("🏃 Top xe vi phạm tốc độ")
            speeding_display = speeding.nlargest(20, 'speed_kmph')[
                ['vehicle_id', 'speed_kmph', 'district', 'owner_name']
            ].round(1)
            st.dataframe(speeding_display, use_container_width=True)
        
        if not critical_fuel.empty:
            st.subheader("🚨 Xe cần tiếp nhiên liệu gấp")
            critical_display = critical_fuel.nsmallest(20, 'fuel_level')[
                ['vehicle_id', 'fuel_level', 'district', 'owner_name']
            ].round(1)
            st.dataframe(critical_display, use_container_width=True)
        
        # Charts
        col1, col2 = st.columns(2)
        
        with col1:
            if not speeding.empty:
                st.subheader("📊 Vi phạm theo loại xe")
                violation_by_type = speeding.groupby('vehicle_type').size().reset_index(name='count')
                fig_vio = px.bar(violation_by_type, x='vehicle_type', y='count',
                               title="Số lượng vi phạm tốc độ theo loại xe")
                st.plotly_chart(fig_vio, use_container_width=True)
        
        with col2:
            st.subheader("⛽ Phân bố mức nhiên liệu")
            fig_fuel = px.histogram(df, x='fuel_level', nbins=20,
                                  title="Phân bố mức nhiên liệu của đội xe")
            fig_fuel.add_vline(x=20, line_dash="dash", line_color="red",
                             annotation_text="Ngưỡng cảnh báo")
            st.plotly_chart(fig_fuel, use_container_width=True)
    
    else:
        st.error("❌ Không thể tải dữ liệu cảnh báo")

# Auto refresh
if st.sidebar.checkbox("🔄 Tự động làm mới (30s)"):
    time.sleep(30)
    st.rerun()

# Footer
st.markdown("---")
st.markdown(
    "<div style='text-align: center; color: #666; font-size: 0.9rem;'>"
    "🚦 Smart Traffic Analytics | Powered by Streamlit & FastAPI"
    "</div>", 
    unsafe_allow_html=True
)