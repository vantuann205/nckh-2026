import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import datetime, timedelta

def show(df):
    st.markdown('<div class="main-header">📊 Smart Traffic Dashboard</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Realtime Vehicle Monitoring</div>', unsafe_allow_html=True)
    
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
        # Active vehicles (simulated as vehicles in last hour)
        recent_time = df['timestamp'].max() - timedelta(hours=1)
        active_vehicles = len(df[df['timestamp'] > recent_time])
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
    
    # Charts Row 1
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("🚗 Vehicle Distribution by Type")
        vehicle_counts = df['vehicle_type'].value_counts()
        fig_pie = px.pie(
            values=vehicle_counts.values,
            names=vehicle_counts.index,
            title="Vehicle Types",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        st.subheader("📍 Traffic by District")
        district_counts = df['district'].value_counts().head(10)
        fig_bar = px.bar(
            x=district_counts.values,
            y=district_counts.index,
            orientation='h',
            title="Top 10 Districts by Vehicle Count",
            color=district_counts.values,
            color_continuous_scale='viridis'
        )
        fig_bar.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_bar, use_container_width=True)
    
    # Charts Row 2
    col1, col2 = st.columns(2)
    
    with col1:
        st.subheader("⚡ Speed Distribution")
        speed_bins = pd.cut(df['speed_kmph'], bins=[0, 20, 40, 60, 80, 120], labels=['0-20', '20-40', '40-60', '60-80', '80+'])
        speed_counts = speed_bins.value_counts().sort_index()
        
        fig_hist = px.bar(
            x=speed_counts.index,
            y=speed_counts.values,
            title="Speed Range Distribution",
            color=speed_counts.values,
            color_continuous_scale='reds'
        )
        fig_hist.update_layout(xaxis_title="Speed Range (km/h)", yaxis_title="Vehicle Count")
        st.plotly_chart(fig_hist, use_container_width=True)
    
    with col2:
        st.subheader("🕐 Hourly Traffic Pattern")
        hourly_traffic = df.groupby('hour').size().reset_index(name='count')
        
        fig_line = px.line(
            hourly_traffic,
            x='hour',
            y='count',
            title="Traffic Volume by Hour",
            markers=True
        )
        fig_line.update_layout(xaxis_title="Hour of Day", yaxis_title="Vehicle Count")
        st.plotly_chart(fig_line, use_container_width=True)
    
    # Real-time metrics
    st.subheader("🔄 Real-time Metrics")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric(
            label="🚨 Speeding Violations",
            value=df['has_speeding_alert'].sum(),
            delta=f"{(df['has_speeding_alert'].sum() / len(df) * 100):.1f}% of total"
        )
    
    with col2:
        st.metric(
            label="⛽ Low Fuel Alerts",
            value=df['has_fuel_alert'].sum(),
            delta=f"{(df['has_fuel_alert'].sum() / len(df) * 100):.1f}% of total"
        )
    
    with col3:
        avg_passengers = df['passenger_count'].mean()
        st.metric(
            label="👥 Average Passengers",
            value=f"{avg_passengers:.1f}",
            delta=f"Max: {df['passenger_count'].max()}"
        )
    
    # Congestion heatmap
    st.subheader("🌡️ Congestion Heatmap by District")
    
    congestion_data = df.groupby(['district', 'congestion_level']).size().unstack(fill_value=0)
    if not congestion_data.empty:
        fig_heatmap = px.imshow(
            congestion_data.T,
            title="Congestion Levels by District",
            color_continuous_scale='RdYlGn_r',
            aspect='auto'
        )
        fig_heatmap.update_layout(
            xaxis_title="District",
            yaxis_title="Congestion Level"
        )
        st.plotly_chart(fig_heatmap, use_container_width=True)
    
    # Recent alerts table
    st.subheader("⚠️ Recent Alerts")
    
    # Extract alerts from the alerts column
    alerts_data = []
    for idx, row in df.iterrows():
        if isinstance(row['alerts'], list) and len(row['alerts']) > 0:
            for alert in row['alerts']:
                if isinstance(alert, dict):
                    alerts_data.append({
                        'Vehicle ID': row['vehicle_id'],
                        'Alert Type': alert.get('type', 'Unknown'),
                        'Description': alert.get('description', ''),
                        'Severity': alert.get('severity', 'Unknown'),
                        'District': row['district'],
                        'Speed': f"{row['speed_kmph']:.1f} km/h",
                        'Timestamp': alert.get('timestamp', row['timestamp'])
                    })
    
    if alerts_data:
        alerts_df = pd.DataFrame(alerts_data)
        st.dataframe(alerts_df.head(20), use_container_width=True)
    else:
        st.info("No alerts found in the current dataset")
    
    # Auto-refresh simulation
    if st.button("🔄 Refresh Dashboard"):
        st.rerun()