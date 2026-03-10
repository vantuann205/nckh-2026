import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def show(df):
    st.markdown('<div class="main-header">⚠️ Alerts & Violations</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Traffic Violations & Safety Monitoring</div>', unsafe_allow_html=True)
    
    # Alert Overview Metrics
    st.subheader("🚨 Alert Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_alerts = df['alert_count'].sum()
        st.metric("🚨 Total Alerts", f"{total_alerts:,}")
    
    with col2:
        speeding_alerts = df['has_speeding_alert'].sum()
        st.metric("🏃 Speeding Violations", f"{speeding_alerts:,}")
    
    with col3:
        fuel_alerts = df['has_fuel_alert'].sum()
        st.metric("⛽ Low Fuel Alerts", f"{fuel_alerts:,}")
    
    with col4:
        high_congestion = len(df[df['congestion_level'] == 'High'])
        st.metric("🔴 High Congestion Areas", f"{high_congestion:,}")
    
    # Alert Severity Analysis
    st.subheader("📊 Alert Analysis")
    
    # Extract all alerts from the dataset
    alerts_data = []
    for idx, row in df.iterrows():
        if isinstance(row['alerts'], list) and len(row['alerts']) > 0:
            for alert in row['alerts']:
                if isinstance(alert, dict):
                    alerts_data.append({
                        'vehicle_id': row['vehicle_id'],
                        'vehicle_type': row['vehicle_type'],
                        'alert_type': alert.get('type', 'Unknown'),
                        'description': alert.get('description', ''),
                        'severity': alert.get('severity', 'Unknown'),
                        'district': row['district'],
                        'speed_kmph': row['speed_kmph'],
                        'fuel_level': row['fuel_level_percentage'],
                        'congestion_level': row['congestion_level'],
                        'timestamp': alert.get('timestamp', row['timestamp'])
                    })
    
    if alerts_data:
        alerts_df = pd.DataFrame(alerts_data)
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Alert type distribution
            alert_type_counts = alerts_df['alert_type'].value_counts()
            fig_alert_types = px.pie(
                values=alert_type_counts.values,
                names=alert_type_counts.index,
                title="Alert Types Distribution",
                color_discrete_sequence=px.colors.qualitative.Set1
            )
            st.plotly_chart(fig_alert_types, use_container_width=True)
        
        with col2:
            # Alert severity distribution
            severity_counts = alerts_df['severity'].value_counts()
            fig_severity = px.bar(
                x=severity_counts.index,
                y=severity_counts.values,
                title="Alert Severity Distribution",
                color=severity_counts.values,
                color_continuous_scale='reds'
            )
            fig_severity.update_layout(
                xaxis_title="Severity Level",
                yaxis_title="Number of Alerts"
            )
            st.plotly_chart(fig_severity, use_container_width=True)
        
        # Alerts by district
        district_alerts = alerts_df['district'].value_counts().head(10)
        fig_district_alerts = px.bar(
            x=district_alerts.values,
            y=district_alerts.index,
            orientation='h',
            title="Top 10 Districts by Alert Count",
            color=district_alerts.values,
            color_continuous_scale='oranges'
        )
        fig_district_alerts.update_layout(yaxis={'categoryorder':'total ascending'})
        st.plotly_chart(fig_district_alerts, use_container_width=True)
        
        # Recent alerts table
        st.subheader("📋 Recent Alerts")
        
        # Sort by timestamp if available
        if 'timestamp' in alerts_df.columns:
            alerts_df['timestamp'] = pd.to_datetime(alerts_df['timestamp'], errors='coerce')
            recent_alerts = alerts_df.sort_values('timestamp', ascending=False).head(50)
        else:
            recent_alerts = alerts_df.head(50)
        
        st.dataframe(recent_alerts, use_container_width=True)
        
    else:
        st.info("No detailed alert data found in the current dataset")
    
    # Speeding Analysis
    st.subheader("🏃 Speeding Violation Analysis")
    
    # Define speed limits by vehicle type (example values)
    speed_limits = {
        'Motorbike': 60,
        'Car': 80,
        'Bus': 70,
        'Truck': 65
    }
    
    # Calculate speeding violations based on assumed speed limits
    df_speeding = df.copy()
    df_speeding['speed_limit'] = df_speeding['vehicle_type'].map(speed_limits).fillna(60)
    df_speeding['is_speeding'] = df_speeding['speed_kmph'] > df_speeding['speed_limit']
    df_speeding['speed_excess'] = df_speeding['speed_kmph'] - df_speeding['speed_limit']
    df_speeding['speed_excess'] = df_speeding['speed_excess'].clip(lower=0)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Speeding by vehicle type
        speeding_by_type = df_speeding.groupby('vehicle_type').agg({
            'is_speeding': 'sum',
            'vehicle_id': 'count'
        })
        speeding_by_type['speeding_rate'] = (speeding_by_type['is_speeding'] / speeding_by_type['vehicle_id'] * 100).round(2)
        speeding_by_type.columns = ['Speeding Count', 'Total Vehicles', 'Speeding Rate (%)']
        
        fig_speeding_rate = px.bar(
            x=speeding_by_type.index,
            y=speeding_by_type['Speeding Rate (%)'],
            title="Speeding Rate by Vehicle Type",
            color=speeding_by_type['Speeding Rate (%)'],
            color_continuous_scale='reds'
        )
        fig_speeding_rate.update_layout(
            xaxis_title="Vehicle Type",
            yaxis_title="Speeding Rate (%)"
        )
        st.plotly_chart(fig_speeding_rate, use_container_width=True)
    
    with col2:
        # Speed excess distribution
        speeding_vehicles = df_speeding[df_speeding['is_speeding']]
        if not speeding_vehicles.empty:
            fig_excess = px.histogram(
                speeding_vehicles,
                x='speed_excess',
                nbins=20,
                title="Speed Excess Distribution",
                color_discrete_sequence=['#d62728']
            )
            fig_excess.update_layout(
                xaxis_title="Speed Excess (km/h)",
                yaxis_title="Number of Violations"
            )
            st.plotly_chart(fig_excess, use_container_width=True)
        else:
            st.info("No speeding violations detected")
    
    # Top speeding offenders
    if not speeding_vehicles.empty:
        st.subheader("🏆 Top Speeding Offenders")
        top_speeders = speeding_vehicles.nlargest(20, 'speed_excess')[
            ['vehicle_id', 'vehicle_type', 'speed_kmph', 'speed_limit', 'speed_excess', 'district']
        ].round(2)
        st.dataframe(top_speeders, use_container_width=True)
    
    # Fuel Level Violations
    st.subheader("⛽ Fuel Level Monitoring")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Low fuel by vehicle type
        low_fuel_by_type = df.groupby('vehicle_type').agg({
            'has_fuel_alert': 'sum',
            'vehicle_id': 'count'
        })
        low_fuel_by_type['low_fuel_rate'] = (low_fuel_by_type['has_fuel_alert'] / low_fuel_by_type['vehicle_id'] * 100).round(2)
        low_fuel_by_type.columns = ['Low Fuel Count', 'Total Vehicles', 'Low Fuel Rate (%)']
        
        fig_fuel_rate = px.bar(
            x=low_fuel_by_type.index,
            y=low_fuel_by_type['Low Fuel Rate (%)'],
            title="Low Fuel Rate by Vehicle Type",
            color=low_fuel_by_type['Low Fuel Rate (%)'],
            color_continuous_scale='oranges'
        )
        st.plotly_chart(fig_fuel_rate, use_container_width=True)
    
    with col2:
        # Fuel level distribution
        fig_fuel_dist = px.histogram(
            df,
            x='fuel_level_percentage',
            nbins=20,
            title="Fleet Fuel Level Distribution",
            color_discrete_sequence=['#ff7f0e']
        )
        fig_fuel_dist.add_vline(x=20, line_dash="dash", line_color="red", 
                               annotation_text="Low Fuel Threshold")
        fig_fuel_dist.update_layout(
            xaxis_title="Fuel Level (%)",
            yaxis_title="Number of Vehicles"
        )
        st.plotly_chart(fig_fuel_dist, use_container_width=True)
    
    # Critical fuel alerts
    critical_fuel = df[df['fuel_level_percentage'] < 10]
    if not critical_fuel.empty:
        st.subheader("🚨 Critical Fuel Alerts (< 10%)")
        critical_fuel_display = critical_fuel[
            ['vehicle_id', 'vehicle_type', 'fuel_level_percentage', 'district', 'speed_kmph']
        ].sort_values('fuel_level_percentage')
        st.dataframe(critical_fuel_display, use_container_width=True)
    
    # Congestion Analysis
    st.subheader("🚦 Traffic Congestion Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Congestion by district
        congestion_by_district = df.groupby(['district', 'congestion_level']).size().unstack(fill_value=0)
        if not congestion_by_district.empty:
            fig_congestion = px.bar(
                congestion_by_district.head(10),
                title="Congestion Levels by District (Top 10)",
                color_discrete_sequence=px.colors.qualitative.Set2
            )
            fig_congestion.update_layout(
                xaxis_title="District",
                yaxis_title="Number of Vehicles",
                legend_title="Congestion Level"
            )
            st.plotly_chart(fig_congestion, use_container_width=True)
    
    with col2:
        # Speed vs Congestion
        avg_speed_by_congestion = df.groupby('congestion_level')['speed_kmph'].mean().sort_values(ascending=False)
        fig_speed_congestion = px.bar(
            x=avg_speed_by_congestion.index,
            y=avg_speed_by_congestion.values,
            title="Average Speed by Congestion Level",
            color=avg_speed_by_congestion.values,
            color_continuous_scale='viridis'
        )
        fig_speed_congestion.update_layout(
            xaxis_title="Congestion Level",
            yaxis_title="Average Speed (km/h)"
        )
        st.plotly_chart(fig_speed_congestion, use_container_width=True)
    
    # Risk Score Calculation
    st.subheader("⚠️ Vehicle Risk Assessment")
    
    # Calculate risk score based on multiple factors
    df_risk = df.copy()
    
    # Risk factors (normalized to 0-1 scale)
    df_risk['speed_risk'] = (df_risk['speed_kmph'] > 80).astype(int)  # High speed risk
    df_risk['fuel_risk'] = (df_risk['fuel_level_percentage'] < 20).astype(int)  # Low fuel risk
    df_risk['congestion_risk'] = (df_risk['congestion_level'] == 'High').astype(int)  # High congestion risk
    df_risk['alert_risk'] = (df_risk['alert_count'] > 0).astype(int)  # Has alerts
    
    # Combined risk score (0-4 scale)
    df_risk['risk_score'] = (
        df_risk['speed_risk'] + 
        df_risk['fuel_risk'] + 
        df_risk['congestion_risk'] + 
        df_risk['alert_risk']
    )
    
    # Risk categories
    df_risk['risk_category'] = pd.cut(
        df_risk['risk_score'],
        bins=[-1, 0, 1, 2, 4],
        labels=['Low', 'Medium', 'High', 'Critical']
    )
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Risk distribution
        risk_counts = df_risk['risk_category'].value_counts()
        fig_risk = px.pie(
            values=risk_counts.values,
            names=risk_counts.index,
            title="Vehicle Risk Distribution",
            color_discrete_sequence=['green', 'yellow', 'orange', 'red']
        )
        st.plotly_chart(fig_risk, use_container_width=True)
    
    with col2:
        # High-risk vehicles by type
        high_risk = df_risk[df_risk['risk_category'].isin(['High', 'Critical'])]
        if not high_risk.empty:
            risk_by_type = high_risk['vehicle_type'].value_counts()
            fig_risk_type = px.bar(
                x=risk_by_type.index,
                y=risk_by_type.values,
                title="High-Risk Vehicles by Type",
                color=risk_by_type.values,
                color_continuous_scale='reds'
            )
            st.plotly_chart(fig_risk_type, use_container_width=True)
        else:
            st.info("No high-risk vehicles identified")
    
    # Critical vehicles requiring immediate attention
    critical_vehicles = df_risk[df_risk['risk_category'] == 'Critical']
    if not critical_vehicles.empty:
        st.subheader("🚨 Critical Vehicles Requiring Immediate Attention")
        critical_display = critical_vehicles[
            ['vehicle_id', 'vehicle_type', 'speed_kmph', 'fuel_level_percentage', 
             'congestion_level', 'alert_count', 'district', 'risk_score']
        ].sort_values('risk_score', ascending=False)
        st.dataframe(critical_display, use_container_width=True)
    else:
        st.success("✅ No critical risk vehicles identified")