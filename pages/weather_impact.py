import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go

def show(df):
    st.markdown('<div class="main-header">🌤️ Weather Impact Analysis</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Weather Conditions & Traffic Correlation</div>', unsafe_allow_html=True)
    
    # Weather Overview
    st.subheader("🌡️ Weather Conditions Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        avg_temp = df['temperature'].mean()
        st.metric("🌡️ Average Temperature", f"{avg_temp:.1f}°C")
    
    with col2:
        avg_humidity = df['humidity'].mean()
        st.metric("💧 Average Humidity", f"{avg_humidity:.1f}%")
    
    with col3:
        weather_conditions = df['weather'].nunique()
        st.metric("🌤️ Weather Conditions", weather_conditions)
    
    with col4:
        most_common_weather = df['weather'].mode().iloc[0] if not df['weather'].empty else "Unknown"
        st.metric("☀️ Most Common", most_common_weather)
    
    # Weather Distribution
    col1, col2 = st.columns(2)
    
    with col1:
        # Weather condition distribution
        weather_counts = df['weather'].value_counts()
        fig_weather = px.pie(
            values=weather_counts.values,
            names=weather_counts.index,
            title="Weather Condition Distribution"
        )
        st.plotly_chart(fig_weather, use_container_width=True)
    
    with col2:
        # Temperature distribution
        fig_temp = px.histogram(
            df,
            x='temperature',
            nbins=20,
            title="Temperature Distribution",
            color_discrete_sequence=['#ff6b6b']
        )
        st.plotly_chart(fig_temp, use_container_width=True)
    
    # Weather Impact on Traffic
    st.subheader("🚦 Weather Impact on Traffic Performance")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Speed by weather condition
        weather_speed = df.groupby('weather')['speed_kmph'].mean().sort_values(ascending=False)
        fig_weather_speed = px.bar(
            x=weather_speed.index,
            y=weather_speed.values,
            title="Average Speed by Weather Condition",
            color=weather_speed.values,
            color_continuous_scale='viridis'
        )
        st.plotly_chart(fig_weather_speed, use_container_width=True)
    
    with col2:
        # Congestion by weather
        weather_congestion = pd.crosstab(df['weather'], df['congestion_level'])
        fig_congestion = px.bar(
            weather_congestion,
            title="Congestion Levels by Weather Condition",
            barmode='stack'
        )
        st.plotly_chart(fig_congestion, use_container_width=True)