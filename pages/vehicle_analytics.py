import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots

def show(df):
    st.markdown('<div class="main-header">🚗 Vehicle Analytics</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Comprehensive Vehicle Performance Analysis</div>', unsafe_allow_html=True)
    
    # Vehicle Overview Metrics
    st.subheader("📊 Vehicle Fleet Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        total_vehicles = df['vehicle_id'].nunique()
        st.metric("🚗 Total Unique Vehicles", f"{total_vehicles:,}")
    
    with col2:
        avg_speed = df['speed_kmph'].mean()
        st.metric("⚡ Fleet Average Speed", f"{avg_speed:.1f} km/h")
    
    with col3:
        avg_fuel = df['fuel_level_percentage'].mean()
        st.metric("⛽ Average Fuel Level", f"{avg_fuel:.1f}%")
    
    with col4:
        avg_passengers = df['passenger_count'].mean()
        st.metric("👥 Average Passengers", f"{avg_passengers:.1f}")
    
    # Vehicle Type Analysis
    st.subheader("🚙 Vehicle Type Distribution & Performance")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Vehicle type pie chart
        vehicle_counts = df['vehicle_type'].value_counts()
        fig_pie = px.pie(
            values=vehicle_counts.values,
            names=vehicle_counts.index,
            title="Fleet Composition by Vehicle Type",
            color_discrete_sequence=px.colors.qualitative.Set3
        )
        fig_pie.update_traces(textposition='inside', textinfo='percent+label')
        st.plotly_chart(fig_pie, use_container_width=True)
    
    with col2:
        # Vehicle type performance metrics
        vehicle_stats = df.groupby('vehicle_type').agg({
            'speed_kmph': 'mean',
            'fuel_level_percentage': 'mean',
            'passenger_count': 'mean',
            'vehicle_id': 'count'
        }).round(2)
        vehicle_stats.columns = ['Avg Speed (km/h)', 'Avg Fuel (%)', 'Avg Passengers', 'Count']
        
        st.write("**Performance by Vehicle Type:**")
        st.dataframe(vehicle_stats, use_container_width=True)
    
    # Speed Analysis
    st.subheader("⚡ Speed Distribution Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Speed histogram
        fig_hist = px.histogram(
            df,
            x='speed_kmph',
            nbins=30,
            title="Speed Distribution Across Fleet",
            color_discrete_sequence=['#1f77b4']
        )
        fig_hist.update_layout(
            xaxis_title="Speed (km/h)",
            yaxis_title="Number of Vehicles"
        )
        st.plotly_chart(fig_hist, use_container_width=True)
    
    with col2:
        # Speed by vehicle type box plot
        fig_box = px.box(
            df,
            x='vehicle_type',
            y='speed_kmph',
            title="Speed Distribution by Vehicle Type",
            color='vehicle_type'
        )
        fig_box.update_layout(xaxis_title="Vehicle Type", yaxis_title="Speed (km/h)")
        st.plotly_chart(fig_box, use_container_width=True)
    
    # Speed ranges analysis
    speed_bins = pd.cut(df['speed_kmph'], bins=[0, 20, 40, 60, 80, 120], labels=['0-20', '20-40', '40-60', '60-80', '80+'])
    speed_vehicle_type = pd.crosstab(speed_bins, df['vehicle_type'])
    
    fig_stacked = px.bar(
        speed_vehicle_type,
        title="Speed Range Distribution by Vehicle Type",
        color_discrete_sequence=px.colors.qualitative.Pastel
    )
    fig_stacked.update_layout(
        xaxis_title="Speed Range (km/h)",
        yaxis_title="Number of Vehicles",
        legend_title="Vehicle Type"
    )
    st.plotly_chart(fig_stacked, use_container_width=True)
    
    # Passenger Analysis
    st.subheader("👥 Passenger Analytics")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Passenger count by vehicle type
        passenger_stats = df.groupby('vehicle_type')['passenger_count'].agg(['mean', 'max', 'min']).round(2)
        passenger_stats.columns = ['Average', 'Maximum', 'Minimum']
        
        fig_passenger = px.bar(
            passenger_stats,
            title="Passenger Statistics by Vehicle Type",
            barmode='group'
        )
        fig_passenger.update_layout(
            xaxis_title="Vehicle Type",
            yaxis_title="Passenger Count"
        )
        st.plotly_chart(fig_passenger, use_container_width=True)
    
    with col2:
        # Passenger vs Speed correlation
        fig_scatter = px.scatter(
            df.sample(min(2000, len(df))),  # Sample for performance
            x='passenger_count',
            y='speed_kmph',
            color='vehicle_type',
            size='fuel_level_percentage',
            title="Passenger Count vs Speed",
            hover_data=['district']
        )
        fig_scatter.update_layout(
            xaxis_title="Passenger Count",
            yaxis_title="Speed (km/h)"
        )
        st.plotly_chart(fig_scatter, use_container_width=True)
    
    # Fuel Analysis
    st.subheader("⛽ Fuel Level Analysis")
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Fuel level distribution
        fig_fuel_hist = px.histogram(
            df,
            x='fuel_level_percentage',
            nbins=20,
            title="Fuel Level Distribution",
            color_discrete_sequence=['#ff7f0e']
        )
        fig_fuel_hist.update_layout(
            xaxis_title="Fuel Level (%)",
            yaxis_title="Number of Vehicles"
        )
        st.plotly_chart(fig_fuel_hist, use_container_width=True)
    
    with col2:
        # Low fuel alerts by vehicle type
        low_fuel_df = df[df['fuel_level_percentage'] < 20]
        if not low_fuel_df.empty:
            low_fuel_counts = low_fuel_df['vehicle_type'].value_counts()
            fig_low_fuel = px.bar(
                x=low_fuel_counts.index,
                y=low_fuel_counts.values,
                title="Low Fuel Alerts by Vehicle Type",
                color=low_fuel_counts.values,
                color_continuous_scale='reds'
            )
            fig_low_fuel.update_layout(
                xaxis_title="Vehicle Type",
                yaxis_title="Number of Low Fuel Vehicles"
            )
            st.plotly_chart(fig_low_fuel, use_container_width=True)
        else:
            st.info("No vehicles with low fuel levels found")
    
    # Performance Correlation Matrix
    st.subheader("🔗 Performance Correlation Analysis")
    
    # Select numerical columns for correlation
    numerical_cols = ['speed_kmph', 'fuel_level_percentage', 'passenger_count', 'temperature', 'humidity']
    correlation_matrix = df[numerical_cols].corr()
    
    fig_corr = px.imshow(
        correlation_matrix,
        title="Vehicle Performance Correlation Matrix",
        color_continuous_scale='RdBu',
        aspect='auto'
    )
    fig_corr.update_layout(
        xaxis_title="Metrics",
        yaxis_title="Metrics"
    )
    st.plotly_chart(fig_corr, use_container_width=True)
    
    # Top Performers Analysis
    st.subheader("🏆 Top Performers")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.write("**🚀 Fastest Vehicles**")
        fastest_vehicles = df.nlargest(10, 'speed_kmph')[['vehicle_id', 'vehicle_type', 'speed_kmph', 'district']]
        st.dataframe(fastest_vehicles, use_container_width=True)
    
    with col2:
        st.write("**⛽ Most Fuel Efficient**")
        # Assuming higher fuel level indicates better efficiency
        efficient_vehicles = df.nlargest(10, 'fuel_level_percentage')[['vehicle_id', 'vehicle_type', 'fuel_level_percentage', 'district']]
        st.dataframe(efficient_vehicles, use_container_width=True)
    
    with col3:
        st.write("**👥 Highest Capacity Utilization**")
        high_capacity = df.nlargest(10, 'passenger_count')[['vehicle_id', 'vehicle_type', 'passenger_count', 'district']]
        st.dataframe(high_capacity, use_container_width=True)
    
    # Vehicle Performance Trends
    st.subheader("📈 Performance Trends")
    
    # Group by hour to show trends
    hourly_performance = df.groupby('hour').agg({
        'speed_kmph': 'mean',
        'fuel_level_percentage': 'mean',
        'passenger_count': 'mean'
    }).reset_index()
    
    # Create subplot with secondary y-axis
    fig_trends = make_subplots(
        rows=1, cols=1,
        specs=[[{"secondary_y": True}]]
    )
    
    # Add speed trend
    fig_trends.add_trace(
        go.Scatter(
            x=hourly_performance['hour'],
            y=hourly_performance['speed_kmph'],
            name="Average Speed",
            line=dict(color='blue')
        ),
        secondary_y=False
    )
    
    # Add fuel trend
    fig_trends.add_trace(
        go.Scatter(
            x=hourly_performance['hour'],
            y=hourly_performance['fuel_level_percentage'],
            name="Average Fuel Level",
            line=dict(color='orange')
        ),
        secondary_y=True
    )
    
    # Add passenger trend
    fig_trends.add_trace(
        go.Scatter(
            x=hourly_performance['hour'],
            y=hourly_performance['passenger_count'],
            name="Average Passengers",
            line=dict(color='green')
        ),
        secondary_y=False
    )
    
    fig_trends.update_layout(title="Hourly Performance Trends")
    fig_trends.update_xaxes(title_text="Hour of Day")
    fig_trends.update_yaxes(title_text="Speed (km/h) / Passengers", secondary_y=False)
    fig_trends.update_yaxes(title_text="Fuel Level (%)", secondary_y=True)
    
    st.plotly_chart(fig_trends, use_container_width=True)
    
    # Vehicle Efficiency Score
    st.subheader("🎯 Vehicle Efficiency Scoring")
    
    # Calculate efficiency score (normalized combination of speed, fuel, and capacity utilization)
    df_score = df.copy()
    df_score['speed_score'] = (df_score['speed_kmph'] - df_score['speed_kmph'].min()) / (df_score['speed_kmph'].max() - df_score['speed_kmph'].min())
    df_score['fuel_score'] = df_score['fuel_level_percentage'] / 100
    df_score['passenger_score'] = (df_score['passenger_count'] - df_score['passenger_count'].min()) / (df_score['passenger_count'].max() - df_score['passenger_count'].min())
    
    # Combined efficiency score (weighted average)
    df_score['efficiency_score'] = (
        0.4 * df_score['speed_score'] + 
        0.3 * df_score['fuel_score'] + 
        0.3 * df_score['passenger_score']
    ) * 100
    
    # Top efficient vehicles
    top_efficient = df_score.nlargest(20, 'efficiency_score')[
        ['vehicle_id', 'vehicle_type', 'speed_kmph', 'fuel_level_percentage', 'passenger_count', 'efficiency_score', 'district']
    ].round(2)
    
    st.write("**🏆 Top 20 Most Efficient Vehicles (Combined Score)**")
    st.dataframe(top_efficient, use_container_width=True)
    
    # Efficiency score distribution
    fig_efficiency = px.histogram(
        df_score,
        x='efficiency_score',
        nbins=30,
        title="Vehicle Efficiency Score Distribution",
        color_discrete_sequence=['#2ca02c']
    )
    fig_efficiency.update_layout(
        xaxis_title="Efficiency Score",
        yaxis_title="Number of Vehicles"
    )
    st.plotly_chart(fig_efficiency, use_container_width=True)