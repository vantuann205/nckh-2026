import streamlit as st
import pandas as pd
import plotly.express as px

def show(df):
    st.markdown('<div class="main-header">🔍 Query Engine</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Advanced Data Querying & Custom Analysis</div>', unsafe_allow_html=True)
    
    # Query Builder Interface
    st.subheader("🛠️ Query Builder")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.write("**Select Columns:**")
        available_columns = [
            'vehicle_id', 'vehicle_type', 'speed_kmph', 'fuel_level_percentage',
            'passenger_count', 'district', 'street', 'congestion_level',
            'weather', 'temperature', 'humidity', 'timestamp'
        ]
        selected_columns = st.multiselect(
            "Choose columns to display:",
            options=available_columns,
            default=['vehicle_id', 'vehicle_type', 'speed_kmph', 'district']
        )
    
    with col2:
        st.write("**Filters:**")
        
        # Vehicle type filter
        vehicle_types = st.multiselect(
            "Vehicle Types:",
            options=df['vehicle_type'].unique(),
            default=list(df['vehicle_type'].unique())
        )
        
        # Speed filter
        speed_range = st.slider(
            "Speed Range (km/h):",
            min_value=0,
            max_value=int(df['speed_kmph'].max()),
            value=(0, int(df['speed_kmph'].max()))
        )
    
    # Additional filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        districts = st.multiselect(
            "Districts:",
            options=df['district'].unique(),
            default=list(df['district'].unique())[:5]
        )
    
    with col2:
        fuel_range = st.slider(
            "Fuel Level (%):",
            min_value=0,
            max_value=100,
            value=(0, 100)
        )
    
    with col3:
        weather_conditions = st.multiselect(
            "Weather Conditions:",
            options=df['weather'].unique(),
            default=list(df['weather'].unique())
        )
    
    # Apply filters
    filtered_df = df[
        (df['vehicle_type'].isin(vehicle_types)) &
        (df['speed_kmph'] >= speed_range[0]) &
        (df['speed_kmph'] <= speed_range[1]) &
        (df['district'].isin(districts)) &
        (df['fuel_level_percentage'] >= fuel_range[0]) &
        (df['fuel_level_percentage'] <= fuel_range[1]) &
        (df['weather'].isin(weather_conditions))
    ]
    
    st.info(f"📊 Query Result: {len(filtered_df):,} records")
    
    # Display results
    if selected_columns and not filtered_df.empty:
        result_df = filtered_df[selected_columns].head(1000)
        st.dataframe(result_df, use_container_width=True)
        
        # Export options
        csv = result_df.to_csv(index=False)
        st.download_button(
            label="📥 Download Results",
            data=csv,
            file_name="query_results.csv",
            mime="text/csv"
        )
    
    # Quick Analytics
    if not filtered_df.empty:
        st.subheader("📈 Quick Analytics")
        
        col1, col2 = st.columns(2)
        
        with col1:
            # Summary statistics
            numeric_cols = ['speed_kmph', 'fuel_level_percentage', 'passenger_count']
            summary_stats = filtered_df[numeric_cols].describe()
            st.write("**Summary Statistics:**")
            st.dataframe(summary_stats)
        
        with col2:
            # Top districts
            top_districts = filtered_df['district'].value_counts().head(10)
            fig_districts = px.bar(
                x=top_districts.values,
                y=top_districts.index,
                orientation='h',
                title="Top Districts in Query Results"
            )
            st.plotly_chart(fig_districts, use_container_width=True)