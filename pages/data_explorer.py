import streamlit as st
import pandas as pd
import plotly.express as px

def show(df):
    st.markdown('<div class="main-header">📈 Data Explorer</div>', unsafe_allow_html=True)
    st.markdown('<div class="sub-header">Raw Data Analysis & SQL-like Queries</div>', unsafe_allow_html=True)
    
    # Data overview
    st.subheader("📊 Dataset Overview")
    
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Records", f"{len(df):,}")
    
    with col2:
        st.metric("Columns", len(df.columns))
    
    with col3:
        memory_usage = df.memory_usage(deep=True).sum() / 1024**2
        st.metric("Memory Usage", f"{memory_usage:.1f} MB")
    
    with col4:
        date_range = (df['timestamp'].max() - df['timestamp'].min()).days
        st.metric("Date Range", f"{date_range} days")
    
    # Advanced Filters
    st.subheader("🔧 Advanced Filters")
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Vehicle type filter
        vehicle_types = st.multiselect(
            "Vehicle Types",
            options=df['vehicle_type'].unique(),
            default=df['vehicle_type'].unique()
        )
    
    with col2:
        # District filter
        districts = st.multiselect(
            "Districts",
            options=df['district'].unique(),
            default=list(df['district'].unique())[:5]  # Limit default selection
        )
    
    with col3:
        # Speed range
        speed_min, speed_max = st.slider(
            "Speed Range (km/h)",
            min_value=0,
            max_value=int(df['speed_kmph'].max()),
            value=(0, int(df['speed_kmph'].max())),
            step=5
        )
    
    # Additional filters
    col1, col2, col3 = st.columns(3)
    
    with col1:
        # Fuel level filter
        fuel_min, fuel_max = st.slider(
            "Fuel Level (%)",
            min_value=0,
            max_value=100,
            value=(0, 100),
            step=5
        )
    
    with col2:
        # Passenger count filter
        passenger_min, passenger_max = st.slider(
            "Passenger Count",
            min_value=int(df['passenger_count'].min()),
            max_value=int(df['passenger_count'].max()),
            value=(int(df['passenger_count'].min()), int(df['passenger_count'].max()))
        )
    
    with col3:
        # Congestion level filter
        congestion_levels = st.multiselect(
            "Congestion Levels",
            options=df['congestion_level'].unique(),
            default=df['congestion_level'].unique()
        )
    
    # Apply filters
    filtered_df = df[
        (df['vehicle_type'].isin(vehicle_types)) &
        (df['district'].isin(districts)) &
        (df['speed_kmph'] >= speed_min) &
        (df['speed_kmph'] <= speed_max) &
        (df['fuel_level_percentage'] >= fuel_min) &
        (df['fuel_level_percentage'] <= fuel_max) &
        (df['passenger_count'] >= passenger_min) &
        (df['passenger_count'] <= passenger_max) &
        (df['congestion_level'].isin(congestion_levels))
    ]
    
    st.info(f"📊 Filtered dataset: {len(filtered_df):,} records ({len(filtered_df)/len(df)*100:.1f}% of total)")
    
    # SQL-like Query Interface
    st.subheader("🔍 SQL-like Query Interface")
    
    query_examples = [
        "SELECT * FROM data WHERE speed_kmph > 80",
        "SELECT district, COUNT(*) as vehicle_count FROM data GROUP BY district ORDER BY vehicle_count DESC",
        "SELECT vehicle_type, AVG(speed_kmph) as avg_speed FROM data GROUP BY vehicle_type",
        "SELECT * FROM data WHERE fuel_level_percentage < 20 AND has_fuel_alert = True"
    ]
    
    selected_example = st.selectbox("Query Examples", ["Custom Query"] + query_examples)
    
    if selected_example != "Custom Query":
        query = selected_example
    else:
        query = st.text_area(
            "Enter your query (use 'data' as table name):",
            height=100,
            placeholder="SELECT * FROM data WHERE speed_kmph > 60 LIMIT 100"
        )
    
    if st.button("🚀 Execute Query"):
        try:
            # Simple query parser (basic implementation)
            if query.strip():
                # For demo purposes, we'll implement basic SELECT queries
                query_lower = query.lower().strip()
                
                if query_lower.startswith('select'):
                    # Parse basic SELECT queries
                    result_df = execute_simple_query(filtered_df, query)
                    
                    if result_df is not None and not result_df.empty:
                        st.success(f"✅ Query executed successfully! {len(result_df)} rows returned.")
                        st.dataframe(result_df, use_container_width=True)
                        
                        # Download option
                        csv = result_df.to_csv(index=False)
                        st.download_button(
                            label="📥 Download Results as CSV",
                            data=csv,
                            file_name="query_results.csv",
                            mime="text/csv"
                        )
                    else:
                        st.warning("Query returned no results")
                else:
                    st.error("Only SELECT queries are supported in this demo")
        except Exception as e:
            st.error(f"Query error: {str(e)}")
    
    # Data Table with Search and Sort
    st.subheader("📋 Data Table")
    
    # Search functionality
    search_term = st.text_input("🔍 Search in data:", placeholder="Enter search term...")
    
    if search_term:
        # Search across string columns
        string_columns = ['vehicle_id', 'owner_name', 'license_number', 'street', 'district', 'city', 'vehicle_type', 'weather']
        mask = pd.Series([False] * len(filtered_df))
        
        for col in string_columns:
            if col in filtered_df.columns:
                mask |= filtered_df[col].astype(str).str.contains(search_term, case=False, na=False)
        
        search_df = filtered_df[mask]
        st.info(f"🔍 Found {len(search_df)} records matching '{search_term}'")
    else:
        search_df = filtered_df
    
    # Column selection
    available_columns = [
        'vehicle_id', 'owner_name', 'license_number', 'speed_kmph', 
        'street', 'district', 'city', 'timestamp', 'vehicle_type',
        'fuel_level_percentage', 'passenger_count', 'weather',
        'congestion_level', 'has_speeding_alert', 'has_fuel_alert'
    ]
    
    selected_columns = st.multiselect(
        "Select columns to display:",
        options=available_columns,
        default=['vehicle_id', 'speed_kmph', 'street', 'district', 'vehicle_type', 'timestamp']
    )
    
    if selected_columns:
        display_df = search_df[selected_columns].head(1000)  # Limit for performance
        
        # Sort options
        sort_column = st.selectbox("Sort by:", options=selected_columns)
        sort_order = st.radio("Sort order:", ["Ascending", "Descending"])
        
        if sort_column:
            ascending = sort_order == "Ascending"
            display_df = display_df.sort_values(sort_column, ascending=ascending)
        
        st.dataframe(display_df, use_container_width=True)
        
        # Export options
        col1, col2 = st.columns(2)
        
        with col1:
            csv = display_df.to_csv(index=False)
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name="traffic_data_export.csv",
                mime="text/csv"
            )
        
        with col2:
            json_data = display_df.to_json(orient='records', indent=2)
            st.download_button(
                label="📥 Download as JSON",
                data=json_data,
                file_name="traffic_data_export.json",
                mime="application/json"
            )
    
    # Data Statistics
    st.subheader("📈 Data Statistics")
    
    if not filtered_df.empty:
        # Numerical columns statistics
        numerical_cols = ['speed_kmph', 'fuel_level_percentage', 'passenger_count', 'temperature', 'humidity']
        numerical_stats = filtered_df[numerical_cols].describe()
        
        st.write("**Numerical Columns Statistics:**")
        st.dataframe(numerical_stats, use_container_width=True)
        
        # Categorical columns statistics
        categorical_cols = ['vehicle_type', 'district', 'congestion_level', 'weather']
        
        col1, col2 = st.columns(2)
        
        with col1:
            st.write("**Vehicle Type Distribution:**")
            vehicle_dist = filtered_df['vehicle_type'].value_counts()
            st.dataframe(vehicle_dist, use_container_width=True)
        
        with col2:
            st.write("**District Distribution:**")
            district_dist = filtered_df['district'].value_counts().head(10)
            st.dataframe(district_dist, use_container_width=True)

def execute_simple_query(df, query):
    """Simple query executor for basic SELECT statements"""
    try:
        query = query.strip()
        
        # Very basic query parsing - for demo purposes only
        if query.lower().startswith('select * from data'):
            # Handle WHERE clause
            if 'where' in query.lower():
                where_part = query.lower().split('where')[1].strip()
                
                # Simple condition parsing
                if 'speed_kmph >' in where_part:
                    value = float(where_part.split('speed_kmph >')[1].strip())
                    return df[df['speed_kmph'] > value]
                elif 'fuel_level_percentage <' in where_part:
                    value = float(where_part.split('fuel_level_percentage <')[1].strip())
                    return df[df['fuel_level_percentage'] < value]
            
            # Handle LIMIT
            if 'limit' in query.lower():
                limit = int(query.lower().split('limit')[1].strip())
                return df.head(limit)
            
            return df
        
        elif 'group by district' in query.lower():
            return df.groupby('district').size().reset_index(name='vehicle_count').sort_values('vehicle_count', ascending=False)
        
        elif 'group by vehicle_type' in query.lower():
            return df.groupby('vehicle_type').agg({'speed_kmph': 'mean'}).reset_index()
        
        else:
            return df.head(100)  # Default fallback
            
    except Exception as e:
        raise Exception(f"Query parsing error: {str(e)}")