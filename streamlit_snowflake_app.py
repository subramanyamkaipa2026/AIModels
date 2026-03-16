"""
Streamlit App to Display Snowflake Table Data
Supports both test data mode and live Snowflake connection
"""

import streamlit as st
import pandas as pd
import snowflake.connector
from configparser import ConfigParser
import os
from datetime import datetime, timedelta
import random

# Page configuration
st.set_page_config(
    page_title="Snowflake Data Viewer",
    page_icon="❄️",
    layout="wide",
    initial_sidebar_state="expanded"
)

def generate_test_data(num_rows=100):
    """Generate test data matching FACT_ISG table structure"""
    
    # Sample data for realistic generation
    first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Lisa', 'James', 'Mary']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez', 'Martinez']
    products = ['Premium Plus', 'Gold Plan', 'Silver Plan', 'Bronze Plan', 'Platinum Elite']
    states = ['CA', 'TX', 'FL', 'NY', 'PA', 'IL', 'OH', 'GA', 'NC', 'MI']
    
    data = []
    base_date = datetime(2023, 8, 1)
    
    for i in range(num_rows):
        subscriber_id = f"SUB{1000000 + i}"
        mcid = f"MC{2000000 + i}"
        
        # Generate random dates
        effective_date = base_date + timedelta(days=random.randint(0, 30))
        termination_date = effective_date + timedelta(days=random.randint(30, 365))
        
        # Generate random premium amount
        premium = round(random.uniform(100, 1500), 2)
        
        row = {
            'DETOK_SBSCRBR_ID': subscriber_id,
            'DETOK_FRST_NM': random.choice(first_names),
            'DETOK_LAST_NM': random.choice(last_names),
            'MCID': mcid,
            'PROD_CF_NM': random.choice(products),
            'MDCL_MCID_EFCTV_DT': effective_date.strftime('%Y-%m-%d'),
            'MDCL_MCID_TRMNTN_DT': termination_date.strftime('%Y-%m-%d'),
            'MDCL_RVNU_PREM_AMT': premium,
            'ST_CD': random.choice(states)
        }
        data.append(row)
    
    return pd.DataFrame(data)

def load_snowflake_config(config_file='snowflake_config.ini'):
    """Load Snowflake configuration from file"""
    config = ConfigParser()
    
    if not os.path.exists(config_file):
        return None
    
    config.read(config_file)
    return config

def connect_to_snowflake(config):
    """Create connection to Snowflake"""
    try:
        conn = snowflake.connector.connect(
            user=config['SNOWFLAKE']['user'],
            password=config['SNOWFLAKE']['password'],
            account=config['SNOWFLAKE']['account'],
            warehouse=config['SNOWFLAKE']['warehouse'],
            database=config['SNOWFLAKE']['database'],
            schema=config['SNOWFLAKE']['schema'],
            role=config['SNOWFLAKE']['role']
        )
        return conn
    except Exception as e:
        st.error(f"Connection failed: {str(e)}")
        return None

def fetch_snowflake_data(conn, table_name, limit=1000):
    """Fetch data from Snowflake table"""
    try:
        query = f"SELECT * FROM {table_name} LIMIT {limit}"
        df = pd.read_sql(query, conn)
        return df
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return None

def display_data_summary(df):
    """Display summary statistics of the data"""
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric("Total Records", f"{len(df):,}")
    
    with col2:
        if 'MDCL_RVNU_PREM_AMT' in df.columns:
            avg_premium = df['MDCL_RVNU_PREM_AMT'].mean()
            st.metric("Avg Premium", f"${avg_premium:,.2f}")
        else:
            st.metric("Columns", len(df.columns))
    
    with col3:
        if 'ST_CD' in df.columns:
            unique_states = df['ST_CD'].nunique()
            st.metric("Unique States", unique_states)
        else:
            st.metric("Data Type", "Snowflake" if st.session_state.get('use_snowflake', False) else "Test Data")
    
    with col4:
        if 'PROD_CF_NM' in df.columns:
            unique_products = df['PROD_CF_NM'].nunique()
            st.metric("Unique Products", unique_products)
        else:
            st.metric("Memory Usage", f"{df.memory_usage(deep=True).sum() / 1024:.1f} KB")

def main():
    """Main Streamlit application"""
    
    # Title and description
    st.title("❄️ Snowflake Data Viewer")
    st.markdown("---")
    
    # Sidebar configuration
    st.sidebar.header("⚙️ Configuration")
    
    # Data source selection
    data_source = st.sidebar.radio(
        "Select Data Source:",
        ["Test Data", "Snowflake Connection"],
        help="Choose between test data or live Snowflake connection"
    )
    
    # Initialize session state
    if 'use_snowflake' not in st.session_state:
        st.session_state.use_snowflake = False
    
    df = None
    
    # Handle data source
    if data_source == "Test Data":
        st.session_state.use_snowflake = False
        
        # Test data options
        num_rows = st.sidebar.slider("Number of Test Rows:", 10, 1000, 100, 10)
        
        if st.sidebar.button("Generate Test Data", type="primary"):
            with st.spinner("Generating test data..."):
                df = generate_test_data(num_rows)
                st.session_state.df = df
                st.success(f"✅ Generated {len(df)} test records")
        
        # Load from session state if exists
        if 'df' in st.session_state:
            df = st.session_state.df
    
    else:  # Snowflake Connection
        st.session_state.use_snowflake = True
        
        # Load config
        config = load_snowflake_config()
        
        if config is None:
            st.error("❌ Snowflake config file not found!")
            st.info("Please ensure 'snowflake_config.ini' exists in the project directory.")
            return
        
        # Display connection info
        st.sidebar.info(f"""
        **Connection Details:**
        - Account: {config['SNOWFLAKE']['account']}
        - Database: {config['SNOWFLAKE']['database']}
        - Schema: {config['SNOWFLAKE']['schema']}
        - Table: {config['SNOWFLAKE']['table']}
        """)
        
        # Row limit for Snowflake query
        row_limit = st.sidebar.number_input("Row Limit:", min_value=10, max_value=10000, value=1000, step=100)
        
        if st.sidebar.button("Fetch Snowflake Data", type="primary"):
            with st.spinner("Connecting to Snowflake..."):
                conn = connect_to_snowflake(config)
                
                if conn:
                    st.success("✅ Connected to Snowflake")
                    
                    with st.spinner("Fetching data..."):
                        table_name = config['SNOWFLAKE']['table']
                        df = fetch_snowflake_data(conn, table_name, row_limit)
                        
                        if df is not None:
                            st.session_state.df = df
                            st.success(f"✅ Fetched {len(df)} records from {table_name}")
                        
                    conn.close()
        
        # Load from session state if exists
        if 'df' in st.session_state:
            df = st.session_state.df
    
    # Display data if available
    if df is not None and not df.empty:
        st.markdown("---")
        
        # Summary metrics
        st.subheader("📊 Data Summary")
        display_data_summary(df)
        
        st.markdown("---")
        
        # Data table with filters
        st.subheader("📋 Data Table")
        
        # Add column filter
        col1, col2 = st.columns([3, 1])
        with col1:
            if st.checkbox("Show Column Selector"):
                selected_columns = st.multiselect(
                    "Select columns to display:",
                    options=df.columns.tolist(),
                    default=df.columns.tolist()
                )
                df_display = df[selected_columns]
            else:
                df_display = df
        
        with col2:
            show_index = st.checkbox("Show Index", value=False)
        
        # Display dataframe
        st.dataframe(
            df_display,
            use_container_width=True,
            hide_index=not show_index,
            height=400
        )
        
        # Download option
        st.markdown("---")
        st.subheader("💾 Export Data")
        
        col1, col2 = st.columns(2)
        
        with col1:
            csv = df.to_csv(index=False).encode('utf-8')
            st.download_button(
                label="📥 Download as CSV",
                data=csv,
                file_name=f"snowflake_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
                mime="text/csv",
                use_container_width=True
            )
        
        with col2:
            # Convert to Excel
            from io import BytesIO
            buffer = BytesIO()
            with pd.ExcelWriter(buffer, engine='openpyxl') as writer:
                df.to_excel(writer, index=False, sheet_name='Data')
            
            st.download_button(
                label="📥 Download as Excel",
                data=buffer.getvalue(),
                file_name=f"snowflake_data_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                use_container_width=True
            )
        
        # Additional analytics
        if st.checkbox("Show Advanced Analytics"):
            st.markdown("---")
            st.subheader("📈 Analytics")
            
            tab1, tab2, tab3 = st.tabs(["Statistics", "Distribution", "Data Info"])
            
            with tab1:
                st.write("**Descriptive Statistics:**")
                st.dataframe(df.describe(), use_container_width=True)
            
            with tab2:
                if 'ST_CD' in df.columns:
                    st.write("**State Distribution:**")
                    state_counts = df['ST_CD'].value_counts()
                    st.bar_chart(state_counts)
                
                if 'PROD_CF_NM' in df.columns:
                    st.write("**Product Distribution:**")
                    product_counts = df['PROD_CF_NM'].value_counts()
                    st.bar_chart(product_counts)
            
            with tab3:
                st.write("**Data Types:**")
                info_df = pd.DataFrame({
                    'Column': df.columns,
                    'Type': df.dtypes.values,
                    'Non-Null Count': df.count().values,
                    'Null Count': df.isnull().sum().values
                })
                st.dataframe(info_df, use_container_width=True)
    
    else:
        st.info("👆 Please select a data source and click the button to load data.")
    
    # Footer
    st.markdown("---")
    st.markdown(
        """
        <div style='text-align: center; color: gray;'>
            <small>Snowflake Data Viewer | Built with Streamlit ❄️</small>
        </div>
        """,
        unsafe_allow_html=True
    )

if __name__ == "__main__":
    main()
