import streamlit as st
import requests
import pandas as pd
import plotly.express as px
from datetime import datetime
import os
from dotenv import load_dotenv

load_dotenv()

# API endpoints
API_BASE_URL = "https://dynaledger-fast-api-930030449616.us-east1.run.app"

def check_data_availability(source, year, quarter):
    """Check if data is available in Snowflake"""
    try:
        response = requests.get(
            f"{API_BASE_URL}/check-availability",
            params={"source": source, "year": year, "quarter": quarter}
        )
        return response.json().get("available", False)
    except Exception as e:
        st.error(f"Error checking data availability: {str(e)}")
        return False

def fetch_financial_data(year, quarter, data_type, source):
    """Fetch financial data based on the selected parameters"""
    try:
        response = requests.get(
            f"{API_BASE_URL}/get-financial-data",
            params={"year": year, "quarter": quarter, "data_type": data_type, "source": source}
        )
        if response.status_code == 200:
            return response.json()
        else:
            st.error(f"Failed to fetch data: {response.text}")
            return None
    except Exception as e:
        st.error(f"Error fetching data: {str(e)}")
        return None
    
def execute_custom_query(query, data_source):
    """Execute custom query against Snowflake"""
    try:
        response = requests.post(
            f"{API_BASE_URL}/execute-custom-query",
            json={"query": query},
            params={"data_source": data_source}
        )
        if response.status_code == 200:
            return pd.DataFrame(response.json()["data"])
        else:
            st.error(f"Query failed: {response.text}")
            return None
    except Exception as e:
        st.error(f"Error executing query: {str(e)}")
        return None

def display_table_schemas(data_source):
    # Define the schema for the raw tables
    raw_table_schema = {
        "sec_num": [
            {"name": "ADSH", "type": "Varchar"},
            {"name": "TAG", "type": "Varchar"},
            {"name": "VERSION", "type": "Varchar"},
            {"name": "DDATE", "type": "Date"},
            {"name": "QTRS", "type": "Integer"},
            {"name": "UOM", "type": "Varchar"},
            {"name": "SEGMENTS", "type": "Varchar"},
            {"name": "COREG", "type": "Varchar"},
            {"name": "VALUE", "type": "Float"},
            {"name": "FOOTNOTE", "type": "Varchar"},
            {"name": "SOURCE_FILE", "type": "Varchar"}
        ],
        "sec_sub": [
            {"name": "ADSH", "type": "Varchar"},
            {"name": "CIK", "type": "Integer"},
            {"name": "NAME", "type": "Varchar"},
            {"name": "SIC", "type": "Integer"},
            {"name": "COUNTRYBA", "type": "Varchar"},
            {"name": "STPRBA", "type": "Varchar"},
            {"name": "CITYBA", "type": "Varchar"},
            {"name": "ZIPBA", "type": "Varchar"},
            {"name": "BAS1", "type": "Varchar"},
            {"name": "BAS2", "type": "Varchar"},
            {"name": "BAPH", "type": "Varchar"},
            {"name": "COUNTRYMA", "type": "Varchar"},
            {"name": "STPRMA", "type": "Varchar"},
            {"name": "CITYMA", "type": "Varchar"},
            {"name": "ZIPMA", "type": "Varchar"},
            {"name": "MAS1", "type": "Varchar"},
            {"name": "MAS2", "type": "Varchar"},
            {"name": "COUNTRYINC", "type": "Varchar"},
            {"name": "STPRINC", "type": "Varchar"},
            {"name": "EIN", "type": "Integer"},
            {"name": "FORMER", "type": "Varchar"},
            {"name": "CHANGED", "type": "Date"},
            {"name": "AFS", "type": "Varchar"},
            {"name": "WKSI", "type": "Integer"},
            {"name": "FYE", "type": "Integer"},
            {"name": "FORM", "type": "Varchar"},
            {"name": "PERIOD", "type": "Date"},
            {"name": "FY", "type": "Integer"},
            {"name": "FP", "type": "Varchar"},
            {"name": "FILED", "type": "Date"},
            {"name": "ACCEPTED", "type": "Timestamp"},
            {"name": "PREVRPT", "type": "Integer"},
            {"name": "DETAIL", "type": "Integer"},
            {"name": "INSTANCE", "type": "Varchar"},
            {"name": "NCIKS", "type": "Integer"},
            {"name": "ACIKS", "type": "Varchar"},
            {"name": "SOURCE_FILE", "type": "Varchar"}
        ],
        "sec_tag": [
            {"name": "TAG", "type": "Varchar"},
            {"name": "VERSION", "type": "Varchar"},
            {"name": "CUSTOM", "type": "Integer"},
            {"name": "ABSTRACT", "type": "Integer"},
            {"name": "DATATYPE", "type": "Varchar"},
            {"name": "IORD", "type": "Char"},
            {"name": "CRDR", "type": "Char"},
            {"name": "TLABEL", "type": "Varchar"},
            {"name": "DOC", "type": "Text"},
            {"name": "SOURCE_FILE", "type": "Varchar"}
        ],
        "sec_pre": [
            {"name": "ADSH", "type": "Varchar"},
            {"name": "REPORT", "type": "Integer"},
            {"name": "LINE", "type": "Integer"},
            {"name": "STMT", "type": "Varchar"},
            {"name": "INPTH", "type": "Integer"},
            {"name": "RFILE", "type": "Varchar"},
            {"name": "TAG", "type": "Varchar"},
            {"name": "VERSION", "type": "Varchar"},
            {"name": "PLABEL", "type": "Varchar"},
            {"name": "NEGATING", "type": "Integer"},
            {"name": "SOURCE_FILE", "type": "Varchar"}
        ]
    }

    # Define the schema for the fact tables
    fact_table_schema = [
        {"name": "ADSH", "type": "VARCHAR(20)"},
        {"name": "CIK", "type": "NUMBER(38,0)"},
        {"name": "COMPANY_NAME", "type": "VARCHAR(150)"},
        {"name": "FILING_DATE", "type": "NUMBER(38,0)"},
        {"name": "FISCAL_YEAR", "type": "NUMBER(38,0)"},
        {"name": "FISCAL_PERIOD", "type": "VARCHAR(2)"},
        {"name": "TAG", "type": "VARCHAR(256)"},
        {"name": "UNIT_OF_MEASURE", "type": "VARCHAR(20)"},
        {"name": "REPORT_DATE", "type": "NUMBER(8,0)"},
        {"name": "QTRS", "type": "NUMBER(38,0)"},
        {"name": "STATEMENT_TYPE", "type": "VARCHAR(2)"},
        {"name": "PLABEL", "type": "VARCHAR(512)"},
        {"name": "TOTAL_VALUE", "type": "NUMBER(38,10)"}
    ]

    if data_source == "Raw":
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            table_name, columns = list(raw_table_schema.items())[0]
            st.subheader(f"Table: {table_name}")
            df = pd.DataFrame(columns)
            st.dataframe(df, height=300, width=300)

        with col2:
            table_name, columns = list(raw_table_schema.items())[1]
            st.subheader(f"Table: {table_name}")
            df = pd.DataFrame(columns)
            st.dataframe(df, height=300, width=300)

        with col3:
            table_name, columns = list(raw_table_schema.items())[2]
            st.subheader(f"Table: {table_name}")
            df = pd.DataFrame(columns)
            st.dataframe(df, height=300, width=300)

        with col4:
            table_name, columns = list(raw_table_schema.items())[3]
            st.subheader(f"Table: {table_name}")
            df = pd.DataFrame(columns)
            st.dataframe(df, height=300, width=300)

        st.markdown("""
            **Note:** Append the year and quarter to the table name. 
            For example: `sec_num_2020Q2` / `sec_sub_2021Q3`.
            """)
    
    elif data_source == "JSON":
        st.subheader("Table: sec_json")
        json_columns = [
            {"name": "SYMBOL", "type": "Varchar"},
            {"name": "COMPANY_NAME", "type": "Varchar"},
            {"name": "START_DATE", "type": "Date"},
            {"name": "END_DATE", "type": "Date"},
            {"name": "RAW_JSON", "type": "variant[]"}
        ]
        st.markdown("""
            **Note:** Append the year and quarter to the table name. 
            For example: `sec_data_2020Q2` / `sec_data_2021Q3`.
            """)
        df = pd.DataFrame(json_columns)
        st.dataframe(df, height=200, width=400)

    elif data_source == "Fact Tables":
        st.subheader("Tables: balance_sheet / cash_flow / income_statement")
        st.markdown("""
            **Note:** Append the year and quarter to the table name. 
            For example: `balance_sheet_2020Q2` / `cash_flow_2021Q3` / `income_statement_2022Q4`.
            """)
        df = pd.DataFrame(fact_table_schema)
        st.dataframe(df, height=300, width=600)
def main():
    st.set_page_config(
        page_title="SEC Financial Data Explorer",
        layout="wide",
        initial_sidebar_state="expanded"
    )

    st.title("SEC Financial Data Explorer")
    
    # Create tabs for different functionalities
    tab1, tab2 = st.tabs(["Data Explorer", "Custom Query"])
    
    with tab1:
        st.header("Data Explorer")
        
        # Input controls
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            source = st.selectbox(
                "Select Data Source",
                ["RAW", "JSON", "FACT TABLES"],
                key="data_explorer_source"
            )
        with col2:
            year = st.selectbox(
                "Select Year",
                range(2009, datetime.now().year + 1),
                key="data_explorer_year"
            )
        with col3:
            quarter = st.selectbox(
                "Select Quarter",
                ["Q1", "Q2", "Q3", "Q4"],
                key="data_explorer_quarter"
            )
        
        with col4:
            data_type = st.selectbox(
                "Select Data Type",
                ["Balance Sheet", "Income Statement", "Cash Flow"],
                key="data_explorer_data_type"
            )

        if st.button("Load Data"):
            data = fetch_financial_data(year, quarter, data_type, source)
            if data:
                df = pd.DataFrame(data["data"])
                execution_time = data.get("execution_time", 0)
                st.write(f"Query executed in {execution_time} seconds.")
                
                if not df.empty:
                    st.subheader(f"({data_type} for {year} {quarter})")
                    
                    # Adjust column names based on the data source
                    if source == "FACT TABLES":
                        name_col, value_col = 'COMPANY_NAME', 'TOTAL_VALUE'
                    elif source == "JSON":
                        name_col, value_col = 'COMPANY_NAME', 'VALUE'
                    else:  # RAW
                        name_col, value_col = 'NAME', 'VALUE'
                    
                    if name_col in df.columns and value_col in df.columns:
                        df_aggregated = df.groupby(name_col).agg({value_col: 'sum'}).reset_index()
        
                        # Sort by total value and select top 10 companies
                        top_10_companies = df_aggregated.nlargest(10, value_col)[name_col]
                        
                        # Filter the original DataFrame to include only rows for the top 10 companies
                        df_top_10 = df[df[name_col].isin(top_10_companies)]
                        st.dataframe(df_top_10)
                        
                        # Sort data to get top and bottom 10 companies
                        df_sorted = df_aggregated.sort_values(by=value_col, ascending=False)
                        top_10 = df_sorted.head(10)
                        bottom_10 = df_sorted.tail(10)
                        
                        # Create a bar chart for top 10 companies
                        st.subheader("Top 10 Companies")
                        fig_top = px.bar(top_10, x=name_col, y=value_col, title=f"Top 10 {data_type} Overview")
                        st.plotly_chart(fig_top, use_container_width=True)
                        
                        # Create a bar chart for bottom 10 companies
                        st.subheader("Bottom 10 Companies")
                        fig_bottom = px.bar(bottom_10, x=name_col, y=value_col, title=f"Bottom 10 {data_type} Overview")
                        st.plotly_chart(fig_bottom, use_container_width=True)
                        
                        # Optional: Add a summary graph
                        st.subheader("Summary Graph")
                        summary_df = df.groupby(name_col).agg({value_col: 'sum'}).reset_index()
                        fig_summary = px.pie(summary_df, values=value_col, names=name_col, title="Company Value Distribution")
                        st.plotly_chart(fig_summary, use_container_width=True)
                    else:
                        st.warning(f"The data does not contain '{name_col}' or '{value_col}' columns.")
                else:
                    st.warning("No data available for the selected criteria.")
    
    with tab2:
        st.header("Custom Query")
        data_source = st.selectbox("Select Data Source", ["Raw", "JSON", "Fact Tables"])
        display_table_schemas(data_source)

        # Query input
        st.subheader("Execute Custom SQL Query")
        query = st.text_area(
            "Enter your SQL query:",
            height=200,
            help="Enter a valid SQL query to execute against the Snowflake database"
        )
        
        if st.button("Execute Query"):
            if query:
                df = execute_custom_query(query, data_source)
                if df is not None:
                    st.success("Query executed successfully!")
                    st.dataframe(df)
            else:
                st.warning("Please enter a query to execute")
                
                
if __name__ == "__main__":
    main()