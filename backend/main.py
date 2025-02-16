from fastapi import FastAPI, HTTPException, Query
from dotenv import load_dotenv
import snowflake.connector
import os
import numpy as np
from pydantic import BaseModel
import time
 
app = FastAPI()
load_dotenv()
 
 
def get_snowflake_connection(schema_name: str):
    try:
        conn = snowflake.connector.connect(
            user=os.getenv('SNOWFLAKE_USER'),
            password=os.getenv('SNOWFLAKE_PASSWORD'),
            account=os.getenv('SNOWFLAKE_ACCOUNT'),
            warehouse=os.getenv('SNOWFLAKE_WAREHOUSE'),
            database=os.getenv('SNOWFLAKE_DATABASE'),
            schema=schema_name  # Set schema dynamically
        )
        print(f"Successfully connected to Snowflake with schema {schema_name}.")
        return conn
    except Exception as e:
        print(f"Error connecting to Snowflake: {str(e)}")
        raise HTTPException(status_code=500, detail="Database connection error")
 
 
class QueryModel(BaseModel):
    query: str
   
   
def sanitize_float_values(data):
    """Convert special float values to None."""
    for item in data:
        for key, value in item.items():
            if isinstance(value, float) and (np.isnan(value) or np.isinf(value)):
                item[key] = None
    return data
 
 
@app.get("/check-availability")
async def check_data_availability(source: str, year: int, quarter: str):
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
       
        # Query to check if data exists for the given parameters
        query = f"""
        SELECT COUNT(*)
        FROM sec_tag_data_raw
        WHERE source_file = '{year}Q{quarter.replace("Q", "")}'
        """
       
        cur.execute(query)
        count = cur.fetchone()[0]
       
        return {"available": count > 0}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
 
@app.get("/get-table-info")
async def get_table_info(data_source: str, year: int, quarter: str):
    try:
        if data_source == "RAW":
            schema_name = "SEC_DATA_RAW"
        elif data_source == "JSON":
            schema_name = "SEC_DATA_JSON"
        elif data_source == "Fact Tables":
            schema_name = "SEC_DATA_DFT"
        conn = get_snowflake_connection(schema_name)
        cur = conn.cursor()
       
        stage_name = f"{year}Q{quarter.replace('Q', '')}"
        if data_source == "Raw":
            tables = [f"SEC_NUM_{stage_name}", f"SEC_PRE_{stage_name}", f"SEC_SUB_{stage_name}", f"SEC_TAG_{stage_name}"]
        elif data_source == "JSON":
            tables = [f"SEC_DATA_{stage_name}"]
        elif data_source == "Fact Tables":
            tables = [f"BALANCE_SHEET_{stage_name}", f"INCOME_STATEMENT_{stage_name}", f"CASH_FLOW_{stage_name}"]
        else:
            raise HTTPException(status_code=400, detail="Invalid data source")
       
        table_info = []
        for table in tables:
            result = cur.execute(f"DESCRIBE TABLE {schema_name}.{table}")
            columns = [{"name": row[0], "type": row[1]} for row in cur.fetchall()]
           
            cur.execute(f"SELECT * FROM {schema_name}.{table} LIMIT 3")
            sample_data = [dict(zip([desc[0] for desc in cur.description], row)) for row in cur.fetchall()]
           
            table_info.append({"name": table, "columns": columns, "sample_data": sample_data})
       
        return table_info
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to fetch table info")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
           
@app.post("/execute-custom-query")
async def execute_custom_query(query_model: QueryModel, data_source: str):
    try:
        if data_source == "Raw":
            schema_name = "SEC_DATA_RAW"
        elif data_source == "JSON":
            schema_name = "SEC_DATA_JSON"
        elif data_source == "Fact Tables":
            schema_name = "SEC_DATA_DFT"
        conn = get_snowflake_connection(schema_name)
        cur = conn.cursor()
        query = query_model.query
        cur.execute(query)
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
        # Sanitize float values
        sanitized_results = sanitize_float_values(results)
        return {"data": sanitized_results}
    except Exception as e:
        raise HTTPException(status_code=500, detail="Failed to execute query")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
           
                       
@app.get("/get-financial-data")
async def get_financial_data(year: int, quarter: str, data_type: str, source: str):
    try:
        schema_name = None
        query = None
        if source == "RAW":
            schema_name = "SEC_DATA_RAW"
        elif source == "FACT TABLES":
            schema_name = "SEC_DATA_DFT"
        elif source == "JSON":
            schema_name = "SEC_DATA_JSON"
        conn = get_snowflake_connection(schema_name)
        cur = conn.cursor()
       
        start_time = time.time()
        if source == "RAW":
            # Existing logic for RAW data
            stmt_type = {
                "Income Statement": "IC",
                "Balance Sheet": "BS",
                "Cash Flow": "CF"
            }.get(data_type)
           
            if not stmt_type:
                raise HTTPException(status_code=400, detail="Invalid data type")
           
            query = f"""
            SELECT
                s.adsh, s.cik, s.name, s.sic, s.countryba, s.stprba, s.cityba, s.filed,
                p.line, p.plabel, n.tag, n.version, n.ddate, n.qtrs, n.uom, n.value
            FROM
                {schema_name}.sec_sub_{year}Q{quarter.replace('Q', '')} s
            JOIN
                {schema_name}.sec_pre_{year}Q{quarter.replace('Q', '')} p ON s.adsh = p.adsh
            JOIN
                {schema_name}.sec_num_{year}Q{quarter.replace('Q', '')} n ON s.adsh = n.adsh AND p.tag = n.tag AND p.version = n.version
            WHERE
                p.stmt = '{stmt_type}'
            ORDER BY
                s.adsh, p.line;
            """
        elif source == "FACT TABLES":
            # Existing logic for FACT data
            table_name = {
                "Balance Sheet": f"BALANCE_SHEET_{year}Q{quarter.replace('Q', '')}",
                "Income Statement": f"INCOME_STATEMENT_{year}Q{quarter.replace('Q', '')}",
                "Cash Flow": f"CASH_FLOW_{year}Q{quarter.replace('Q', '')}"
            }.get(data_type)
           
            if not table_name:
                raise HTTPException(status_code=400, detail="Invalid data type")
           
            query = f"SELECT * FROM {schema_name}.{table_name}"
       
        elif source == "JSON":
            # Logic for JSON data
            view_name = {
                "Balance Sheet": f"view_balance_sheet_{year}_Q{quarter.replace('Q', '')}",
                "Income Statement": f"view_income_statement_{year}_Q{quarter.replace('Q', '')}",
                "Cash Flow": f"view_cash_flow_{year}_Q{quarter.replace('Q', '')}"
            }.get(data_type)
           
            if not view_name:
                raise HTTPException(status_code=400, detail="Invalid data type")
           
            query = f"SELECT * FROM {schema_name}.{view_name}"
       
        print(f"Executing query: {query}")
        cur.execute(query)
        execution_time = time.time() - start_time
        columns = [desc[0] for desc in cur.description]
        rows = cur.fetchall()
        results = [dict(zip(columns, row)) for row in rows]
       
        sanitized_results = sanitize_float_values(results)
       
        return {"data": sanitized_results, "execution_time": execution_time}
    except Exception as e:
        print(f"Error executing query: {str(e)}")
        raise HTTPException(status_code=500, detail="Failed to fetch data from the database")
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
           
           
@app.get("/query-data")
async def execute_query(query: str = Query(..., min_length=1)):
    try:
        conn = get_snowflake_connection()
        cur = conn.cursor()
       
        # Execute the query
        cur.execute(query)
       
        # Fetch column names
        columns = [desc[0] for desc in cur.description]
       
        # Fetch all rows
        rows = cur.fetchall()
       
        # Convert to list of dictionaries
        results = [dict(zip(columns, row)) for row in rows]
       
       
        sanitized_results = sanitize_float_values(results)
       
        return {"data": sanitized_results}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        if 'cur' in locals():
            cur.close()
        if 'conn' in locals():
            conn.close()
 