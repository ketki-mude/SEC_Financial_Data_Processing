from diagrams import Diagram, Cluster
from diagrams.programming.language import Python
from diagrams.onprem.client import Users
from diagrams.gcp.compute import Run
from diagrams.aws.storage import S3
from diagrams.custom import Custom

# Set diagram formatting
graph_attr = {
    "fontsize": "24",
    "bgcolor": "white",
    "splines": "ortho",
    "rankdir": "TB"
}

# Base path for images
base_path = r"/Users/janvichitroda/Documents/Janvi/NEU/Big_Data_Intelligence_Analytics/Assignment 2/Lab4/architecture-diagram/input_icons"

# Create the diagram
with Diagram("SEC Financial Data", show=False, graph_attr=graph_attr, outformat="jpg", filename="architecture_diagram"):
   
    # User/Client
    user = Users("End User")
   
    # Frontend Cluster
    with Cluster("Frontend"):
        streamlit = Custom("Streamlit UI", f"{base_path}/streamlit.png")
   
    # Cloud Infrastructure Cluster
    with Cluster("GCP"):
        # GCP Cloud Run hosting the FastAPI backend
        cloud_run = Run("Cloud Run")

        with Cluster("Backend"):
            fastapi = Custom("FastAPI", f"{base_path}/FastAPI.png")

            with Cluster("Backend"):
                airflow = Custom("Airflow", f"{base_path}/Airflow.png")
                
                # Processing Pipelines
                with Cluster("Data Loading Pipeline"):
                    # Open Source Stack
                    with Cluster("Raw Data Loading"):
                        raw_pipeline = Custom("Raw Pipeline", f"{base_path}/parquet.png")
                    
                    # Enterprise Stack
                    with Cluster("JSON Data Loading"):
                        json_pipeline = Custom("JSON Pipeline", f"{base_path}/json.png")
                    
                    # DBT Stack
                    with Cluster("Fact Tables Data Loading"):
                        fact_pipeline = Custom("DBT Pipeline", f"{base_path}/dbt.png")

    # Storage Cluster
    with Cluster("Intermediate Storage"):
        s3_storage = S3("AWS S3\nStorage")
    
    with Cluster("Final Storage"):
        snowflake = Custom("Snowflake", f"{base_path}/snowflake.png")
 
    # Define the flow
    user >> streamlit >> cloud_run >> fastapi >> airflow
    
    # Processing flows
    airflow >> raw_pipeline
    airflow >> json_pipeline
    airflow >> fact_pipeline

    # Data flow into storage
    [raw_pipeline, json_pipeline] >> s3_storage
    fact_pipeline >> snowflake
    s3_storage >> snowflake

    # Return flow
    fastapi >> cloud_run >> streamlit
    snowflake >> fastapi
