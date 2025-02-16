# **SEC Financial Dataset Processing System**
This project automates the ingestion, transformation, and validation of SEC financial datasets using **Snowflake, Apache Airflow, DBT, FastAPI, and Streamlit**. It ensures seamless data processing, integrity checks, and interactive financial data visualization.

---
## **📌 Project Resources**
- 📘 **Google Codelab:** [CodeLab](https://codelabs-preview.appspot.com/?file_id=1mBO6xQxSLutdNoxKHmQyAxqUgg8YdiZU9MckpMwRXxM#6)
- 🌐 **App (Streamlit Cloud):** [Streamlit Link](https://dynaledger-ixvkclqgn7gofx9bzf6erk.streamlit.app/)
- 🌍 **Apache Airflow:** [Airflow](http://34.145.156.207:8080/)
- 🎥 **YouTube Demo:** [YouTube Video](https://youtu.be/7x4iwCADyJA)

---

## **📌 Technologies Used**
![Snowflake](https://img.shields.io/badge/-Snowflake-56CCF2?style=for-the-badge&logo=snowflake&logoColor=white)
![Apache Airflow](https://img.shields.io/badge/-Apache_Airflow-017CEE?style=for-the-badge&logo=apache-airflow&logoColor=white)
![DBT](https://img.shields.io/badge/-DBT-FE6829?style=for-the-badge&logo=dbt&logoColor=white)
![FastAPI](https://img.shields.io/badge/-FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white)
![Streamlit](https://img.shields.io/badge/-Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white)
![AWS S3](https://img.shields.io/badge/-AWS_S3-569A31?style=for-the-badge&logo=amazon-s3&logoColor=white)

---

## **📌 Architecture Diagram**
<p align="center">
  <img src="https://github.com/Damg7245-BigDataIntelligence/DynaLedger/blob/main/architecture-diagram/architecture_diagram.jpg" 
       alt="Architecture Diagram" width="600">
</p>

---

## **📌 Project Flow**

### **Step 1: User Query Selection**
- Users have three options to select from **JSON, RAW, and Fact Tables** to query SEC financial data.
- Additionally, users can write and execute **custom SQL queries** on the SEC dataset.
- The selected request is sent to **Snowflake via FastAPI**.

### **Step 2: Data Pipeline Execution**
- **Three automated pipelines** are running for **JSON, RAW, and DBT** transformations.
- Each pipeline ensures structured data processing and ingestion into Snowflake.

### **Step 3: Fetching & Displaying Data**
- If the user selects **RAW data**, the processed data from the **RAW pipeline in Snowflake** is fetched.
- The retrieved data is displayed in **tabular format** along with **graphical representations**.

### **Step 4: JSON and Fact Table Processing**
- If the user selects **JSON** and **Fact Table**, the respective pipeline in Snowflake processes the data.
- The transformed data is displayed in **tables and graphs** for better visualization.

---

## **📌 Contributions**
| **Name** | **Contribution** |
|----------|----------------|
| **Janvi Bharatkumar Chitroda** | 33% - **DBT Data Loading**, **Snowflake**, **DBT Airflow Pipeline**, **Cloud Deployment**, **S3 Connection**. |
| **Ketki Mude** | 33% - **JSON Conversion & Data Loading**, **Snowflake**, **S3 Connection**, **JSON Airflow Pipeline**. |
| **Sahil Mutha** | 33% - **RAW Data Loading**, **Snowflake**, **S3 Connection**, **Streamlit**, **FastAPI**, **RAW Airflow Pipeline Deployment**. |

---

## **📌 Attestation**
**WE CERTIFY THAT WE HAVE NOT USED ANY OTHER STUDENTS' WORK IN OUR ASSIGNMENT AND COMPLY WITH THE POLICIES OUTLINED IN THE STUDENT HANDBOOK.**
