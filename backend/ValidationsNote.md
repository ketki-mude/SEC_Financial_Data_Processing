**DBT Schema Validations and Handling Failed Cases**

## **1. Overview of Validations**
This document outlines the validation checks implemented in the DBT schema for ensuring data quality in SEC financial datasets. Each table undergoes a series of validations to maintain data integrity and consistency.

### **Types of Validations Applied**
#### **a. Primary Key Constraints**
- Ensures unique and non-null values in primary key fields.

#### **b. Not Null Constraints**
- Ensures required fields contain data and are not left empty.

#### **c. Accepted Values Constraints**
- Ensures that categorical fields only contain predefined values.

#### **d. Format Constraints (Regex-Based Validations)**
- Enforces specific formats such as ISO country codes, date formats, and numeric constraints.

#### **e. Range Constraints**
- Ensures numeric fields fall within expected ranges, such as fiscal years and SIC codes.

## **2. Handling Validation Failures**
If a validation fails, appropriate remediation strategies are applied to clean or reject the data before further processing. Below are specific cases and resolution approaches:

### **For num.txt**
- **Duplicate `adsh` (Primary Key Violation)**
  - Identify duplicates and remove erroneous records.
  - If duplicates are valid, introduce a composite key by adding another unique identifier.
- **Invalid `qtrs` Values**
  - Ensure rounding of the quarter values to the nearest whole number.
  - If the value is 0, confirm that it represents a point-in-time value and not a quarterly report.

### **For tag.txt**
- **Duplicate `tag` Values**
  - Investigate the source of duplication.
  - Deduplicate by considering the most recent entry or merging values logically.

### **For sub.txt**
- **Invalid `afs` Values**
  - Remove or map extra values (e.g., `2-ACC`, `1-LAF`, `4-NON`) to standard values.
- **Invalid `ein` Format**
  - Enforce regex `^[0-9]{9}$` by truncating or filtering out non-compliant EINs.
- **Incorrect `sic` Values**
  - Ensure all SIC codes have four digits.
  - Prefix with leading zeroes if necessary.
- **Invalid `form` Entries**
  - Enforce regex `^[A-Za-z0-9-]+$` and remove incorrect values.
- **Missing `countryinc` Values**
  - Use external sources to populate missing country incorporation data.
- **Invalid `fye` Values**
  - Ensure `fye` has four digits.
  - Add leading zeroes where required.
- **Missing `cityba`, `fp`, `fy`, `fye`, `countryba` Values**
  - Impute missing values where possible or mark them as `UNKNOWN`.
- **Incorrect `accepted` Date Format**
  - Convert timestamp values into standard format (YYYYMMDD).
- **Invalid `baph` Phone Number Format**
  - Apply regex `^[0-9 ()+\-]+$` and remove incorrect values.

### **For pre.txt**
- **Missing `llabel` Values**
  - Use `plabel` or other available metadata as a fallback.
  - If unavailable, replace with `UNKNOWN` or flag for manual review.

## **3. Summary of Handling Approach**
1. **Deduplication** – Remove or merge duplicate entries.
2. **Data Correction** – Adjust values to fit required formats.
3. **Imputation** – Fill missing values using logical assumptions or external sources.
4. **Rejection** – Discard non-recoverable records to maintain data quality.
5. **Standardization** – Convert data formats to meet consistency standards.

By implementing these strategies, we ensure that our DBT models maintain high-quality financial data for further analysis and reporting.

