# SCD2
# SCD Type 2 Implementation using Azure Data Factory, Azure Synapse SQL, and Azure Databricks

## Project Overview
This project demonstrates **Slowly Changing Dimension (SCD) Type 2** implementation using Azure services.

The objective is to track historical changes in employee data.  
If any employee attribute changes (e.g., address or salary), the old record is expired and a new record is inserted.

---

## Architecture

SQL Database (Source)
↓
Azure Data Factory (Copy Activity)
↓
Azure Data Lake Storage (ADLS Gen2)
↓
Azure Databricks (PySpark Transformation)
↓
Azure Synapse SQL (Target Dimension Table)

---

## Technologies Used

- Azure SQL Database
- Azure Data Factory (ADF)
- Azure Data Lake Storage Gen2 (ADLS)
- Azure Databricks
- PySpark
- Azure Synapse SQL

---

## Source Data

### Day 1 Data

| EmpID | EmpName | EmpSal | EmpAdd |
|-------|---------|--------|--------|
| 11 | Ajay | 20000 | Pune |
| 12 | Amit | 25000 | Mumbai |

### Day 2 Data

| EmpID | EmpName | EmpSal | EmpAdd |
|-------|---------|--------|--------|
| 11 | Ajay | 20000 | Hyd |
| 12 | Amit | 25000 | Mumbai |
| 13 | Kawal | 30000 | Nasik |

---

## Target SCD Type 2 Table Structure

```sql
CREATE TABLE DimEmployee (
    Emp_key INT IDENTITY(1,1),
    EmpID INT,
    EmpName VARCHAR(50),
    EmpSal INT,
    EmpAdd VARCHAR(50),
    St_dt DATE,
    End_dt DATE,
    IsActive BIT
);
```

---

## Implementation Steps

### Step 1: Create Source Table

```sql
CREATE TABLE Employee_Source (
    EmpID INT,
    EmpName VARCHAR(50),
    EmpSal INT,
    EmpAdd VARCHAR(50),
    LoadDate DATE
);
```

---

### Step 2: Insert Initial Data

```sql
INSERT INTO Employee_Source VALUES
(11,'Ajay',20000,'Pune','2025-07-21'),
(12,'Amit',25000,'Mumbai','2025-07-21');
```

---

### Step 3: Copy Data to ADLS using ADF

ADF Copy Activity used to move source data from SQL Database to Azure Data Lake Storage.

---

### Step 4: Read Data in Databricks

```python
df = spark.read.option("header",True).csv("/mnt/raw/employee/")
```

---

### Step 5: Read Active Target Records

```python
target_df = spark.read.format("jdbc") \
.option("dbtable","DimEmployee") \
.load()

active_df = target_df.filter("IsActive = 1")
```

---

### Step 6: Detect New and Changed Records

```python
joined = source_df.alias("src").join(
    active_df.alias("tgt"),
    "EmpID",
    "left"
)
```

### New Records

```python
new_records = joined.filter("tgt.EmpID is null")
```

### Changed Records

```python
changed_records = joined.filter(
"src.EmpAdd <> tgt.EmpAdd OR src.EmpSal <> tgt.EmpSal"
)
```

---

### Step 7: Expire Old Records

```sql
UPDATE DimEmployee
SET End_dt='2025-07-22',
    IsActive=0
WHERE EmpID=11
AND IsActive=1;
```

---

### Step 8: Insert New Version

Insert updated and new records with:

- New Start Date
- End Date = 9999-12-31
- IsActive = 1

---

## Final Output

| Emp_key | EmpID | EmpName | EmpSal | EmpAdd | St_dt | End_dt | IsActive |
|---------|-------|---------|--------|--------|-------|---------|----------|
| 1 | 11 | Ajay | 20000 | Pune | 2025-07-21 | 2025-07-22 | 0 |
| 2 | 12 | Amit | 25000 | Mumbai | 2025-07-21 | 9999-12-31 | 1 |
| 3 | 11 | Ajay | 20000 | Hyd | 2025-07-23 | 9999-12-31 | 1 |
| 4 | 13 | Kawal | 30000 | Nasik | 2025-07-23 | 9999-12-31 | 1 |

---

## Key Learning

- Implemented Slowly Changing Dimension Type 2
- Tracked historical data changes
- Used ADF for orchestration
- Used Databricks for transformation logic
- Used Synapse SQL for target storage

---

## Author

**Vishal Chaudhary**  
Azure Data Engineer
