# Datamart-Design

# **ETL Process and Datamart Design for Recruitment Data Engineer Test**

## **Overview**

This project demonstrates the implementation of an ETL process to build a dimensional datamart for OLAP analysis. The system extracts data from a MySQL database (`recruitment_dev`), transforms it into a standardized schema, and loads it into a PostgreSQL datamart. This datamart is designed to support dashboard requirements such as total stock, stock per entity tag, and stock per material with various filters.

---

## **Requirements**

### **Source Database**
- **Database**: MySQL
- **Schema**: Operational (as shown in `Data Source Schema` diagram)
- **Relevant Tables**:
  - `stocks`
  - `batches`
  - `entities`
  - `provinces`
  - `regencies`
  - `master_materials`
  - `master_activities`
  - `entity_tags`

### **Target Database**
- **Database**: PostgreSQL
- **Schema**: `data_mart_design`
- **Model**: Dimensional (Star Schema)

### **ETL Workflow**
- **Tools**: Apache Airflow
- **Frequency**: Daily (`@daily` schedule)

---

## **Process**

### **1. Extract**
The `extract_data` function queries the source database to pull data from multiple tables. The query uses SQL JOINs to combine relevant data into a single "One Big Table" extract.

#### Query Highlights:
- Ensures all required columns for the datamart are included.
- Joins all necessary tables to gather entity, material, activity, and stock information.

#### Handling:
- Null values are replaced with `None` to ensure compatibility.
- Datetime columns are converted to string format to avoid serialization errors in Airflow.

---

### **2. Transform**
The `transform_data` function prepares the data for the dimensional schema:
- **Dimension Tables**:
  - `dim_entities`: Contains entity metadata (e.g., entity name, tag, province, regency).
  - `dim_materials`: Includes material metadata (e.g., material name, vaccine status).
  - `dim_activities`: Maps activity IDs to names.
  - `dim_dates`: Tracks unique dates for stock operations.
- **Fact Table**:
  - `fact_stocks`: Stores stock quantities (on-hand, unreceived) by entity, material, activity, and date.

#### Transform Logic:
- Deduplicates data for each dimension table.
- Ensures the fact table links correctly to dimension tables via foreign keys.

---

### **3. Load**
The `load_data` function inserts the transformed data into the PostgreSQL datamart:
- Uses `INSERT ... ON CONFLICT DO NOTHING` to prevent duplicate entries.
- Ensures referential integrity between fact and dimension tables.
- Serial primary keys (e.g., `fact_id`) in the fact table are auto-generated.

---

## **Datamart Design**

### **Dimensional Schema**
#### **Dimension Tables**
1. **`dim_entities`**:
   - `entity_id` (Primary Key)
   - `entity_name`
   - `entity_tag`
   - `province`
   - `regency`

2. **`dim_materials`**:
   - `material_id` (Primary Key)
   - `material_name`
   - `vaccine_status`
   - `stockcount_status`
   - `addremove_status`
   - `openvial_status`

3. **`dim_activities`**:
   - `activity_id` (Primary Key)
   - `activity_name`

4. **`dim_dates`**:
   - `date_id` (Primary Key)
   - `date`
   - `expired_date`

#### **Fact Table**
1. **`fact_stocks`**:
   - `fact_id` (Primary Key)
   - `entity_id` (Foreign Key)
   - `material_id` (Foreign Key)
   - `activity_id` (Foreign Key)
   - `date` (DATE)
   - `on_hand_stock`
   - `unreceived_stock`

---

## **DAG Implementation**

The DAG, `data_mart_etl_dag`, defines the ETL workflow:
1. **Extract Task**:
   - Connects to MySQL using SQLAlchemy and retrieves data using a pre-defined query.
2. **Transform Task**:
   - Cleans and normalizes the extracted data for the target schema.
3. **Load Task**:
   - Inserts data into PostgreSQL using `ON CONFLICT DO NOTHING` to prevent duplication.

The script ensures:
- Logs are available for debugging and monitoring.
- Tasks are modular and easy to maintain.

---

## **Query Design**

1. **Total Stock**:
   ```sql
   SELECT SUM(on_hand_stock) AS total_stock
   FROM data_mart_design.fact_stocks;
   ```

2. **Stock per Entity Tag**:
   ```sql
   SELECT e.entity_tag, SUM(fs.on_hand_stock) AS stock_per_tag
   FROM data_mart_design.fact_stocks fs
   JOIN data_mart_design.dim_entities e ON fs.entity_id = e.entity_id
   GROUP BY e.entity_tag;
   ```

3. **Stock per Material**:
   ```sql
   SELECT m.material_name, SUM(fs.on_hand_stock) AS stock_per_material
   FROM data_mart_design.fact_stocks fs
   JOIN data_mart_design.dim_materials m ON fs.material_id = m.material_id
   GROUP BY m.material_name;
   ```

---

## **File Description**

1. **`Tugas Rekrutment Data Engineer Badr Interactive.docx`**:
   - Contains the task description and requirements.
2. **`data_mart_etl_dag.py`**:
   - Implements the ETL workflow using Apache Airflow.
3. **`Datamart Design.drawio.png`**:
   - Visual representation of the datamart schema.
4. **`datamart_ddl.sql`**:
   - SQL script for creating the dimensional schema.
5. **`query.sql`**:
   - SQL queries to generate required dashboard metrics.

---

## **Execution Instructions**

1. **Set Up Database Connections**:
   - Update `SOURCE_DB_URI` and `TARGET_DB_URI` in `data_mart_etl_dag.py` with valid credentials.

2. **Deploy DAG**:
   - Place `data_mart_etl_dag.py` in the Airflow DAGs folder.
   - Start the Airflow scheduler and webserver.

3. **Run ETL**:
   - Trigger the DAG via the Airflow UI or CLI.

4. **Verify Datamart**:
   - Check the PostgreSQL datamart to ensure data consistency.

5. **Query Dashboard Metrics**:
   - Execute queries in `query.sql` to generate dashboard outputs.

---

## **Assumptions**

- Data integrity in the source database is maintained (e.g., no missing foreign key references).
- No major schema changes in the source database.

---

## **Contact**

For any issues or inquiries regarding this implementation, please contact [Your Name/Email].
