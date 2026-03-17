# Data Source APIs --> BigQuery --> Power BI using Nekt

## 📖 Context
One of my customers needed to:
- Extract data from ClickUp
- Consolidate it into Google BigQuery
- Build reports in Power BI

The original solution consisted of:
- A custom Python ETL API
- Executed via a scheduled job in GCP
- Direct extraction through ClickUp endpoints
- Manual load into BigQuery

## ⚠️ Identified Problems
The previous architecture presented:
- ❌ Hart-to-maintain code
- ❌ High dependency on specific technical knowledge
- ❌ Lack of governance and traceability
- ❌ Limited scalability
- ❌ Difficulty adapting to API changes

## 🧠 Main Objetives
Restructure the solution using the Nekt platform, aiming to:
- Simplify the solution (less code)
- Increase governance
- Improve scalability
- Improve maintainability
- Implement an organized architecture (Medallion Architecture)
  
## 🏗️ New Architecture
```
ClickUp
   ↓
Nekt Source             (Extract to Bronze)
   ↓
Nekt PySpark Notebook   (Transform to Silver)
   ↓
Nekt Destination        (Load to Gold)
   ↓
BigQuery                (BI Data Source)
   ↓
Power BI
```
<img width="1470" height="856" alt="image" src="https://github.com/user-attachments/assets/b6dd9fe0-37dd-431e-a06f-21a2fe8d6aac" />
<img width="1514" height="741" alt="Captura de tela 2026-03-02 174037" src="https://github.com/user-attachments/assets/8d7b4d5a-eaeb-4199-bb23-68caff986738" />

### 🥉 Extraction (Bronze)
- Use of Nekt’s native REST source
- Authenticated connection to the ClickUp API
- Structured raw data ingestion
- Executions traceable via the platform

### 🥈 Transformations (Silver)
- PySpark notebook 
- Data cleaning and standardization
- Proper data typing
- Structured for analytical consumption

### 🥇 Final Load (Gold)
- Use of Nekt’s native destination
- Automated write into BigQuery
- BI-ready structured dataset

## 🔥 Before/After Comparison
| **Before**             | **After**               |
|------------------------|-------------------------|
| 100% manual code       | Low-code solution       |
| No governance          | Auditable executions    |
| High complexity        | Simplified maintenance  |
| Limited scalability    | Native scalability      |
| No rastreability       | Platform observability  |

## 📈 Technical Results
- ✅ Reduced operational complexity
- ✅ Reduced operational risks
- ✅ Significant improvement in traceability
- ✅ Easy expansion to new endpoints
- ✅ Clear separation of responsibilities (Medallion Architecture)

## 🛠️ Technology Stack
- ClickUp (API)
- Nekt (Data Platform)
- Docker
- PySpark
- GCP
- Google BigQuery
- Power BI








