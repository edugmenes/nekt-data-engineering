# Data Source APIs --> BigQuery --> Power BI Integration using Nekt

## 📖 Context
One of my customers needed to:
- Extract data from ClickUp and Conta Azul ERP
- Consolidate it into Google BigQuery
- Build reports in Power BI

The original solution consisted of:
- A custom Python ETL API
- Executed via a scheduled job in GCP
- Direct extraction through APIs endpoints
- Manual load into BigQuery

## ⚠️ Identified Problems
The previous architecture presented:
- ❌ Hart-to-maintain code
- ❌ High dependency on specific technical knowledge
- ❌ Lack of governance and traceability
- ❌ Limited scalability
- ❌ Difficulty adapting to APIs changes

## 🧠 Main Objetives
Restructure the solution using the Nekt platform, aiming to:
- Simplify the solution (less code)
- Increase governance
- Improve scalability
- Improve maintainability
- Implement an organized architecture (Medallion Architecture)
  
## 🏗️ New Architecture
```
ClickUp API
Conta Azul API          (Sources)
   ↓
Nekt Source             (Extract to Bronze)
   ↓
Nekt PySpark Notebook   (Transform to Silver)
   ↓
Nekt Destination        (Load to Gold)
   ↓
BigQuery                (Analytical Datasets)
   ↓
Power BI                (BI layer)
```
<img width="3129" height="1112" alt="Untitled-2026-03-02-1640" src="https://github.com/user-attachments/assets/668f0dce-a15f-4c4a-8eb7-0e39963ea1f8" />
<img width="1915" height="864" alt="image" src="https://github.com/user-attachments/assets/0fa3b453-71c6-4673-933d-1fbba71b1010" />


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
- ClickUp (REST APIs)
- Conta Azul (REST APIs)
- Nekt (Data Engineering Platform)
- Docker
- PySpark
- GCP
- Google BigQuery
- Power BI








