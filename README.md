🚀 POC – Integração ClickUp → BigQuery → Power BI (Studio61)

📖 Contexto
A Studio61 (agência de marketing) precisava:
- Extrair dados do ClickUp
- Consolidar em Google BigQuery
- Construir relatórios no Power BI

A solução original consistia em:
- Script Python customizado
- Executado via job agendado na GCP
- Extração direta via APIs
- Carga manual no BigQuery

⚠️ Problemas Identificados
A arquitetura anterior apresentava:
❌ Código difícil de manter
❌ Alta dependência de conhecimento técnico específico
❌ Ausência de governança e rastreabilidade
❌ Escalabilidade limitada
❌ Dificuldade de adaptação a mudanças da API

🧠 Objetivo da POC
- Reestruturar a solução utilizando a plataforma Nekt, buscando:
- Simplificação da engenharia de dados
- Maior governança
- Escalabilidade
- Melhor manutenção
- Arquitetura organizada (medalhão)

🏗️ Arquitetura da Nova Solução
```
ClickUp
   ↓
Nekt Source (Extraction)
   ↓
Nekt Notebook (Transform - PySpark)
   ↓
Nekt Destination (Load)
   ↓
BigQuery (Gold - Base para BI)
   ↓
Power BI
```
<img width="1514" height="741" alt="Captura de tela 2026-03-02 174037" src="https://github.com/user-attachments/assets/8d7b4d5a-eaeb-4199-bb23-68caff986738" />

🔌 Extração (Bronze)
- Utilização de source nativa REST da Nekt
- Conexão autenticada com API do ClickUp
- Carga raw estruturada
- Execuções rastreáveis via plataforma

🔄 Transformação (Silver)
- Notebook em PySpark 
- Limpeza e padronização
- Tratamento de campos aninhados
- Tipagem adequada
- Estruturação para consumo analítico

📦 Carga Final (Gold)
- Utilização de destination nativa da Nekt
- Escrita automatizada no BigQuery
- Dataset estruturado para BI

🔥 Vantagens da Nova Arquitetura
Antes 				| Depois
----------------------------------------------------------------
Código 100% manual		| Solução low-code
Sem governança			| Execuções auditáveis
Alta complexidade		| Manutenção simplificada
Escalabilidade limitada		| Escalabilidade nativa
Falhas difíceis de rastrear	| Observabilidade via plataforma

📈 Resultados Técnicos
- Redução da complexidade operacional
- Melhoria significativa na rastreabilidade
- Facilidade de expansão para novos endpoints
- Separação clara de responsabilidades (medalhão)
- Redução do risco operacional

🛠️ Stack Tecnológica
- ClickUp (API)
- Nekt (Data Platform)
- Docker (para os testes de transformação usando a SDK da Nekt)
- PySpark
- Google BigQuery
- Power BI
- GCP

