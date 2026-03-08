# Sanctions Monitoring — Multi-Agent Production Architecture

## Technical Deep-Dive & Interview Preparation Guide

### Built on Azure AI Foundry

---

## 1. EXECUTIVE SUMMARY

This document describes a **production-grade Sanctions Monitoring System** that combines a traditional high-throughput screening pipeline (Sanctions Manager) with a modern **Multi-Agent AI Triage System** built on **Azure AI Foundry Agent Service**. The AI layer assists bank compliance analysts in reviewing, confirming, or waiving sanctions alerts — dramatically reducing manual effort while maintaining full regulatory auditability.

The architecture employs three core multi-agent design patterns:

- **Supervisor Pattern** — A coordinator agent orchestrates the entire triage workflow
- **Fan-out Pattern** — Four specialist agents execute in parallel for throughput
- **Fan-in Pattern** — A risk assessment agent aggregates all evidence into a single disposition recommendation

A **Human-in-the-Loop** layer ensures that a bank analyst always makes the final Confirm/Waive decision, satisfying regulatory requirements.

---

## 2. END-TO-END ARCHITECTURE WALKTHROUGH

The architecture is divided into four major zones:

```
┌──────────────┐     ┌───────────────────┐     ┌──────────────────────────────┐     ┌──────────────────┐
│  Data Sources │────▶│ Sanctions Manager │────▶│ Multi-Agent Alert Triage     │────▶│ Azure AI Foundry │
│  (Batch/RT)   │     │ (Alert Generation)│     │ System (AI-Assisted Review)  │     │ Platform Services│
└──────────────┘     └───────────────────┘     └──────────────────────────────┘     └──────────────────┘
```

---

## 3. ZONE 1 — EXTERNAL DATA SOURCES

### 3.1 Landing Zone (Batch Ingestion)

- **Purpose**: Receives large batch files (customer lists, wire transfers, trade records) split into thousands of smaller chunks for parallel processing.
- **Format**: Typically CSV, XML, or fixed-width flat files from upstream core banking systems.
- **Volume**: Millions of records per day; files are pre-split into ~1,000-record segments to enable horizontal scaling.

### 3.2 IBM MQ / Kafka (Real-Time Stream)

- **Purpose**: Receives real-time transaction messages — wire transfers, SWIFT messages, payments — as they occur.
- **Technology**: IBM MQ for legacy mainframe integration; Apache Kafka for modern event-streaming workloads.
- **Latency Target**: Sub-second ingestion into the screening pipeline.

---

## 4. ZONE 2 — SANCTIONS MANAGER (Alert Generation Pipeline)

This is the traditional **screening engine** responsible for generating sanctions alerts. It runs inside a dedicated environment (on-prem or private cloud).

### 4.1 Component Breakdown

| Component                              | Role                                                                                                                                | Technology                                                            |
| -------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------- |
| **3rd-Party Sanctions Data Provider**  | External cloud service delivering sanctions lists (OFAC SDN, EU Consolidated, UN, HMT, etc.)                                        | Dow Jones, Refinitiv World-Check, Accuity                             |
| **Sanction Ingestion Service**         | Pulls/receives updated sanctions lists, normalizes data, and loads into the Sanction Master DB                                      | REST/SFTP pull, scheduled via cron/Airflow                            |
| **File Picker Orchestrator**           | Picks up incoming batch files from the Landing Zone and real-time messages from MQ/Kafka; routes them into the parser               | Stateless microservice, K8s-deployed                                  |
| **Parser Service (Parallel Scrubber)** | Parses, normalizes, and tokenizes customer/transaction records into a canonical format ready for matching                           | Multi-threaded; horizontally scaled pods                              |
| **High-Throughput Message Bus**        | Internal pub/sub layer decoupling the parser from the matching engine; enables backpressure handling                                | Kafka / Azure Service Bus / RabbitMQ                                  |
| **Matching Engine (Fuzzy Logic)**      | The core screening algorithm — performs fuzzy name matching (Jaro-Winkler, Levenshtein, phonetic) against the Sanction Master DB    | Custom C++/Java engine or vendor solution (e.g., Fircosoft, Actimize) |
| **Sanction Master DB (Redis + SQL)**   | Stores the normalized sanctions lists; Redis for hot-path lookups, SQL for full relational queries                                  | Redis Cluster + Azure SQL / PostgreSQL                                |
| **Alert Management Service**           | Receives match results from the engine and creates **alerts** for analyst review; this is the handoff point to the AI triage system | REST API, writes to alert queue                                       |
| **API Gateway + Rate Limiter**         | Entry point for the analyst-facing UI and downstream systems; enforces rate limits, throttling, and routing                         | Azure API Management / Kong / Envoy                                   |
| **IDP / Entra ID (Auth)**              | Identity provider authenticating all API calls; integrated with Microsoft Entra ID (Azure AD) for SSO and RBAC                      | OAuth 2.0 / OpenID Connect                                            |

### 4.2 Data Flow (Alert Generation)

```
Landing Zone ──┐
               ├──▶ File Picker ──▶ Parser ──▶ Message Bus ──▶ Matching Engine ──┬──▶ Alert Management Service
IBM MQ/Kafka ──┘         Orchestrator    (Parallel        (High-Throughput)   (Fuzzy Logic)    │            ▼
                                          Scrubber)                                │     [NEW ALERT created]
                                                                                   │
                                                           Sanction Master DB ◀────┘
                                                           (Redis + SQL)
                                                                ▲
                                                                │
                                               Sanction Ingestion Service ◀── 3rd-Party Provider
```

---

## 5. ZONE 3 — MULTI-AGENT ALERT TRIAGE SYSTEM

This is the AI-powered layer that **assists bank analysts** in reviewing alerts. It is built entirely on **Azure AI Foundry Agent Service** — NOT Semantic Kernel. Each agent is a managed agent instance with its own system prompt, tools, and capabilities.

### 5.1 Design Pattern: Supervisor + Fan-out / Fan-in

```
                            ┌──────────────────────────┐
                            │  Triage Coordinator Agent │  ◀── Supervisor Pattern
                            │  (Plans & Delegates)      │
                            └─────────┬────────────────┘
                    ┌─────────┬───────┴───────┬──────────┐
                    ▼         ▼               ▼          ▼       ◀── Fan-out (Parallel)
            ┌───────────┐ ┌──────────┐ ┌───────────┐ ┌───────────┐
            │ Alert     │ │ Entity   │ │ Historical│ │ Compliance│
            │ Enrichment│ │ Resolution│ │ Analysis │ │ Rules     │
            │ Agent     │ │ Agent    │ │ Agent     │ │ Agent     │
            └─────┬─────┘ └────┬─────┘ └─────┬─────┘ └─────┬─────┘
                  └──────┬─────┴──────┬───────┘             │
                         ▼            ▼                     ▼       ◀── Fan-in (Aggregate)
                    ┌──────────────────────────────────────┐
                    │       Risk Assessment Agent          │
                    │   (Score + Recommendation)           │
                    └─────────────┬────────────────────────┘
                                  ▼                                  ◀── Human-in-the-Loop
                    ┌──────────────────────────────────────┐
                    │   Bank Analyst Review Dashboard      │
                    │     [ Confirm ]     [ Waive ]        │
                    └─────────────┬────────────────────────┘
                    ┌─────────────┼────────────────┐
                    ▼             ▼                 ▼
              Alert Disposition  Audit Trail    Feedback Loop
              Service            (Compliance)   (Learning)
```

### 5.2 Agent-by-Agent Detail

#### 5.2.1 Triage Coordinator Agent (Supervisor)

| Attribute                   | Detail                                                                                                                                             |
| --------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Role**                    | Receives a new alert from the Alert Management Service, analyzes it, plans the triage steps, and delegates sub-tasks to the four specialist agents |
| **Azure AI Foundry Config** | Registered as a managed agent in Azure AI Agent Service; uses GPT-4o as the backbone model                                                         |
| **System Prompt**           | Instructs the agent to act as a sanctions compliance triage coordinator; defines the delegation protocol                                           |
| **Tools**                   | `delegate_to_agent(agent_name, task_payload)` — invokes specialist agents via the Agent Service API                                                |
| **Thread**                  | Creates or reuses a **Shared Agent Thread** (conversation memory) stored in Azure Cosmos DB so all agents share context about the current alert    |
| **Pattern**                 | **Supervisor** — it does NOT do the analysis itself; it only plans and delegates                                                                   |

#### 5.2.2 Alert Enrichment Agent

| Attribute        | Detail                                                                                                                                                                     |
| ---------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Role**         | Enriches the raw alert with contextual data — KYC records, transaction history, account profile, entity corporate structure                                                |
| **Tools**        | `fetch_kyc_profile(entity_id)`, `get_transaction_history(account_id, days)`, `get_entity_details(entity_id)` — all backed by Azure Functions calling internal banking APIs |
| **Data Sources** | Core banking KYC system, transaction data warehouse, entity master                                                                                                         |
| **Output**       | Structured JSON with enriched entity profile and recent transaction summary appended to the shared thread                                                                  |

#### 5.2.3 Entity Resolution Agent

| Attribute      | Detail                                                                                                                              |
| -------------- | ----------------------------------------------------------------------------------------------------------------------------------- |
| **Role**       | Performs deep entity resolution — verifies whether the matched name is truly the sanctioned entity or a false positive              |
| **Techniques** | Multi-field comparison: name variants (transliteration, aliases), date of birth, nationality, passport/ID numbers, address matching |
| **Tools**      | `compare_entities(alert_entity, sanction_entity)`, `check_alias_database(name)`, `verify_nationality(entity_id)`                    |
| **Output**     | Entity match confidence score (0-100) with detailed field-by-field comparison matrix                                                |

#### 5.2.4 Historical Analysis Agent

| Attribute            | Detail                                                                                                                                                    |
| -------------------- | --------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Role**             | Searches for similar past alerts and their dispositions using RAG (Retrieval-Augmented Generation)                                                        |
| **Azure Connection** | Connects to **Azure AI Search** for vector/hybrid search over historical alert embeddings                                                                 |
| **RAG Pipeline**     | Query embedding (text-embedding-3-large via Azure OpenAI) → Vector search in Azure AI Search → Retrieve top-K similar alerts with their analyst decisions |
| **Tools**            | `search_similar_alerts(alert_features, top_k)`, `get_disposition_history(entity_name)`                                                                    |
| **Output**           | List of similar past alerts, their outcomes (confirmed/waived), and reasoning patterns                                                                    |

#### 5.2.5 Compliance Rules Agent

| Attribute          | Detail                                                                                                                                                |
| ------------------ | ----------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Role**           | Evaluates the alert against regulatory rules and business logic; determines if auto-disposition is possible                                           |
| **Tools**          | `evaluate_compliance_rules(alert_data)`, `check_whitelist(entity_id)`, `check_de_minimis_threshold(amount, currency)` — backed by **Azure Functions** |
| **Rules Examples** | De minimis thresholds, pre-approved whitelists, country-specific exemptions, SDN vs. non-SDN list priority                                            |
| **Output**         | Rule evaluation results — which rules passed/failed, whether auto-waive conditions are met                                                            |

#### 5.2.6 Risk Assessment Agent (Fan-in Aggregator)

| Attribute         | Detail                                                                                                                                     |
| ----------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| **Role**          | Aggregates outputs from all four specialist agents; computes a composite risk score; generates a human-readable recommendation             |
| **Pattern**       | **Fan-in** — waits for all specialist agents to complete, then synthesizes                                                                 |
| **Scoring Model** | Weighted composite: Entity Resolution confidence (40%) + Historical pattern (25%) + Compliance rules (20%) + Enrichment risk signals (15%) |
| **Output**        | Risk score (0-100), risk level (High/Medium/Low), recommended disposition (Confirm or Waive), and a narrative justification                |
| **Thread**        | Appends the final assessment to the shared thread for full traceability                                                                    |

### 5.3 Human-in-the-Loop: Bank Analyst Review Dashboard

| Attribute              | Detail                                                                                                                                         |
| ---------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------- |
| **Purpose**            | Presents the AI-generated triage package to the bank compliance analyst for final decision                                                     |
| **Contents Displayed** | Alert details, enriched entity data, entity match score, similar past alerts, rule evaluation results, composite risk score, AI recommendation |
| **Actions**            | **[ Confirm ]** — escalate as a true sanctions hit; **[ Waive ]** — dismiss as a false positive                                                |
| **Why Required**       | Regulatory mandates (OFAC, FCA, MAS) require human sign-off on sanctions decisions; AI provides decision support, not autonomous action        |
| **Technology**         | React/Angular SPA calling the Agent Service API through the API Gateway                                                                        |

### 5.4 Output & Actions Layer

| Component                     | Role                                                                                                                                                                                     |
| ----------------------------- | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| **Alert Disposition Service** | Persists the analyst's Confirm/Waive decision; updates the Sanctions Manager's Alert Management Service (dashed feedback arrow); triggers downstream workflows (SAR filing for confirms) |
| **Audit Trail**               | Immutable decision log capturing: all agent outputs, risk score, analyst identity, timestamp, reasoning — required for regulatory examination and model governance                       |
| **Feedback Loop**             | Analyst decisions are fed back into the knowledge base (Azure Blob Storage) and used to retrain/fine-tune embeddings, improving future RAG retrieval and risk scoring                    |

### 5.5 Shared Agent Thread (Conversation Memory)

- All agents operate on a **single shared thread** per alert triage session.
- The thread is a sequential conversation log stored in **Azure Cosmos DB** (via Agent Service's built-in thread management).
- Each specialist agent appends its findings to the thread; the Risk Assessment Agent reads the full thread to aggregate.
- This ensures **full context propagation** without direct agent-to-agent communication — agents communicate through the shared thread (blackboard pattern).

---

## 6. ZONE 4 — AZURE AI FOUNDRY PLATFORM SERVICES

All AI agent infrastructure runs on **Azure AI Foundry** — Microsoft's unified platform for building, deploying, and managing AI applications.

### 6.1 Service-by-Service Detail

| Azure Service                    | Role in Architecture                                                                                                             | Connection Points                                                                                                                          |
| -------------------------------- | -------------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------ |
| **Azure AI Agent Service**       | Managed runtime for all 6 agents; handles agent lifecycle, thread management, tool invocation routing, multi-agent orchestration | Triage Coordinator Agent registers here; all agents are managed instances                                                                  |
| **Azure OpenAI**                 | Provides the LLM backbone: **GPT-4o** for agent reasoning/planning, **text-embedding-3-large** for vector embeddings             | Every agent's LLM calls route through Azure OpenAI; Historical Analysis Agent uses embeddings                                              |
| **Azure AI Search**              | Vector store + hybrid search engine; stores embeddings of historical alerts and sanctions knowledge documents                    | Historical Analysis Agent queries this for similar past alerts (RAG retrieval)                                                             |
| **Azure Cosmos DB**              | Stores agent threads (conversation memory), session state, and agent configuration                                               | Agent Service persists all thread data here; shared thread is a Cosmos document                                                            |
| **Azure Blob Storage**           | Knowledge base repository — sanctions list documents, SOPs, compliance guidelines, past alert archives                           | Feedback Loop writes analyst decisions here; documents are chunked and indexed into AI Search                                              |
| **Azure Functions**              | Serverless compute hosting custom tool endpoints that agents call                                                                | Alert Enrichment Agent's KYC/transaction tools; Compliance Rules Agent's rule evaluation tools; Entity Resolution Agent's comparison tools |
| **Azure Monitor (App Insights)** | Observability — agent tracing, latency metrics, error rates, token usage tracking                                                | Audit Trail feeds here; all agent invocations are traced end-to-end                                                                        |
| **Microsoft Entra ID**           | Identity and access management — Managed Identity for service-to-service auth; RBAC for agent permissions                        | API Gateway authenticates via Entra ID; agents use Managed Identity to call Azure services                                                 |
| **Content Safety**               | Prompt Shields protecting against prompt injection; Responsible AI filters                                                       | All agent inputs/outputs pass through Content Safety checks                                                                                |
| **Evaluation & Tracing**         | Agent quality measurement — groundedness, relevance, coherence metrics; A/B testing of prompt variations                         | Used during development and production monitoring to ensure agent quality                                                                  |

### 6.2 Network & Security Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Azure Virtual Network                       │
│                                                                 │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────────────┐   │
│  │ Private      │    │ Azure AI     │    │ Azure OpenAI     │   │
│  │ Endpoints    │───▶│ Agent Service│───▶│ (Private Endpoint)│   │
│  └─────────────┘    └──────────────┘    └──────────────────┘   │
│         │                                                       │
│         │            ┌──────────────┐    ┌──────────────────┐   │
│         └───────────▶│ Azure AI     │    │ Azure Cosmos DB  │   │
│                      │ Search       │    │ (Private Endpoint)│   │
│                      └──────────────┘    └──────────────────┘   │
│                                                                 │
│  Authentication: Microsoft Entra ID (Managed Identity)          │
│  Network: Private Endpoints + NSGs + No public internet access  │
│  Encryption: TLS 1.3 in transit, AES-256 at rest, CMK support  │
└─────────────────────────────────────────────────────────────────┘
```

- **Private Endpoints**: All Azure services are accessed via Private Endpoints within a VNet — no public internet exposure.
- **Managed Identity**: Agents authenticate to Azure services using system-assigned Managed Identity — no secrets or connection strings in code.
- **RBAC**: Fine-grained role-based access control on every resource; principle of least privilege.
- **Data Encryption**: TLS 1.3 in transit; AES-256 at rest with Customer-Managed Keys (CMK) for regulated data.

### 6.3 Data Flow: Alert → AI Triage → Disposition

```
Step 1:  Matching Engine detects fuzzy match → creates alert in Alert Management Service
Step 2:  Alert Management Service publishes "New Alert" event
Step 3:  Triage Coordinator Agent receives the alert via webhook/queue trigger
Step 4:  Coordinator creates a new shared thread in Azure Cosmos DB (via Agent Service)
Step 5:  Coordinator delegates to 4 specialist agents IN PARALLEL (fan-out):
           ├── Alert Enrichment Agent   → calls Azure Functions (KYC, transactions)
           ├── Entity Resolution Agent  → calls Azure Functions (entity comparison)
           ├── Historical Analysis Agent → calls Azure AI Search (RAG vector search)
           └── Compliance Rules Agent   → calls Azure Functions (rule evaluation)
Step 6:  Each specialist writes findings to the shared thread
Step 7:  Risk Assessment Agent reads the full thread (fan-in), computes risk score
Step 8:  Risk Assessment Agent appends recommendation to thread
Step 9:  Bank Analyst Dashboard renders the triage package (evidence + recommendation)
Step 10: Analyst clicks [Confirm] or [Waive]
Step 11: Alert Disposition Service records the decision
Step 12: Audit Trail logs the full decision chain for compliance
Step 13: Feedback Loop stores the decision in Azure Blob Storage for future RAG improvement
Step 14: Disposition Result sent back to Sanctions Manager's Alert Management Service
```

---

## 7. MULTI-AGENT DESIGN PATTERNS EXPLAINED

### 7.1 Supervisor Pattern

- **What**: A single "boss" agent (Triage Coordinator) orchestrates the workflow. It does NOT perform analysis itself — it plans and delegates.
- **Why**: Provides a single control point for triage logic; easy to modify the triage plan without changing specialist agents; supports dynamic routing (e.g., skip Entity Resolution if alert is a known entity).
- **Azure Implementation**: The Coordinator is an Agent Service agent with `delegate_to_agent` tool definitions. It uses GPT-4o to reason about which specialists to invoke based on alert characteristics.

### 7.2 Fan-out Pattern

- **What**: The Coordinator dispatches tasks to multiple specialist agents simultaneously — they execute in parallel and independently.
- **Why**: Drastically reduces triage latency. Instead of sequential calls (30+ seconds), parallel execution completes in the time of the slowest agent (~8-10 seconds).
- **Azure Implementation**: Azure AI Agent Service supports concurrent agent invocations on the same thread. The Coordinator issues all four delegations, and the Agent Service manages parallel execution.

### 7.3 Fan-in Pattern

- **What**: The Risk Assessment Agent waits for all specialist agents to complete, then reads the full shared thread to aggregate evidence.
- **Why**: Combines diverse evidence types (KYC data, entity scores, historical patterns, rule evaluations) into a single coherent risk score and recommendation.
- **Azure Implementation**: The Coordinator invokes the Risk Assessment Agent only after all specialists have completed (tracked via thread message completion). The Risk Assessment Agent's system prompt instructs it to synthesize all thread messages.

### 7.4 Human-in-the-Loop Pattern

- **What**: The AI system provides a recommendation, but a human analyst makes the final decision.
- **Why**: Regulatory compliance requires human accountability for sanctions decisions. The AI accelerates review but does not replace the analyst.
- **Azure Implementation**: The Dashboard calls the Agent Service API to retrieve the thread contents and recommendation; the analyst's decision is captured via the API Gateway and persisted through the Disposition Service.

---

## 8. KEY TECHNICAL SPECIFICATIONS

| Dimension             | Specification                                                        |
| --------------------- | -------------------------------------------------------------------- |
| **Agent Framework**   | Azure AI Foundry Agent Service (managed, serverless)                 |
| **LLM Model**         | Azure OpenAI GPT-4o (reasoning), text-embedding-3-large (embeddings) |
| **Vector Store**      | Azure AI Search with HNSW index, 1536-dimension vectors              |
| **State Management**  | Azure Cosmos DB (thread persistence, session state)                  |
| **Tool Execution**    | Azure Functions (Python/C#) with HTTP triggers                       |
| **Authentication**    | Microsoft Entra ID, Managed Identity, RBAC                           |
| **Network**           | Azure VNet + Private Endpoints + NSGs                                |
| **Observability**     | Azure Monitor + Application Insights + OpenTelemetry tracing         |
| **Responsible AI**    | Azure Content Safety + Prompt Shields                                |
| **Throughput Target** | 500+ alerts triaged per hour (with 4-agent parallelism)              |
| **Avg Triage Time**   | ~12 seconds (AI processing) + analyst decision time                  |
| **Availability**      | 99.9% SLA (leveraging Azure AI Foundry SLA)                          |

---

## 9. WHY AZURE AI FOUNDRY (NOT SEMANTIC KERNEL)

| Criteria                | Azure AI Foundry Agent Service                       | Semantic Kernel                            |
| ----------------------- | ---------------------------------------------------- | ------------------------------------------ |
| **Deployment**          | Fully managed, serverless — no infra to manage       | Self-hosted, requires compute provisioning |
| **Multi-Agent**         | Native multi-agent orchestration with thread sharing | Manual orchestration via code              |
| **Thread Management**   | Built-in thread persistence (Cosmos DB backed)       | Must implement own conversation store      |
| **Tool Calling**        | Managed tool routing with automatic function calling | SDK-level plugin registration              |
| **Scaling**             | Auto-scales based on alert volume                    | Manual scaling of hosting infra            |
| **Enterprise Security** | Private endpoints, Managed Identity, RBAC built-in   | Must configure all security layers         |
| **Tracing**             | Built-in agent tracing and evaluation                | Requires custom telemetry                  |
| **Compliance**          | SOC 2, ISO 27001, GDPR compliant platform            | Depends on hosting environment             |

**Decision**: For a banking production system handling sanctions data, the **managed, enterprise-grade** nature of Azure AI Foundry Agent Service is the correct choice. It eliminates operational burden and provides compliance-ready infrastructure.

---

## 10. DEPLOYMENT & DEVOPS

```
┌─────────────┐     ┌──────────────┐     ┌────────────────┐     ┌──────────────┐
│  GitHub Repo │────▶│ GitHub       │────▶│ Bicep/Terraform│────▶│ Azure AI     │
│  (Agent Code)│     │ Actions CI/CD│     │ IaC Deployment │     │ Foundry      │
└─────────────┘     └──────────────┘     └────────────────┘     └──────────────┘
                                                                       │
                                                          ┌────────────┼────────────┐
                                                          ▼            ▼            ▼
                                                     Agent Service  AI Search   Cosmos DB
                                                     (Agents)       (Index)     (Threads)
```

- **Infrastructure as Code**: Bicep templates or Terraform for reproducible deployments
- **CI/CD**: GitHub Actions pipeline — lint → test → deploy agents → run evaluation suite
- **Agent Versioning**: Agent definitions (system prompts, tool configs) version-controlled in Git
- **Blue-Green Deployment**: New agent versions deployed alongside old ones; traffic gradually shifted

---

---

## 11. INTERVIEW QUESTIONS & ANSWERS (30)

---

### Q1: Can you explain the overall architecture of your Sanctions Monitoring system?

**A:** The architecture has four zones: (1) **External Data Sources** — batch files from a Landing Zone and real-time transactions from IBM MQ/Kafka; (2) **Sanctions Manager** — the traditional screening pipeline with a File Picker, Parser, High-Throughput Message Bus, Fuzzy Matching Engine, and Alert Management Service; (3) **Multi-Agent Alert Triage System** — six AI agents on Azure AI Foundry Agent Service that assist bank analysts in reviewing alerts using Supervisor, Fan-out, and Fan-in patterns with a Human-in-the-Loop; (4) **Azure AI Foundry Platform** — managed services including Azure OpenAI, AI Search, Cosmos DB, Functions, Monitor, Entra ID, and Content Safety.

---

### Q2: Why did you choose a multi-agent architecture instead of a single monolithic AI agent?

**A:** A single agent would face several problems: (1) **Context window limits** — cramming KYC data, entity resolution, historical search, and compliance rules into one prompt exceeds practical token limits and degrades reasoning. (2) **Latency** — sequential processing of all triage steps takes too long. (3) **Maintainability** — a monolithic prompt is hard to update when regulations change. Multi-agent allows **separation of concerns** (each agent is an expert), **parallel execution** (fan-out cuts latency by 4x), and **independent updates** (change the Compliance Rules Agent without touching others).

---

### Q3: Explain the Supervisor Pattern in your architecture.

**A:** The Triage Coordinator Agent acts as the supervisor. When it receives a new alert, it analyzes the alert metadata and plans which specialist agents to invoke. It doesn't perform analysis itself — it delegates. This mirrors a real compliance team where a senior analyst assigns sub-tasks to specialists. The Coordinator uses GPT-4o to reason about the triage plan and calls a `delegate_to_agent` tool to invoke each specialist. This pattern provides a single orchestration point and enables dynamic routing — for example, skipping the Entity Resolution Agent if the match is exact.

---

### Q4: How does the Fan-out pattern work, and what are the performance benefits?

**A:** After the Coordinator plans the triage, it dispatches tasks to four specialist agents **simultaneously**: Alert Enrichment, Entity Resolution, Historical Analysis, and Compliance Rules. Each agent runs independently and in parallel. The performance benefit is that instead of running sequentially (e.g., 4 agents × 8 seconds = 32 seconds), we complete in the time of the slowest agent (~10 seconds). This is critical when processing hundreds of alerts per hour.

---

### Q5: How do the agents communicate with each other?

**A:** Agents do NOT communicate directly with each other. They use a **Shared Agent Thread** — a sequential conversation log stored in Azure Cosmos DB via the Agent Service's built-in thread management. Each specialist appends its findings as a message to the thread. The Risk Assessment Agent (fan-in) then reads the complete thread to aggregate all evidence. This is a **blackboard pattern** — agents write to a shared workspace rather than sending messages to each other.

---

### Q6: What is the Shared Agent Thread, and how is it persisted?

**A:** The Shared Agent Thread is a conversation-style data structure managed by Azure AI Agent Service. For each alert triage session, a new thread is created. Each agent's input and output is appended as messages. The thread is persisted in **Azure Cosmos DB** automatically by the Agent Service — we don't manage the storage directly. The thread serves as both conversation memory and an audit trail of the AI reasoning process.

---

### Q7: Explain the Fan-in pattern and the Risk Assessment Agent.

**A:** The Fan-in pattern is the aggregation step. After all four specialist agents complete and write to the shared thread, the Risk Assessment Agent is invoked. It reads the entire thread containing outputs from all specialists and computes a weighted composite risk score: Entity Resolution confidence (40%), Historical patterns (25%), Compliance rules (20%), and Enrichment signals (15%). It then generates a recommendation (Confirm or Waive) with a narrative justification. This aggregation ensures no single signal dominates the decision.

---

### Q8: Why is Human-in-the-Loop necessary? Can't the AI auto-decide?

**A:** Regulatory frameworks (OFAC, FCA, MAS, FATF guidelines) **require human accountability** for sanctions decisions. A false negative (waiving a real sanctions hit) could result in billions in fines and criminal liability. The AI dramatically accelerates review — turning a 20-minute manual review into a 12-second AI package + quick analyst decision — but the bank analyst must make the final call. The AI provides decision _support_, not autonomous decisioning. Some low-risk alerts may qualify for **semi-automated waive** (analyst reviews in batch), but high-risk alerts always require individual review.

---

### Q9: How does the Historical Analysis Agent use RAG?

**A:** The Historical Analysis Agent implements Retrieval-Augmented Generation: (1) The alert's features (entity name, transaction type, country, amount) are embedded using **text-embedding-3-large** via Azure OpenAI. (2) The embedding is used to perform a **hybrid search** (vector + keyword) against an Azure AI Search index containing historical alert embeddings. (3) The top-K most similar past alerts are retrieved with their disposition history (confirmed/waived) and analyst reasoning. (4) The agent uses GPT-4o to synthesize these results into a pattern analysis — e.g., "7 similar alerts in the past 12 months, all waived due to common name match." This gives the analyst historical context.

---

### Q10: How are Azure Functions used as agent tools?

**A:** Each specialist agent has defined **tools** (function definitions in the Agent Service). These tools are backed by **Azure Functions** with HTTP triggers. For example, the Alert Enrichment Agent has a `fetch_kyc_profile(entity_id)` tool — when the agent decides to call this tool, the Agent Service routes the call to the corresponding Azure Function, which queries the internal KYC API and returns structured JSON. Azure Functions provide serverless, auto-scaling compute for these tools, and they're secured via Managed Identity.

---

### Q11: How does the Alert Management Service connect to the Triage Coordinator?

**A:** When the Matching Engine produces a hit and the Alert Management Service creates an alert, it publishes a "New Alert" event. The Triage Coordinator Agent receives this via a **webhook or queue trigger** (Azure Service Bus or Event Grid). The alert payload includes the matched entity name, sanctions list reference, transaction details, and match score. The Coordinator then creates a new thread and begins the triage workflow. The connection arrow in the architecture shows this as a thick blue "New Alert" edge from the Sanctions Manager to the Multi-Agent system.

---

### Q12: Explain the Disposition Result feedback loop back to the Sanctions Manager.

**A:** After the analyst clicks Confirm or Waive, the Alert Disposition Service records the decision and sends a "Disposition Result" message back to the Sanctions Manager's Alert Management Service (shown as a dashed gold arrow in the architecture). This updates the alert status in the sanctions system — marking it as confirmed (triggering SAR filing and escalation) or waived (closing the alert). This bidirectional integration ensures the sanctions system of record is always in sync with the AI triage decisions.

---

### Q13: How do you ensure data security for sanctions data?

**A:** Multiple layers: (1) **Network isolation** — all Azure services accessed via Private Endpoints within a VNet; no public internet exposure. (2) **Encryption** — TLS 1.3 in transit, AES-256 at rest with Customer-Managed Keys. (3) **Identity** — Microsoft Entra ID with Managed Identity; no secrets in code. (4) **RBAC** — fine-grained role assignments on every resource. (5) **Content Safety** — Azure Prompt Shields prevent prompt injection attacks. (6) **Data residency** — deploy in specific Azure regions to comply with data sovereignty requirements. (7) **Audit logging** — every agent invocation and decision is logged in Azure Monitor.

---

### Q14: What is Azure AI Foundry Agent Service, and why did you choose it over Semantic Kernel?

**A:** Azure AI Foundry Agent Service is a **fully managed, serverless runtime** for building and deploying AI agents. It handles agent lifecycle, thread management, tool invocation, multi-agent orchestration, and auto-scaling. We chose it over Semantic Kernel because: (1) **Managed infrastructure** — no servers to provision or scale. (2) **Built-in thread persistence** — thread state automatically stored in Cosmos DB. (3) **Enterprise security** — Private Endpoints, Managed Identity, RBAC out of the box. (4) **Native multi-agent support** — the service handles parallel agent execution and thread sharing. (5) **Compliance-ready** — SOC 2, ISO 27001 certified platform. For a banking production system, operational simplicity and compliance are paramount.

---

### Q15: How does the Matching Engine's fuzzy logic work?

**A:** The Matching Engine uses multiple algorithms in combination: **Jaro-Winkler** distance for name similarity, **Levenshtein** edit distance for typo detection, **Soundex/Metaphone** for phonetic matching (catches transliterations), and **token-based matching** for reordered names (e.g., "John Smith" vs. "Smith, John"). Each algorithm produces a score, and a composite threshold determines if a match is generated. The engine also handles aliases, transliterations from non-Latin scripts, and common name variations. Thresholds are tunable per sanctions list and entity type.

---

### Q16: How do you handle high alert volumes?

**A:** At multiple levels: (1) **Sanctions Manager** — the Parser uses parallel scrubber pods that scale horizontally; the Message Bus provides backpressure handling. (2) **Multi-Agent System** — the Fan-out pattern provides 4x parallelism per alert; Azure AI Agent Service auto-scales agent instances based on queue depth. (3) **Azure Functions** — serverless auto-scaling for tool endpoints. (4) **Azure OpenAI** — provisioned throughput units (PTUs) ensure consistent LLM performance under load. (5) **Batching** — low-risk alerts can be batched for analyst review, while high-risk alerts get priority routing. Target: 500+ alerts triaged per hour.

---

### Q17: How is the audit trail implemented for regulatory compliance?

**A:** The Audit Trail captures every step in an **immutable log**: (1) Raw alert data from the Matching Engine. (2) Each specialist agent's output (enrichment data, entity match score, historical matches, rule evaluations). (3) The Risk Assessment Agent's composite score and recommendation. (4) The analyst's identity (from Entra ID), their decision (Confirm/Waive), and timestamp. (5) The full shared thread conversation. This is stored in Azure Monitor (App Insights) for queryability and also archived to Azure Blob Storage with immutable blob policies for long-term regulatory retention (typically 7 years).

---

### Q18: Explain the Compliance Rules Agent in more detail.

**A:** This agent evaluates the alert against a codified set of regulatory and business rules: (1) **De minimis thresholds** — if the transaction amount is below a configurable threshold, it may qualify for auto-waive. (2) **Whitelists** — pre-approved entities that have been verified and cleared by compliance. (3) **Country-specific exemptions** — certain countries may have general license exemptions. (4) **List priority** — an OFAC SDN match is treated differently from a less restrictive list like the Consolidated Screening List. (5) **Dormant account rules** — alerts on accounts with no activity may have different treatment. The rules are stored in configuration (Azure Functions) and version-controlled, making them auditable and changeable without redeploying the agent.

---

### Q19: How does the Entity Resolution Agent differ from the Matching Engine?

**A:** The Matching Engine produces the **initial hit** using fast, high-recall fuzzy matching — it intentionally over-generates matches to avoid missing true positives. The Entity Resolution Agent performs **deep, high-precision verification** on those hits: it compares multiple fields (name variants, DOB, nationality, passport numbers, addresses) between the alert entity and the exact sanctioned individual. While the Matching Engine might flag "Mohammad Khan" as a potential match to a sanctions entry, the Entity Resolution Agent verifies whether this specific Mohammad Khan (DOB, nationality, etc.) is actually the sanctioned person.

---

### Q20: How do you measure agent quality in production?

**A:** Using Azure AI Foundry's **Evaluation & Tracing** capabilities: (1) **Groundedness** — are agent responses grounded in the retrieved data (not hallucinated)? (2) **Relevance** — is the agent's output relevant to the alert being triaged? (3) **Coherence** — is the narrative explanation logically sound? (4) **Accuracy** — comparing AI recommendations against analyst decisions to measure agreement rate. (5) **Latency** — p50/p95/p99 response times per agent. (6) **Token usage** — cost optimization per triage session. We run evaluation suites in CI/CD and monitor production metrics via Azure Monitor dashboards.

---

### Q21: What happens if one specialist agent fails during fan-out?

**A:** The system has fault tolerance: (1) **Retry logic** — Agent Service automatically retries transient failures (HTTP 429, 503). (2) **Timeout handling** — each agent has a configurable timeout (e.g., 30 seconds); if exceeded, the Coordinator receives a timeout signal. (3) **Graceful degradation** — the Risk Assessment Agent can still compute a risk score with partial evidence (e.g., 3 out of 4 agents responded), but with a lower confidence indicator. (4) **Fallback** — if a critical agent (e.g., Entity Resolution) fails, the alert is flagged for manual-only review with a note that AI triage was incomplete. (5) All failures are logged in Azure Monitor for alerting and investigation.

---

### Q22: How is the knowledge base for RAG maintained?

**A:** The knowledge base in Azure Blob Storage contains: historical alert data, sanctions list documents, compliance SOPs, and analyst decision rationales. It's maintained through: (1) **Automated ingestion** — the Feedback Loop continuously writes new analyst decisions. (2) **Batch updates** — sanctions list updates from the 3rd-party provider are chunked, embedded (text-embedding-3-large), and indexed into Azure AI Search. (3) **Manual curation** — compliance team can upload updated SOPs and guidelines. (4) **Versioning** — Blob Storage versioning tracks document changes. (5) **Reindexing** — an Azure Function periodically triggers re-embedding and re-indexing of updated documents.

---

### Q23: How do you handle prompt injection attacks in this system?

**A:** Azure AI Foundry provides **Content Safety** with **Prompt Shields**: (1) All agent inputs are scanned for injection patterns before reaching GPT-4o. (2) System prompts are protected — user/alert data is passed as structured tool parameters, not concatenated into prompts. (3) Output filtering prevents the agent from leaking system prompt contents. (4) Rate limiting at the API Gateway prevents abuse. (5) Agent tools have strict input validation — even if a prompt injection causes the agent to call a tool with malicious parameters, the Azure Function validates inputs independently. Defense in depth.

---

### Q24: What is the cost model for this architecture?

**A:** Costs break down into: (1) **Azure OpenAI** — per-token pricing for GPT-4o (reasoning) and embedding model; this is the largest variable cost; with ~6 agent calls per alert and ~4K tokens each, roughly $0.02-0.05 per alert triage. (2) **Azure AI Agent Service** — pricing based on agent invocations and thread storage. (3) **Azure AI Search** — based on index size and query volume; Standard S1 tier for production. (4) **Azure Cosmos DB** — RU/s consumption for thread read/writes. (5) **Azure Functions** — consumption plan, near-zero cost at moderate volumes. (6) **Azure Monitor** — log ingestion and retention. Estimated total: **$0.05-0.10 per alert triage** at scale, which is drastically less than the $15-25 cost of a fully manual analyst review.

---

### Q25: How do you ensure consistency between agents?

**A:** Three mechanisms: (1) **Shared Thread** — all agents read from and write to the same thread, ensuring a single source of truth per alert. (2) **Structured Output Schemas** — each agent's tool outputs are defined with JSON schemas, ensuring consistent data formats downstream. (3) **System Prompt Governance** — all agent system prompts are version-controlled in Git, reviewed via PR, and tested in the evaluation pipeline before deployment. The Coordinator's system prompt explicitly defines the expected output format for each specialist.

---

### Q26: Describe the end-to-end latency breakdown.

**A:** For a single alert triage:

- Alert delivery (MQ/Kafka → Agent Service): ~200ms
- Coordinator planning: ~1.5s (GPT-4o reasoning)
- Fan-out parallel execution (slowest agent wins):
  - Alert Enrichment: ~3s (Azure Function + internal API call)
  - Entity Resolution: ~2s (Azure Function computation)
  - Historical Analysis: ~4s (embedding + AI Search query + GPT-4o synthesis)
  - Compliance Rules: ~1.5s (Azure Function rule evaluation)
- Fan-in aggregation (Risk Assessment): ~3s (GPT-4o synthesis)
- Dashboard rendering: ~500ms
- **Total AI processing: ~9-12 seconds**
- Analyst review and decision: variable (30s to 5 min depending on complexity)

---

### Q27: How does this architecture handle different sanctions lists?

**A:** The Sanction Ingestion Service normalizes data from multiple list providers (OFAC SDN, EU Consolidated, UN, HMT, etc.) into a canonical schema in the Sanction Master DB. Each match record in an alert carries the **list source identifier**. The Compliance Rules Agent has list-specific rules — for example, OFAC Primary sanctions require stricter treatment than non-SDN lists. The Historical Analysis Agent's vector search is also list-aware, returning similar alerts from the same sanctions list category. This list-aware design ensures regulatory jurisdiction is respected.

---

### Q28: How would you scale this system for a Tier-1 global bank?

**A:** For a bank processing 50,000+ alerts/day: (1) **Azure OpenAI PTUs** — provisioned throughput units for guaranteed LLM capacity; multiple PTU deployments across regions. (2) **Multi-region deployment** — deploy Agent Service and supporting services in multiple Azure regions for latency and compliance. (3) **Cosmos DB partitioning** — partition threads by alert ID for horizontal scalability. (4) **AI Search replicas** — add search replicas for read throughput. (5) **Alert prioritization** — implement a priority queue (high-risk alerts first; low-risk batched). (6) **Caching** — Redis cache for repeated entity lookups and rule evaluations. (7) **Parallel triage sessions** — the Agent Service handles hundreds of concurrent triage sessions.

---

### Q29: How do you version and test agent prompts in production?

**A:** (1) All system prompts, tool definitions, and agent configurations are stored in **Git** alongside application code. (2) Changes go through **PR review** by both engineering and compliance teams. (3) The CI/CD pipeline runs an **evaluation suite** — a set of test alerts with known-good dispositions — to measure recommendation accuracy, groundedness, and coherence before deploying. (4) **Canary deployments** — new agent versions are deployed alongside the current version; a small percentage of alerts are routed to the new version. (5) **A/B metrics** — compare quality metrics between versions in Azure Monitor before full rollout. (6) **Rollback** — if metrics degrade, immediately rollback to the previous agent version.

---

### Q30: What are the key risks and mitigations for this AI-assisted sanctions system?

**A:** Key risks and mitigations:

| Risk                                                                         | Mitigation                                                                                                       |
| ---------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| **Hallucination** — AI fabricates evidence                                   | Agents only output data retrieved from tools (Azure Functions, AI Search); groundedness evaluation in production |
| **False sense of security** — analyst blindly trusts AI                      | Training program; UI shows confidence intervals; high-risk alerts have mandatory manual review steps             |
| **Regulatory rejection** — regulator doesn't accept AI in sanctions workflow | Human-in-the-Loop ensures human accountability; full audit trail; AI is decision _support_, not decisioning      |
| **Prompt injection** — adversary manipulates alert data to influence AI      | Content Safety + Prompt Shields; structured tool inputs; input validation in Azure Functions                     |
| **Model degradation** — LLM performance changes after update                 | Evaluation suite runs on every deployment; GPT-4o model version pinning; monitoring quality metrics              |
| **Data leakage** — sanctions data exposed                                    | Private Endpoints, VNet isolation, Managed Identity, encryption at rest/transit, no public endpoints             |
| **Cost overrun** — LLM token costs spiral                                    | PTU provisioning for predictable costs; prompt optimization; token usage monitoring and alerts                   |
| **Single point of failure** — Coordinator agent down                         | Agent Service provides built-in redundancy; health checks; dead-letter queue for unprocessed alerts              |

---

_Document prepared for Senior Cloud Architect interview demo — Sanctions Multi-Agent Architecture on Azure AI Foundry._
