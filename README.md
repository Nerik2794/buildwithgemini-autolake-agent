# AutoLake Diagnostic Copilot 🚛⚡

> Connected vehicle fleet operations & telemetry diagnostic copilot built with Google Agent Development Kit (ADK), Gemini, and Vertex AI Agent Platform.

![AutoLake Agent Demo](demo.gif)

---

## 📌 Overview

**AutoLake Diagnostic Copilot** monitors connected vehicle health, audits Diagnostic Trouble Codes (DTC), triggers automated telemetry data pipelines, runs Python code execution for statistical calculations, and generates visual telemetry assets rendered directly as interactive **A2UI** cards.

---

## 🛠️ Implemented Capabilities & Google Cloud Services

The agent is implemented in Python using the Google Agent Development Kit (ADK) and deployed to **Vertex AI Agent Runtime** over the **A2A (Agent-to-Agent)** protocol:

| Capability / Google Cloud Service | Implementation Details |
| :--- | :--- |
| **Vertex AI Memory Bank** | Cross-session long-term memory via `VertexAiMemoryBankService` and `PreloadMemoryTool`. Persists operator preferences across sessions (assigned fleet, role, alert thresholds, and preferred units). |
| **Cloud Firestore** | Real-time vehicle fleet telemetry datastore. Records and audits VIN records, mileage, battery health percentage, location, and active DTC fault codes. |
| **Cloud Storage (GCS)** | Public storage bucket storing generated telemetry health charts and fleet vehicle visualization assets. |
| **Imagen / Gemini Image Gen** | Item image generation using `gemini-3.1-flash-lite-image` in global region via Google GenAI SDK (`generate_fleet_visual`). Saves to both ADK tool artifacts and Cloud Storage. |
| **A2UI (v0.8 Basic Catalog)** | Dynamic UI card emission via `a2ui_callback` after-model hook. Renders structured fleet summaries, diagnostic details, and embedded telemetry charts natively. |
| **Code Execution Sandbox** | Secure serverless Python sandbox execution powered by `AgentEngineSandboxCodeExecutor` on Agent Platform for mathematical, statistical, and telemetry calculations. |
| **Open-Meteo Weather API** | Keyless real-time weather integration assessing fleet thermal risk (extreme battery heat degradation or freezing autonomy drops). |

---

## 🧰 Agent Tools Reference

All tools are declared in `app/agent.py`:

- **`list_vehicles(fleet, status)`**: Queries Firestore to filter fleet vehicles by operational status (`active`, `warning`, `maintenance`, `inspection_requested`) or fleet name (`Midwest-Freight`, `WestCoast-Delivery`).
- **`get_vehicle_telemetry(vin)`**: Fetches complete vehicle telemetry by VIN, including battery health, DTC codes, odometer, and last update timestamp.
- **`record_vehicle_telemetry(vin, status, ...)`**: Updates vehicle status, telemetry readings, and operational notes in Cloud Firestore.
- **`trigger_diagnostic_pipeline(vin, reason, priority)`**: Dispatches an emergency high-frequency data extraction pipeline, flagging the vehicle for inspection.
- **`generate_vehicle_health_chart(vin)`**: Generates a health gauge and odometer chart using `matplotlib`, uploads it to Cloud Storage, and returns the public URL.
- **`get_weather_risk(city)`**: Checks current ambient temperature and evaluates thermal operational risk for EV/connected fleets.
- **`generate_fleet_visual(prompt)`**: Generates fleet vehicle models or technical component diagrams with `gemini-3.1-flash-lite-image`.
- **`PreloadMemoryTool` & `generate_memories_callback`**: Preloads and updates operator preferences in the Vertex AI Memory Bank.

> **Note on planned vs. implemented features**: BigQuery analytics tables and Cloud Composer / Dataproc clusters were envisioned in early planning briefs; current implementation leverages direct Cloud Firestore NoSQL storage, Cloud Storage asset hosting, and lightweight Python sandbox execution.

---

## 🚀 Local Setup & Execution

### 1. Prerequisites
- Python 3.11+
- `uv` package manager
- Google Cloud SDK (`gcloud`) authenticated with Application Default Credentials (ADC)

```bash
gcloud auth login
gcloud auth application-default login
gcloud config set project <YOUR_PROJECT_ID>
```

### 2. Install Dependencies
```bash
cd autolake-agent
uv sync
```

### 3. Run Local ADK Web Playground
To test the agent locally with Memory Bank support:

```bash
uv run adk web . --port 8080 --reload_agents --memory_service_uri=agentengine://<YOUR_AGENT_ENGINE_ID>
```

### 4. Run the Chat Web Frontend
To run the included FastAPI proxy and A2UI chat interface locally:

```bash
cd frontend
export AGENT_ENGINE_RESOURCE_NAME="projects/<PROJECT_NUMBER>/locations/<REGION>/reasoningEngines/<ENGINE_ID>"
export AGENT_DIRECTORY="app"
uvicorn main:app --host 0.0.0.0 --port 8081
```

---

## ☁️ Deployment

### Deploy Agent to Vertex AI Agent Runtime
```bash
agents-cli deploy --project <YOUR_PROJECT_ID> --region us-east1
```

### Deploy Frontend to Cloud Run
```bash
gcloud run deploy autolake-frontend \
  --source frontend \
  --project <YOUR_PROJECT_ID> \
  --region us-east1 \
  --allow-unauthenticated \
  --set-env-vars AGENT_ENGINE_RESOURCE_NAME="projects/<PROJECT_NUMBER>/locations/<REGION>/reasoningEngines/<ENGINE_ID>",AGENT_DIRECTORY="app"
```
