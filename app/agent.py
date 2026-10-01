# ruff: noqa
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import datetime
import io
import json
from typing import Any, Dict, List, Optional
import urllib.parse
import urllib.request
from google import genai
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
try:
    from a2ui.basic_catalog.provider import BasicCatalog
    from a2ui.schema.manager import A2uiSchemaManager
    HAS_A2UI_SDK = True
except ImportError:
    HAS_A2UI_SDK = False
from google.adk.agents import Agent
from google.adk.agents.callback_context import CallbackContext
from google.adk.apps import App
from google.adk.code_executors import AgentEngineSandboxCodeExecutor
from google.adk.models import Gemini
from google.adk.tools.preload_memory_tool import PreloadMemoryTool
from google.adk.tools.tool_context import ToolContext
from google.cloud import firestore
from google.cloud import storage
from google.genai import types

from app.a2ui_utils import a2ui_callback

MODEL = "gemini-3.6-flash"
IMAGE_MODEL = "gemini-3.1-flash-lite-image"

# Hardcoded project ID as string so deployed Agent Runtime doesn't resolve to project number
FIRESTORE_PROJECT = "qwiklabs-gcp-04-3cb39bb4f24f"
GCS_BUCKET_NAME = "autolake-telemetry-media-qwiklabs-gcp-04-3cb39bb4f24f"
AGENT_ENGINE_RESOURCE_NAME = "projects/685694541535/locations/us-east1/reasoningEngines/9124712858494435328"
AGENT_ENGINE_ID = "9124712858494435328"
MEMORY_BANK_ID = AGENT_ENGINE_ID
SANDBOX_RESOURCE_NAME = "projects/685694541535/locations/us-east1/reasoningEngines/9124712858494435328/sandboxEnvironments/7015983696838656000"
_db_client: Optional[firestore.Client] = None
_storage_client: Optional[storage.Client] = None
_genai_client: Optional[genai.Client] = None


async def generate_memories_callback(callback_context: CallbackContext) -> None:
    """Callback to extract and persist operator preferences into Memory Bank across sessions."""
    await callback_context.add_session_to_memory()
    return None


def _get_db() -> firestore.Client:
    global _db_client
    if _db_client is None:
        _db_client = firestore.Client(project=FIRESTORE_PROJECT)
    return _db_client


def list_vehicles(fleet: Optional[str] = None, status: Optional[str] = None) -> List[Dict[str, Any]]:
    """Lists vehicles from the Firestore vehicle telemetry collection with optional filters.

    Args:
        fleet: Optional fleet name to filter by (e.g. 'WestCoast-Delivery', 'Midwest-Freight').
        status: Optional status to filter by (e.g. 'active', 'warning', 'maintenance').

    Returns:
        A list of vehicle records with their telemetry metadata and status.
    """
    db = _get_db()
    col = db.collection("vehicles")
    query = col

    if fleet:
        query = query.where("fleet", "==", fleet)
    if status:
        query = query.where("status", "==", status)

    docs = query.stream()
    results = []
    for doc in docs:
        data = doc.to_dict()
        data["id"] = doc.id
        results.append(data)
    return results


def get_vehicle_telemetry(vin: str) -> Dict[str, Any]:
    """Retrieves detailed telemetry and health records for a specific vehicle by VIN.

    Args:
        vin: The Vehicle Identification Number (VIN) to look up.

    Returns:
        The vehicle record including battery health, DTC trouble codes, and last telemetry update.
    """
    db = _get_db()
    doc_ref = db.collection("vehicles").document(vin)
    doc = doc_ref.get()
    if not doc.exists:
        return {"error": f"Vehicle with VIN '{vin}' not found in Firestore."}
    data = doc.to_dict()
    data["id"] = doc.id
    return data


def record_vehicle_telemetry(
    vin: str,
    status: Optional[str] = None,
    battery_health_pct: Optional[float] = None,
    mileage_km: Optional[float] = None,
    dtc_fault_codes: Optional[List[str]] = None,
    notes: Optional[str] = None,
) -> Dict[str, Any]:
    """Updates or adds telemetry information for a vehicle in Firestore.

    Args:
        vin: The Vehicle Identification Number (VIN) to update.
        status: Updated status (e.g. 'active', 'warning', 'maintenance').
        battery_health_pct: Updated battery health percentage.
        mileage_km: Updated mileage in kilometers.
        dtc_fault_codes: Diagnostic trouble codes list (e.g. ['P0A80']).
        notes: Operational or pipeline incident notes.

    Returns:
        Status confirmation of the update.
    """
    db = _get_db()
    doc_ref = db.collection("vehicles").document(vin)

    update_payload: Dict[str, Any] = {
        "last_telemetry_timestamp": datetime.datetime.now(datetime.timezone.utc).isoformat()
    }
    if status is not None:
        update_payload["status"] = status
    if battery_health_pct is not None:
        update_payload["battery_health_pct"] = battery_health_pct
    if mileage_km is not None:
        update_payload["mileage_km"] = mileage_km
    if dtc_fault_codes is not None:
        update_payload["dtc_fault_codes"] = dtc_fault_codes
    if notes is not None:
        update_payload["notes"] = notes

    doc_ref.set(update_payload, merge=True)
    return {"success": True, "vin": vin, "updated_fields": update_payload}


def trigger_diagnostic_pipeline(
    vin: str,
    reason: str,
    priority: str = "high",
) -> Dict[str, Any]:
    """Triggers an on-demand high-frequency telemetry extraction pipeline for a vehicle and flags it for inspection.

    Args:
        vin: The Vehicle Identification Number (VIN) to triage.
        reason: Justification for triggering the diagnostic run (e.g. DTC warning code, battery drop).
        priority: Dispatch priority level ('low', 'medium', 'high', 'critical').

    Returns:
        Details of the triggered Composer Airflow DAG run, dispatch ID, and updated Firestore status.
    """
    db = _get_db()
    doc_ref = db.collection("vehicles").document(vin)
    doc = doc_ref.get()
    if not doc.exists:
        return {"error": f"Vehicle with VIN '{vin}' not found in Firestore."}

    vehicle_data = doc.to_dict() or {}
    dispatch_timestamp = datetime.datetime.now(datetime.timezone.utc).isoformat()
    dag_run_id = f"diag_{vin[-6:]}_{int(datetime.datetime.now().timestamp())}"

    # Update vehicle operational status in Firestore
    doc_ref.set(
        {
            "status": "inspection_requested",
            "last_diagnostic_dispatch": {
                "dag_run_id": dag_run_id,
                "dispatched_at": dispatch_timestamp,
                "reason": reason,
                "priority": priority,
            },
            "notes": f"Diagnostic run {dag_run_id} requested: {reason}",
            "last_telemetry_timestamp": dispatch_timestamp,
        },
        merge=True,
    )

    return {
        "success": True,
        "dag_run_id": dag_run_id,
        "dag_name": "vehicle_high_frequency_telemetry_extract",
        "orchestrator": "Cloud Composer (Airflow)",
        "target_vin": vin,
        "model": vehicle_data.get("model", "Unknown"),
        "fleet": vehicle_data.get("fleet", "Unknown"),
        "priority": priority,
        "reason": reason,
        "dispatched_at": dispatch_timestamp,
        "new_status": "inspection_requested",
        "message": f"Successfully triggered Airflow DAG run {dag_run_id} for VIN {vin}. Vehicle status updated to 'inspection_requested'."
    }


def _get_storage() -> storage.Client:
    global _storage_client
    if _storage_client is None:
        _storage_client = storage.Client(project=FIRESTORE_PROJECT)
    return _storage_client


def generate_vehicle_health_chart(vin: str) -> str:
    """Generates a visual health chart (battery health gauge bar and odometer) for a vehicle and uploads it to GCS.

    Args:
        vin: The Vehicle Identification Number (VIN) to generate the chart for.

    Returns:
        A Markdown-formatted image string pointing to the public Cloud Storage URL.
    """
    db = _get_db()
    doc_ref = db.collection("vehicles").document(vin)
    doc = doc_ref.get()
    if not doc.exists:
        return f"Error: No se encontró telemetría para el vehículo con VIN `{vin}`."

    data = doc.to_dict() or {}
    model = data.get("model", "Vehicle")
    fleet = data.get("fleet", "Fleet")
    battery_pct = float(data.get("battery_health_pct", 100.0))
    mileage_km = float(data.get("mileage_km", 0.0))
    status = data.get("status", "unknown").upper()
    dtc_codes = data.get("dtc_fault_codes", [])

    # Color mapping according to battery health and status
    if battery_pct >= 90:
        bar_color = "#2ecc71"  # Emerald green
    elif battery_pct >= 80:
        bar_color = "#f39c12"  # Amber warning
    else:
        bar_color = "#e74c3c"  # Coral red

    fig, ax = plt.subplots(figsize=(7, 3.2), dpi=150)
    fig.patch.set_facecolor("#1e272e")
    ax.set_facecolor("#2f3640")

    # Horizontal battery gauge bar
    ax.barh(["Battery Health"], [100], color="#57606f", height=0.45, alpha=0.5)
    ax.barh(["Battery Health"], [battery_pct], color=bar_color, height=0.45)

    ax.set_xlim(0, 100)
    ax.set_xlabel("Salud de Batería (%)", color="#f5f6fa", fontsize=11, fontweight="bold")
    ax.tick_params(colors="#f5f6fa", labelsize=10)
    for spine in ax.spines.values():
        spine.set_color("#718093")

    # Value text on bar
    ax.text(
        battery_pct / 2 if battery_pct > 20 else 5,
        0,
        f"{battery_pct:.1f}%",
        va="center",
        ha="center",
        color="#ffffff",
        fontsize=13,
        fontweight="bold",
    )

    # Title and vehicle stats
    dtc_text = ", ".join(dtc_codes) if dtc_codes else "None"
    fig.suptitle(f"AutoLake Telemetry: {model} ({vin})", color="#00d2d3", fontsize=13, fontweight="bold", y=0.98)
    subtitle = f"Flota: {fleet}  |  Estado: {status}  |  Odómetro: {mileage_km:,.1f} km  |  DTC: {dtc_text}"
    ax.set_title(subtitle, color="#dcdde1", fontsize=9, pad=10)

    plt.tight_layout()

    # Save to in-memory buffer
    img_buffer = io.BytesIO()
    plt.savefig(img_buffer, format="png", bbox_inches="tight", facecolor=fig.get_facecolor())
    plt.close(fig)
    img_buffer.seek(0)

    # Upload to Cloud Storage
    client = _get_storage()
    bucket = client.bucket(GCS_BUCKET_NAME)
    blob_name = f"charts/{vin}_{int(datetime.datetime.now().timestamp())}.png"
    blob = bucket.blob(blob_name)
    blob.upload_from_file(img_buffer, content_type="image/png")

    public_url = f"https://storage.googleapis.com/{GCS_BUCKET_NAME}/{blob_name}"
    return f"![AutoLake Health Chart: {vin}]({public_url})\n\n**Gráfico de Salud Generado:** [{blob_name}]({public_url})"


WMO_WEATHER_CODES: Dict[int, str] = {
    0: "Cielo despejado",
    1: "Principalmente despejado",
    2: "Parcialmente nublado",
    3: "Nublado",
    45: "Niebla",
    48: "Niebla con escarcha",
    51: "Llovizna ligera",
    53: "Llovizna moderada",
    55: "Llovizna densa",
    61: "Lluvia ligera",
    63: "Lluvia moderada",
    65: "Lluvia fuerte",
    71: "Nevada ligera",
    73: "Nevada moderada",
    75: "Nevada fuerte",
    80: "Chubascos ligeros",
    81: "Chubascos moderados",
    82: "Chubascos violentos",
    95: "Tormenta eléctrica",
    96: "Tormenta eléctrica con granizo ligero",
    99: "Tormenta eléctrica con granizo fuerte",
}


def get_weather_risk(city: str) -> Dict[str, Any]:
    """Obtains current weather conditions and assesses thermal risk for EV/connected vehicle fleet operations.

    Args:
        city: City name to check (e.g., 'Monterrey', 'Ciudad de México', 'Guadalajara', 'Hermosillo'). Defaults to Mexico if no country specified.

    Returns:
        A dictionary containing temperature (°C), weather condition, thermal risk level, and operational fleet advice.
    """
    clean_city = city.strip()
    search_query = clean_city if ("," in clean_city or "méxico" in clean_city.lower() or "mexico" in clean_city.lower()) else f"{clean_city}, Mexico"
    encoded_query = urllib.parse.quote(search_query)

    # 1. Geocoding via Open-Meteo
    geocoding_url = f"https://geocoding-api.open-meteo.com/v1/search?name={encoded_query}&count=5&language=es&format=json"
    headers = {"User-Agent": "AutoLake-Diagnostic-Copilot/1.0"}

    try:
        req = urllib.request.Request(geocoding_url, headers=headers)
        with urllib.request.urlopen(req, timeout=5) as response:
            geo_data = json.loads(response.read().decode())
    except Exception as exc:
        return {"error": f"Error al consultar el servicio de geocodificación de Open-Meteo: {exc}"}

    results = geo_data.get("results")
    if not results:
        # Fallback without Mexico suffix
        fallback_query = urllib.parse.quote(clean_city)
        fallback_url = f"https://geocoding-api.open-meteo.com/v1/search?name={fallback_query}&count=5&language=es&format=json"
        try:
            req = urllib.request.Request(fallback_url, headers=headers)
            with urllib.request.urlopen(req, timeout=5) as response:
                fallback_data = json.loads(response.read().decode())
                results = fallback_data.get("results")
        except Exception:
            results = None

    if not results:
        return {"error": f"No se pudo encontrar la ubicación geográfica para la ciudad: '{city}'."}

    # Best match selection (prioritize Mexico if country not explicitly specified)
    selected_loc = results[0]
    for candidate in results:
        country = (candidate.get("country") or "").lower()
        if "mex" in country:
            selected_loc = candidate
            break

    lat = selected_loc.get("latitude")
    lon = selected_loc.get("longitude")
    resolved_name = selected_loc.get("name")
    resolved_admin1 = selected_loc.get("admin1", "")
    resolved_country = selected_loc.get("country", "")

    # 2. Weather forecast API via Open-Meteo
    forecast_url = (
        f"https://api.open-meteo.com/v1/forecast?latitude={lat}&longitude={lon}"
        "&current=temperature_2m,weather_code&timezone=auto"
    )

    try:
        req = urllib.request.Request(forecast_url, headers=headers)
        with urllib.request.urlopen(req, timeout=5) as response:
            forecast_data = json.loads(response.read().decode())
    except Exception as exc:
        return {"error": f"Error al consultar el pronóstico del clima de Open-Meteo: {exc}"}

    current = forecast_data.get("current", {})
    temp_c = current.get("temperature_2m")
    weather_code = current.get("weather_code", 0)
    condition_desc = WMO_WEATHER_CODES.get(weather_code, f"Condición WMO código {weather_code}")

    # 3. Fleet thermal risk diagnostic
    if temp_c is None:
        return {"error": f"No se pudo obtener la temperatura actual para {resolved_name}."}

    if temp_c > 35.0:
        risk_level = "ALTO_CALOR_EXTREMO"
        alert_flag = "🚨 ALERTA CRÍTICA: Calor Extremo"
        risk_explanation = (
            f"Temperatura de {temp_c:.1f}°C supera el límite de seguridad térmica (35°C). "
            "Riesgo crítico de sobrecalentamiento del BMS, degradación acelerada de celdas de batería de litio "
            "y activación forzada del sistema de refrigeración de alta potencia."
        )
        fleet_recommendation = (
            "Limitar ciclos de carga rápida DC (CCS/NACS) en las unidades. "
            "Revisar alertas DTC de temperatura en celdas (ej. P0A1F) y monitorear la presión de refrigerante."
        )
    elif temp_c < 0.0:
        risk_level = "ALTO_CONGELAMIENTO"
        alert_flag = "❄️ ALERTA CRÍTICA: Bajas Temperaturas / Congelamiento"
        risk_explanation = (
            f"Temperatura de {temp_c:.1f}°C bajo cero. "
            "Riesgo severo de congelamiento en líneas de fluidos, incremento de resistencia interna en celdas "
            "y caída de autonomía útil estimada entre un 25% y 40%."
        )
        fleet_recommendation = (
            "Habilitar precalentamiento de batería conectado a la red antes de despacho. "
            "Aumentar margen de reserva de kilometraje en la planificación de rutas."
        )
    elif temp_c >= 30.0:
        risk_level = "MODERADO_CALOR"
        alert_flag = "⚠️ PRECAUCIÓN: Temperatura Elevada"
        risk_explanation = f"Temperatura de {temp_c:.1f}°C. Consumo adicional de energía por climatización de cabina y pack."
        fleet_recommendation = "Monitorear estado térmico preventivo en rutas largas."
    else:
        risk_level = "NORMAL_OPTIMO"
        alert_flag = "✅ CONDICIONES ÓPTIMAS"
        risk_explanation = f"Temperatura de {temp_c:.1f}°C dentro del rango térmico óptimo de operación (15°C - 28°C)."
        fleet_recommendation = "Operación estándar de flota sin restricciones térmicas."

    return {
        "city": resolved_name,
        "region": resolved_admin1,
        "country": resolved_country,
        "latitude": lat,
        "longitude": lon,
        "temperature_c": temp_c,
        "condition": condition_desc,
        "weather_code": weather_code,
        "risk_level": risk_level,
        "alert": alert_flag,
        "risk_diagnostic": risk_explanation,
        "fleet_recommendation": fleet_recommendation,
    }


def _get_genai() -> genai.Client:
    global _genai_client
    if _genai_client is None:
        _genai_client = genai.Client(vertexai=True, project=FIRESTORE_PROJECT, location="global")
    return _genai_client


def generate_fleet_visual(
    prompt: str,
    tool_context: ToolContext,
) -> str:
    """Generates an image for an item in the AutoLake connected vehicle fleet (e.g. vehicle models or diagrams) using gemini-3.1-flash-lite-image.

    Args:
        prompt: Description of the vehicle model, fleet asset, or technical component diagram to generate.
        tool_context: Tool context injected by the ADK/Agent Platform runtime for saving artifacts.

    Returns:
        The public HTTPS URL of the image uploaded to Cloud Storage.
    """
    client = _get_genai()
    enhanced_prompt = (
        f"AutoLake Connected Vehicle Fleet: {prompt}. "
        "Professional commercial automotive photography or high-tech engineering concept rendering, 8k resolution, photorealistic."
    )

    try:
        response = client.models.generate_content(
            model=IMAGE_MODEL,
            contents=enhanced_prompt,
        )
    except Exception as exc:
        return f"Error al generar la imagen con el modelo {IMAGE_MODEL}: {exc}"

    image_bytes: Optional[bytes] = None
    mime_type: str = "image/jpeg"

    if response.candidates and response.candidates[0].content and response.candidates[0].content.parts:
        for part in response.candidates[0].content.parts:
            if hasattr(part, "inline_data") and part.inline_data and part.inline_data.data:
                image_bytes = part.inline_data.data
                mime_type = part.inline_data.mime_type or "image/jpeg"
                break

    if not image_bytes:
        return f"Error: El modelo {IMAGE_MODEL} no retornó datos de imagen válidos."

    file_extension = "jpg" if "jpeg" in mime_type else "png"
    timestamp = int(datetime.datetime.now().timestamp())
    filename = f"fleet_visual_{timestamp}.{file_extension}"

    # 1. Save with tool_context.save_artifact so it shows up in Playground Artifacts panel
    artifact_part = types.Part.from_bytes(data=image_bytes, mime_type=mime_type)
    tool_context.save_artifact(filename=filename, artifact=artifact_part)

    # 2. Upload the same image bytes to the public Cloud Storage bucket
    storage_client = _get_storage()
    bucket = storage_client.bucket(GCS_BUCKET_NAME)
    blob_name = f"generated_visuals/{filename}"
    blob = bucket.blob(blob_name)
    blob.upload_from_string(image_bytes, content_type=mime_type)

    public_url = f"https://storage.googleapis.com/{GCS_BUCKET_NAME}/{blob_name}"
    return public_url


if HAS_A2UI_SDK:
    schema_manager = A2uiSchemaManager(
        version="0.8",
        catalogs=[BasicCatalog.get_config("0.8")],
    )

    a2ui_instruction = schema_manager.generate_system_prompt(
        role_description=(
            "Eres AutoLake Diagnostic Copilot, un especialista en operaciones y telemetría de flotas "
            "de vehículos conectados para la plataforma AutoLake. "
            "Cuentas con memoria persistente entre sesiones. Recuerda y personaliza activamente tus interacciones "
            "basándote en las preferencias declaradas por el operador (nombre, rol, flota asignada, umbrales de alerta y unidades métricas).\n\n"
            "### TUS HERRAMIENTAS Y CÓMO USARLAS:\n"
            "1. list_vehicles(fleet, status): Consulta el estado de la flota.\n"
            "2. get_vehicle_telemetry(vin): Obtén reporte detallado por VIN.\n"
            "3. record_vehicle_telemetry(vin, status, ...): Actualiza lecturas o estados.\n"
            "4. trigger_diagnostic_pipeline(vin, reason, priority): ACCIÓN CRÍTICA. Despacha pipeline diagnóstico de alta frecuencia.\n"
            "5. generate_vehicle_health_chart(vin): Genera gráfico/gauge de salud y retorna URL pública en GCS.\n"
            "6. get_weather_risk(city): Diagnóstico térmico/meteorológico para la flota.\n"
            "7. generate_fleet_visual(prompt): Genera render conceptual o diagrama técnico de vehículo y retorna URL pública en GCS.\n"
            "8. Ejecución de Código en Sandbox: Ejecuta código Python seguro para cálculos y análisis numéricos."
        ),
        workflow_description=(
            "Analiza la consulta del operador, llama a las herramientas necesarias para auditar vehículos, "
            "generar gráficos, imágenes o telemetría, y cuando sea oportuno devuelve una interfaz de usuario estructurada en A2UI."
        ),
        ui_description=(
            "Keep every surface tiny and flat: ONE Card > ONE Column > a few Text rows. "
            "Never nest a Card inside a Card. "
            "Use ONLY these components: Card, Column, Row, Text, and Image. Do not use "
            "Table or Heading (unsupported), or Buttons, actions, or forms (they do "
            "nothing in adk web). "
            "You may include one Image component, but only when you have a public https "
            "URL for the image (for example the URL an image tool returns after uploading "
            "to a public bucket). Set the Image url to that exact https link, for example "
            "{\"Image\": {\"url\": {\"literalString\": \"https://...\"}}}. Never point an "
            "Image at a bare filename, an artifact name, or a non-http(s) path. If you do "
            "not have a public URL, add a short Text line noting the image instead. "
            "No markdown in text; use the usageHint property ('h1', 'h2', 'body') for "
            "headings and emphasis. "
            "Output ONLY the raw A2UI JSON array — no prose, and never wrap it in "
            "<a2a_datapart_json> tags or 'kind'/'data'/'metadata' objects."
        ),
        include_schema=True,
        include_examples=True,
    )
else:
    from app.a2ui_prompt import A2UI_INSTRUCTION as a2ui_instruction


root_agent = Agent(
    name="root_agent",
    model=Gemini(
        model=MODEL,
        retry_options=types.HttpRetryOptions(attempts=3),
    ),
    code_executor=AgentEngineSandboxCodeExecutor(
        sandbox_resource_name=SANDBOX_RESOURCE_NAME
    ),
    instruction=a2ui_instruction,
    tools=[
        PreloadMemoryTool(),
        list_vehicles,
        get_vehicle_telemetry,
        record_vehicle_telemetry,
        trigger_diagnostic_pipeline,
        generate_vehicle_health_chart,
        get_weather_risk,
        generate_fleet_visual,
    ],
    after_agent_callback=generate_memories_callback,
    after_model_callback=a2ui_callback,
)

app = App(
    root_agent=root_agent,
    name="app",
)
