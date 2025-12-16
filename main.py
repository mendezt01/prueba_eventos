import json

from fastapi import FastAPI, Request, HTTPException
from google.cloud import bigquery
from datetime import datetime, timezone
import os

app = FastAPI()

PROJECT_ID = "sincere-amulet-481314-u5"
DATASET_ID = "events_raw"
TABLE_NAME = "solace_events"
TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

def get_bq_client() -> bigquery.Client:
    # Crear el cliente "on-demand" evita fallas en startup/import-time
    return bigquery.Client(project=PROJECT_ID)

@app.get("/")
def root():
    return {"message": "FastAPI funcionando en Cloud Run"}

@app.get("/health")
def health():
    return {"status": "ok", "revision": os.getenv("K_REVISION", "unknown")}

@app.post("/events")
async def receive_event(request: Request):
    try:
        event = await request.json()

        for field in ["event_id", "event_type", "event_time", "payload"]:
            if field not in event:
                raise HTTPException(status_code=400, detail=f"Missing field: {field}")

        row = {
    "event_id": event["event_id"],
    "event_type": event["event_type"],
    "event_source": event.get("event_source", "solace"),
    "event_time": event["event_time"],
    "spec_version": event.get("spec_version", "1.0"),
    "payload": json.dumps(event["payload"]),  # <-- CLAVE: convertir a string JSON
    "ingestion_time": datetime.now(timezone.utc).isoformat(),
}


        client = get_bq_client()
        errors = client.insert_rows_json(TABLE_ID, [row])

        if errors:
            raise HTTPException(status_code=500, detail=f"BigQuery insert errors: {errors}")

        return {"status": "accepted"}

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
