from fastapi import FastAPI, Request, HTTPException
from google.cloud import bigquery
from datetime import datetime, timezone

app = FastAPI()

# ===== CONFIGURACIÓN FIJA =====
PROJECT_ID = "sincere-amulet-481314-u5"
DATASET_ID = "events_raw"
TABLE_NAME = "solace_events"

TABLE_ID = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}"

bq_client = bigquery.Client(project=PROJECT_ID)

# ===== ENDPOINTS BÁSICOS =====

@app.get("/")
def root():
    return {"message": "FastAPI funcionando en Cloud Run"}

@app.get("/health")
def health():
    return {"status": "ok"}

# ===== ENDPOINT DE EVENTOS =====

@app.post("/events")
async def receive_event(request: Request):
    try:
        event = await request.json()

        # Validación mínima
        for field in ["event_id", "event_type", "event_time", "payload"]:
            if field not in event:
                raise HTTPException(
                    status_code=400,
                    detail=f"Missing field: {field}"
                )

        row = {
            "event_id": event["event_id"],
            "event_type": event["event_type"],
            "event_source": event.get("event_source", "solace"),
            "event_time": event["event_time"],  # ISO 8601
            "spec_version": event.get("spec_version", "1.0"),
            "payload": event["payload"],        # JSON
            "ingestion_time": datetime.now(timezone.utc).isoformat(),
        }

        errors = bq_client.insert_rows_json(
            TABLE_ID,
            [row]
        )

        if errors:
            raise HTTPException(
                status_code=500,
                detail=f"BigQuery insert errors: {errors}"
            )

        return {"status": "accepted"}

    except HTTPException:
        raise

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=str(e)
        )
