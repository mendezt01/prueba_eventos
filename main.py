from fastapi import FastAPI, Request, HTTPException
from google.cloud import bigquery
from datetime import datetime, timezone

app = FastAPI()

# Ajusta estos 2 valores si tu dataset/tabla tienen otro nombre
PROJECT_ID = "sincere-amulet-481314-u5"
DATASET_ID = "events_raw"
TABLE_ID = f"sincere-amulet-481314-u5.events_raw.solace_events"


bq_client = bigquery.Client()

@app.get("/")
def root():
    return {"message": "FastAPI funcionando en Cloud Run"}

@app.get("/health")
def health():
    return {"status": "ok"}

@app.post("/events")
async def receive_event(request: Request):
    event = await request.json()

    # Validación mínima
    for field in ["event_id", "event_type", "event_time", "payload"]:
        if field not in event:
            raise HTTPException(status_code=400, detail=f"Missing field: {field}")

    row = {
        "event_id": event["event_id"],
        "event_type": event["event_type"],
        "event_source": event.get("event_source", "solace"),
        "event_time": event["event_time"],  # ISO8601 string
        "spec_version": event.get("spec_version", "1.0"),
        "payload": event["payload"],        # JSON
        "ingestion_time": datetime.now(timezone.utc).isoformat(),
    }

    errors = bq_client.insert_rows_json(TABLE_ID, [row])
    if errors:
        raise HTTPException(status_code=500, detail=str(errors))

    return {"status": "accepted"}
