from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"message": "FastAPI funcionando en Cloud Run"}

@app.get("/health")
def health():
    return {"status": "ok"}
