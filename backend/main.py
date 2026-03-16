import os
from pathlib import Path

from dotenv import load_dotenv
from fastapi import FastAPI
from supabase import Client, create_client

BASE_DIR = Path(__file__).resolve().parent
load_dotenv(BASE_DIR / ".env")

app = FastAPI()

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_SERVICE_ROLE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

if not SUPABASE_URL or not SUPABASE_SERVICE_ROLE_KEY:
    raise ValueError("Missing SUPABASE_URL or SUPABASE_SERVICE_ROLE_KEY in .env")

supabase: Client = create_client(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY)


@app.get("/")
def root():
    return {"ok": True, "message": "Evidence Tool API is running"}


@app.get("/test-db")
def test_db():
    try:
        result = supabase.table("jobs").select("*").limit(5).execute()
        return {
            "ok": True,
            "message": "Connected to Supabase successfully",
            "data": result.data,
        }
    except Exception as e:
        return {
            "ok": False,
            "message": "Failed to connect to Supabase",
            "error": str(e),
        }
