from fastapi import FastAPI, HTTPException
from supabase import create_client, Client
from pydantic import BaseModel
import os
from dotenv import load_dotenv

# Load environment variables securely from .env file
load_dotenv()

app = FastAPI(title="Logistics AI Agent API", version="1.0")

# Fetch keys from environment
SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_SERVICE_ROLE_KEY")

# Initialize Database Connection
try:
    if not SUPABASE_URL or not SUPABASE_KEY:
        print("Error: Missing Supabase credentials in .env file!")
    else:
        supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
        print("Connected to Supabase Successfully!")
except Exception as e:
    print(f"Database Connection Error: {e}")

# Data Model for Amazon SP-API Sync
class AmazonOrderSync(BaseModel):
    amazon_order_id: str
    org_id: str
    store_id: str
    raw_data: dict

# 1. Root Endpoint (Health Check)
@app.get("/")
def read_root():
    return {"status": "Agent Backend is Live!", "service": "AI Logistics"}

# 2. Agent Queue Endpoint
@app.get("/agent/pending-claims")
async def get_pending_claims():
    try:
        response = supabase.table("claim_submissions").select("*").eq("status", "ready_to_send").execute()
        return {"count": len(response.data), "claims": response.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 3. Landing Zone Endpoint for Live Amazon Data
@app.post("/sync/order")
async def save_raw_amazon_order(order: AmazonOrderSync):
    try:
        data = {
            "organization_id": order.org_id,
            "store_id": order.store_id,
            "amazon_order_id": order.amazon_order_id,
            "raw_data": order.raw_data,
            "status": "synced"
        }
        supabase.table("marketplace_orders").upsert(data).execute()
        return {"status": "success", "message": "Order synced to Landing Zone"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))