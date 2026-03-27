
from fastapi import FastAPI, HTTPException
from supabase import create_client, Client
from pydantic import BaseModel
import os

app = FastAPI(title="Logistics AI Agent API", version="1.0")

# ⚠️ Insert your Supabase URL and Key here
# Use the 'service_role' key so the backend bypasses RLS restrictions
SUPABASE_URL = "https://oeuytozpnohtpmidptcp.supabase.co"
SUPABASE_KEY = "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6Im9ldXl0b3pwbm9odHBtaWRwdGNwIiwicm9sZSI6InNlcnZpY2Vfcm9sZSIsImlhdCI6MTc3Mzc3Mzk4MywiZXhwIjoyMDg5MzQ5OTgzfQ.uH8nDyC3iocdHNhox4DH7z637w4UAzX-nNNcy3_3Lec"

# Connect to Database
try:
    supabase: Client = create_client(SUPABASE_URL, SUPABASE_KEY)
    print("✅ Connected to Supabase Successfully!")
except Exception as e:
    print(f"❌ Database Connection Error: {e}")

# Model for receiving raw Amazon data
class AmazonOrderSync(BaseModel):
    amazon_order_id: str
    org_id: str
    store_id: str
    raw_data: dict

# 1. Root API (For server health check)
@app.get("/")
def read_root():
    return {"status": "Agent Backend is Live! 🚀", "service": "AI Logistics"}

# 2. Endpoint to fetch ready claims (Main task for your colleague tomorrow)
@app.get("/agent/pending-claims")
async def get_pending_claims():
    try:
        response = supabase.table("claim_submissions").select("*").eq("status", "ready_to_send").execute()
        return {"count": len(response.data), "claims": response.data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# 3. Endpoint to store live Amazon data in the DB (JSONB layer)
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