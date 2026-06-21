from fastapi import FastAPI
import json
import uuid


app = FastAPI()


@app.post("/findlistings")
async def start_job(product: str, budget: int, max_price: int, urls: list):
    job_id = await job_start(product, budget, max_price, urls)
    return {"job_id": job_id, "status": "started"}
