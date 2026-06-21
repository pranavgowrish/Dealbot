from fastapi import FastAPI
import json
import uuid
from thenvoi import Agent
from thenvoi.config import load_agent_config

async def job_start(product, budget, max_price, urls):
    job_id = str(uuid.uuid4())
    room_name = f"{product} War Room ({job_id})"

    task = {
        "type": "start_job",
        "job_id": job_id,
        "room_name": room_name,
        "product_name": product,
        "target_budget": budget,
        "max_price": max_price,
        "listing_urls": urls,
    }

    agent_id, api_key = load_agent_config("backend")
    backend = Agent.create(agent_id=agent_id, api_key=api_key)

    manager_id, _ = load_agent_config("manager")
    await backend.send_message(manager_id, json.dumps(task))
    return job_id

app = FastAPI()


@app.post("/start_job")
async def start_job(product: str, budget: int, max_price: int, urls: list):
    job_id = await job_start(product, budget, max_price, urls)
    return {"job_id": job_id, "status": "started"}
