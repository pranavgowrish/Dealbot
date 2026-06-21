from fastapi import FastAPI
import uuid


async def job_start(product, budget, max_price, urls):
    job_id = str(uuid.uuid4())
    return {
        "job_id": job_id,
        "product_name": product,
        "target_budget": budget,
        "max_price": max_price,
        "listing_urls": urls,
    }


app = FastAPI()


@app.post("/start_job")
async def start_job(product: str, budget: int, max_price: int, urls: list):
    job = await job_start(product, budget, max_price, urls)
    return {"job_id": job["job_id"], "status": "started"}
