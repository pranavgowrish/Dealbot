from __future__ import annotations

import logging
import uuid

from fastapi import BackgroundTasks, FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

from browserb import search_marketplace

logger = logging.getLogger(__name__)

app = FastAPI(title="Dealbot API")

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "http://localhost:5173",
        "http://127.0.0.1:5173",
        "http://localhost:3000",
        "http://127.0.0.1:3000",
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class SearchRequest(BaseModel):
    product: str
    price: float = Field(..., gt=0, description="Max budget / target price")
    location: str
    dateListed: str | None = None
    condition: str | None = None
    color: str | None = None


class ListingItem(BaseModel):
    id: int
    title: str
    price: str
    image_url: str | None = None
    listing_url: str
    area: str
    source: str = "Facebook Marketplace"


class SearchResponse(BaseModel):
    listings: list[ListingItem]


class OrchestrateRequest(BaseModel):
    product: str
    budget: float = Field(..., gt=0)
    max_price: float = Field(..., gt=0)
    listing_urls: list[str] = Field(..., min_length=1, max_length=5)


class OrchestrateResponse(BaseModel):
    job_id: str
    status: str


def _build_search_query(payload: SearchRequest) -> str:
    parts = [payload.product.strip()]
    if payload.condition and payload.condition.strip():
        parts.append(payload.condition.strip())
    if payload.color and payload.color.strip():
        parts.append(payload.color.strip())
    return " ".join(parts)


async def _run_orchestration(
    job_id: str,
    product: str,
    budget: float,
    max_price: float,
    listing_urls: list[str],
) -> None:
    from agent_initializer import run_dealbot

    logger.info(
        "Starting orchestration job %s for %d listing(s)",
        job_id,
        len(listing_urls),
    )
    try:
        await run_dealbot(
            product_name=product,
            target_budget=budget,
            max_price=max_price,
            listing_urls=listing_urls,
        )
        logger.info("Orchestration job %s completed", job_id)
    except Exception:
        logger.exception("Orchestration job %s failed", job_id)


@app.get("/health")
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.post("/api/v1/search", response_model=SearchResponse)
async def search_listings(payload: SearchRequest) -> SearchResponse:
    query = _build_search_query(payload)
    if not query:
        raise HTTPException(status_code=400, detail="Product is required")

    try:
        raw_listings = await search_marketplace(
            item_description=query,
            target_price=payload.price,
            location=payload.location.strip(),
            max_results=10,
        )
    except Exception as exc:
        logger.exception("Marketplace search failed")
        raise HTTPException(
            status_code=502,
            detail=f"Marketplace search failed: {exc}",
        ) from exc

    listings: list[ListingItem] = []
    for index, item in enumerate(raw_listings[:10], start=1):
        listing_url = item.get("listing_url")
        if not listing_url:
            continue
        listings.append(
            ListingItem(
                id=index,
                title=item.get("title") or f"Listing {index}",
                price=item.get("price") or "—",
                image_url=item.get("image_url"),
                listing_url=listing_url,
                area=payload.location.strip(),
            )
        )

    return SearchResponse(listings=listings)


@app.post("/api/v1/orchestrate", response_model=OrchestrateResponse)
async def orchestrate(
    payload: OrchestrateRequest,
    background_tasks: BackgroundTasks,
) -> OrchestrateResponse:
    job_id = str(uuid.uuid4())
    background_tasks.add_task(
        _run_orchestration,
        job_id,
        payload.product.strip(),
        payload.budget,
        payload.max_price,
        payload.listing_urls,
    )
    return OrchestrateResponse(job_id=job_id, status="started")
