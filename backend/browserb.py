"""
Facebook Marketplace search via Browserbase + Stagehand.

Required environment variables:
    BROWSERBASE_API_KEY
    BROWSERBASE_PROJECT_ID
    MODEL_API_KEY
    BROWSERBASE_CONTEXT_ID  (optional – reuses a persistent context if set)
"""

from __future__ import annotations

import json
import os
import urllib.parse
import asyncio
from typing import Any

from dotenv import load_dotenv
from browserbase import Browserbase
from stagehand import AsyncStagehand, APIResponseValidationError


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

async def _stream_to_result(
    stream,
    label: str,
    *,
    verbose: bool = True,
) -> Any | None:
    result_payload: Any | None = None
    async for event in stream:
        if event.type == "log":
            if verbose:
                print(f"[{label}][log] {event.data.message}")
            continue
        status = event.data.status
        if verbose:
            print(f"[{label}][system] status={status}")
        if status == "finished":
            result_payload = event.data.result
        elif status == "error":
            raise RuntimeError(f"{label} stream error: {event.data.error or 'unknown'}")
    return result_payload


def _build_marketplace_url(item_description: str, location: str) -> str:
    encoded_query = urllib.parse.quote_plus(item_description)
    encoded_location = urllib.parse.quote_plus(location)
    return (
        f"https://www.facebook.com/marketplace/search/"
        f"?query={encoded_query}"
        f"&location={encoded_location}"
    )


def _build_instruction(
    item_description: str,
    price_min: float,
    price_max: float,
    location: str,
    max_results: int,
) -> str:
    return f"""You are on a Facebook Marketplace search results page for '{item_description}'.

RULES — read carefully:
- Do NOT click any listing card. Stay on the search results page the entire time.
- Do NOT use the act tool to click anything except the price filter inputs.
- You MUST use the extract tool with domContent mode to read raw HTML attributes.

STEPS:

1. Set price and location filters:
   - Type {price_min:.0f} into the Min price input
   - Type {price_max:.0f} into the Max price input
   - Press Enter and wait 500ms for results to reload.
   - Type "{location}" into the location filter input, select the top dropdown option, and wait 500ms for results to reload.


2. Use the extract tool in domContent mode on the current page.
   You are looking for listing cards. Each card is an <a> tag whose href contains "/marketplace/item/".

   For EACH card extract these exact values from the raw HTML:

   listing_url: The complete href attribute of the <a> tag.
     It looks like: https://www.facebook.com/marketplace/item/1039380025213051/?ref=search&referral_code=null&referral_story_type=post&tracking=browse_serp%3A681b5290-1257-4de3-be24-712806119020
     Copy the FULL href string including all query parameters after the ? symbol.
     Do NOT shorten or truncate it.

   image_url: The complete src attribute of the <img> tag inside that card.
     It looks like: https://scontent-sjc6-1.xx.fbcdn.net/v/t39.84726-6/725147376_1007248095225528_3555731290710451503_n.jpg?stp=c89.0.540.540a_dst-jpg_p180x540_tt6&_nc_cat=108&ccb=1-7&_nc_sid=92e707&_nc_ohc=XoUF57qH2poQ7kNvwGQ6Msr&oh=00_Af-something&oe=6A3CF81E
     Copy the FULL src string including all query parameters after the ? symbol.
     The domain will be scontent-*.xx.fbcdn.net. Do NOT shorten or truncate it.

   title: The text content of the aria-label attribute on the <a> tag, or the first visible span text.

   price: The price string starting with $ visible on the card.

3. Only include listings where the price is between ${price_min:.2f} and ${price_max:.2f} and make sure all the listings chosen are similar prices.
Never abort the full task due to a single small error, just retry that specific task and continue.

4. Return up to {max_results} listings as a raw JSON array — no markdown, no explanation:
[{{"title": "...", "price": "...", "image_url": "https://scontent-....fbcdn.net/v/...FULL URL...", "listing_url": "https://www.facebook.com/marketplace/item/...FULL URL..."}}]
"""


def _parse_listings(raw: Any, price_min: float, price_max: float) -> list[dict]:
    if not raw:
        return []

    text = str(raw).strip()

    if text.startswith("```"):
        lines = text.splitlines()
        text = "\n".join(l for l in lines if not l.strip().startswith("```")).strip()

    start = text.find("[")
    end = text.rfind("]")
    if start == -1 or end == -1 or end <= start:
        print(f"No JSON array found in output:\n{text[:400]}")
        return []

    try:
        parsed = json.loads(text[start:end + 1])
    except json.JSONDecodeError as exc:
        print(f"JSON parse error: {exc}\nSegment: {text[start:end+1][:400]}")
        return []

    if isinstance(parsed, dict):
        for v in parsed.values():
            if isinstance(v, list):
                parsed = v
                break

    def to_float(p: str) -> float | None:
        try:
            return float(str(p).replace("$", "").replace(",", "").strip())
        except (ValueError, AttributeError):
            return None

    return [
        item for item in parsed
        if (pv := to_float(item.get("price", ""))) is not None
        and price_min <= pv <= price_max
    ]


LISTING_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "title": {"type": "string"},
        "price": {"type": "string"},
        "description": {"type": "string"},
        "seller_name": {"type": "string"},
        "location": {"type": "string"},
    },
    "required": ["title", "price", "description", "seller_name", "location"],
}


def _to_dict(value: Any) -> dict[str, Any] | None:
    if value is None:
        return None
    if isinstance(value, dict):
        return value
    if hasattr(value, "model_dump"):
        dumped = value.model_dump()
        return dumped if isinstance(dumped, dict) else None
    return None


def _listing_id(link: str) -> str:
    if "/item/" in link:
        return link.split("/item/", 1)[1].split("/", 1)[0]
    return link[:48]


def _debug_print(label: str, message: str) -> None:
    print(f"[{label}] {message}")


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def search_marketplace(
    item_description: str,
    target_price: float,
    location: str,
    *,
    price_spread: float = 50.0,
    max_results: int = 10,
    max_steps: int = 10,
    context_id: str | None = None,
    model_name: str = "anthropic/claude-sonnet-4-6",
) -> list[dict]:
    load_dotenv()

    price_min = max(0.0, target_price - price_spread)
    price_max = target_price + price_spread
    url = _build_marketplace_url(item_description, location)
    instruction = _build_instruction(
        item_description, price_min, price_max, location, max_results
    )

    resolved_context_id = context_id or os.environ.get("BROWSERBASE_CONTEXT_ID")

    bb = Browserbase(api_key=os.environ["BROWSERBASE_API_KEY"])
    session_opts: dict[str, Any] = {"project_id": os.environ["BROWSERBASE_PROJECT_ID"]}
    if resolved_context_id:
        session_opts["browser_settings"] = {
            "context": {"id": resolved_context_id, "persist": True}
        }

    bb_session = bb.sessions.create(**session_opts)
    print(f"Browserbase session : {bb_session.id}")
    print(f"URL                 : {url}")
    print(f"Price range         : ${price_min:.2f} – ${price_max:.2f}")
    print(f"Location            : {location}")

    async with AsyncStagehand(
        browserbase_api_key=os.environ["BROWSERBASE_API_KEY"],
        browserbase_project_id=os.environ["BROWSERBASE_PROJECT_ID"],
        _strict_response_validation=True,
        model_api_key=os.environ["MODEL_API_KEY"],
    ) as client:
        try:
            session = await client.sessions.start(
                model_name=model_name,
                browserbase_session_id=bb_session.id,
            )
        except APIResponseValidationError as e:
            print(f"Session schema error — HTTP {e.response.status_code}")
            print(e.response.text)
            raise

        if not session.id:
            raise RuntimeError(f"Expected session ID, got {session!r}")

        try:
            await session.navigate(url=url, options={"wait_until": "domcontentloaded"})

            stream = await session.execute(
                agent_config={"model": model_name},
                execute_options={
                    "instruction": instruction,
                    "max_steps": max_steps,
                },
                stream_response=True,
                x_stream_response="true",
            )
            result = await _stream_to_result(stream, "execute")

        finally:
            print(f"\nSession ID: {session.id}")
            try:
                await session.end()
            except Exception:
                pass

    raw_message = result.get("message") if isinstance(result, dict) else result
    print("\nAgent raw output:")
    print(raw_message)

    listings = _parse_listings(raw_message, price_min, price_max)

    print(f"\nFound {len(listings)} listing(s):")
    for idx, item in enumerate(listings, 1):
        print(f"  {idx}. {item.get('title')} — {item.get('price')}")
        print(f"       img: {item.get('image_url', '')[:90]}...")
        print(f"       url: {item.get('listing_url', '')}")

    return listings


async def _scrape_listing(
    link: str,
    model_name: str,
) -> dict[str, Any]:
    """
    Creates one Browserbase session, opens the page,
    extracts listing details, and returns the result.
    """
    resolved_context_id = os.environ.get("BROWSERBASE_CONTEXT_ID")

    bb = Browserbase(api_key=os.environ["BROWSERBASE_API_KEY"])
    session_opts: dict[str, Any] = {"project_id": os.environ["BROWSERBASE_PROJECT_ID"]}
    if resolved_context_id:
        session_opts["browser_settings"] = {
            "context": {"id": resolved_context_id, "persist": True}
        }

    bb_session = bb.sessions.create(**session_opts)

    async with AsyncStagehand(
        browserbase_api_key=os.environ["BROWSERBASE_API_KEY"],
        browserbase_project_id=os.environ["BROWSERBASE_PROJECT_ID"],
        model_api_key=os.environ["MODEL_API_KEY"],
        _strict_response_validation=True,
    ) as client:

        try:
            session = await client.sessions.start(
                model_name=model_name,
                browserbase_session_id=bb_session.id,
            )
        except APIResponseValidationError as e:
            print(f"Session schema error — HTTP {e.response.status_code}")
            print(e.response.text)
            raise

        try:
            await session.navigate(
                url=link,
                options={"wait_until": "domcontentloaded"},
            )

            label = _listing_id(link)
            _debug_print(label, "navigated, extracting listing data…")

            response = await session.extract(
                instruction=(
                    "Extract the Facebook Marketplace listing details "
                    "visible on this page."
                ),
                schema=LISTING_SCHEMA,
                options={"model": model_name},
            )

            data = _to_dict(response.data.result if response.data else None)
            if data:
                _debug_print(
                    label,
                    f"{data.get('title', '?')} — {data.get('price', '?')} "
                    f"({data.get('location', '?')})",
                )
            else:
                _debug_print(
                    label,
                    f"extract returned no data (success={response.success})",
                )

            return {
                "browserbase_session_id": bb_session.id,
                "listing_url": link,
                "data": data,
            }

        finally:
            try:
                await session.end()
            except Exception:
                pass


async def initialize_browsers(
    links: list[str],
    *,
    model_name: str = "anthropic/claude-sonnet-4-6",
) -> list[dict[str, Any]]:
    """
    Launches one Browserbase + Stagehand session per URL
    and scrapes all pages concurrently.
    """

    load_dotenv()

    print(f"Scraping {len(links)} listing(s)…")

    tasks = [
        _scrape_listing(link=link, model_name=model_name)
        for link in links
    ]

    results = await asyncio.gather(*tasks)

    print("\nFinal JSON:")
    print(json.dumps(results, indent=2))

    return results



# ---------------------------------------------------------------------------
# Demo
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    asyncio.run(
        initialize_browsers([
            "https://www.facebook.com/marketplace/item/954225447099368/?ref=search&referral_code=null&referral_story_type=post&tracking=browse_serp%3Ae9ebf225-f0d9-414c-b089-9d392df74096",
            "https://www.facebook.com/marketplace/item/1346267354014284/?ref=search&referral_code=null&referral_story_type=post&tracking=browse_serp%3Ae9ebf225-f0d9-414c-b089-9d392df74096",
        ])
    )