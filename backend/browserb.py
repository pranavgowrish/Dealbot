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
import re
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
    #1. Set location filter:
  # - Press Enter and wait 500ms for results to reload.
  # - Type "{location}" into the location filter input, select the top dropdown option, and wait 500ms for results to reload.

#    - Type {price_min:.0f} into the Min price input
#    - Type {price_max:.0f} into the Max price input

_ITEM_ID_RE = re.compile(r"(?:listing\s+)?(\d{10,})")
_A11Y_REF_RE = re.compile(r"^\[?\d+-\d+\]?$")

_MARKETPLACE_CARDS_JS = """
() => {
  const out = [];
  const seen = new Set();
  for (const anchor of document.querySelectorAll('a[href*="/marketplace/item/"]')) {
    const href = anchor.href || anchor.getAttribute("href") || "";
    const match = href.match(/\\/item\\/(\\d+)/);
    if (!match) continue;
    const itemId = match[1];
    if (seen.has(itemId)) continue;

    const anchorText = (anchor.textContent || "").replace(/\\s+/g, " ").trim();
    const ariaLabel = (anchor.getAttribute("aria-label") || "").trim();
    const textNodes = Array.from(anchor.querySelectorAll("span,div,strong,h2,h3"))
      .map((node) => (node.textContent || "").replace(/\\s+/g, " ").trim())
      .filter(Boolean);
    const titleFromAria = ariaLabel ? ariaLabel.split(",")[0].trim() : "";
    const titleFromText = textNodes.find((value) => {
      if (!value) return false;
      if (/^\\$\\s?\\d[\\d,]*(?:\\.\\d{2})?$/.test(value)) return false;
      if (/^(free|new|used)$/i.test(value)) return false;
      return value.length >= 3;
    }) || "";
    const title = titleFromAria || titleFromText || anchorText.slice(0, 120);

    const priceMatch = (anchorText || ariaLabel).match(/\\$\\s?\\d[\\d,]*(?:\\.\\d{2})?/);
    const price = priceMatch ? priceMatch[0].replace(/\\s+/g, "") : "";

    const img = anchor.querySelector("img");
    let imageUrl = "";
    if (img) {
      imageUrl = img.currentSrc || img.src || img.getAttribute("src") || "";
      if ((!imageUrl || !imageUrl.startsWith("http")) && img.srcset) {
        imageUrl = img.srcset.split(",")[0].trim().split(/\\s+/)[0];
      }
      if ((!imageUrl || !imageUrl.startsWith("http"))) {
        imageUrl = img.getAttribute("data-src") || img.getAttribute("data-imgsrc") || "";
      }
    }

    seen.add(itemId);
    out.push({
      item_id: itemId,
      listing_url: href,
      image_url: imageUrl,
      title,
      price,
    });
  }
  return out;
}
"""


def _build_search_extract_instruction(
    item_description: str,
    price_min: float,
    price_max: float,
    location: str,
    max_results: int,
) -> str:
    return (
        f"Extract visible Facebook Marketplace listing cards on this search "
        f"results page for '{item_description}' near {location}.\n"
        f"For each card, extract title, price (string starting with $), "
        f"and listing_url (full href to /marketplace/item/ if available).\n"
        f"Only include listings priced between ${price_min:.2f} and "
        f"${price_max:.2f}. Return up to {max_results} listings."
    )


def _normalize_listing_url(url: str, title: str = "") -> str:
    if url.startswith("http") and "/marketplace/item/" in url:
        return url

    if "/item/" in url:
        item_id = url.split("/item/", 1)[1].split("/", 1)[0].split("?", 1)[0]
        if item_id.isdigit():
            return f"https://www.facebook.com/marketplace/item/{item_id}/"

    for text in (title, url):
        if match := _ITEM_ID_RE.search(text):
            return f"https://www.facebook.com/marketplace/item/{match.group(1)}/"

    return url


def _listing_item_id(url: str) -> str | None:
    if "/item/" not in url:
        return None
    item_id = url.split("/item/", 1)[1].split("/", 1)[0].split("?", 1)[0]
    return item_id if item_id.isdigit() else None


def _is_valid_image_url(url: str) -> bool:
    if not url or _A11Y_REF_RE.match(url.strip()):
        return False
    return url.startswith("http")


async def _extract_search_dom_cards(connect_url: str) -> list[dict[str, str]]:
    """Read listing href/src/title/price directly from the live DOM via CDP."""
    from playwright.async_api import async_playwright

    cards: list[dict[str, str]] = []
    playwright = await async_playwright().start()
    try:
        browser = await playwright.chromium.connect_over_cdp(connect_url)
        context = browser.contexts[0] if browser.contexts else None
        if context is None:
            return cards

        page = context.pages[0] if context.pages else None
        if page is None:
            return cards

        rows = await page.evaluate(_MARKETPLACE_CARDS_JS)
        for row in rows or []:
            if not isinstance(row, dict):
                continue
            item_id = str(row.get("item_id", "")).strip()
            listing_url = str(row.get("listing_url", "")).strip()
            title = str(row.get("title", "")).strip()
            price = str(row.get("price", "")).strip()
            image_url = str(row.get("image_url", "")).strip()
            if not item_id:
                continue
            cards.append(
                {
                    "item_id": item_id,
                    "listing_url": listing_url,
                    "title": title,
                    "price": price,
                    "image_url": image_url,
                }
            )
    finally:
        await playwright.stop()

    return cards


def _normalize_dom_listings(
    cards: list[dict[str, str]],
    price_min: float,
    price_max: float,
    max_results: int,
) -> list[dict]:
    normalized: list[dict] = []
    seen_item_ids: set[str] = set()

    for card in cards:
        listing_url = _normalize_listing_url(
            str(card.get("listing_url", "")).strip(),
            str(card.get("title", "")).strip(),
        )
        item_id = _listing_item_id(listing_url)
        if not listing_url.startswith("http") or not item_id or item_id in seen_item_ids:
            continue

        price = str(card.get("price", "")).strip()
        price_value = _price_to_float(price)
        if price_value is None or not (price_min <= price_value <= price_max):
            continue

        image_url = str(card.get("image_url", "")).strip()
        if not _is_valid_image_url(image_url):
            image_url = ""

        title = str(card.get("title", "")).strip() or f"Marketplace listing {item_id}"
        normalized.append(
            {
                "title": title,
                "price": price,
                "listing_url": listing_url,
                "image_url": image_url,
            }
        )
        seen_item_ids.add(item_id)
        if len(normalized) >= max_results:
            break

    return normalized


def _price_to_float(price: str) -> float | None:
    try:
        return float(str(price).replace("$", "").replace(",", "").strip())
    except (ValueError, AttributeError):
        return None


def _normalize_listings(
    raw: Any,
    price_min: float,
    price_max: float,
    max_results: int,
) -> list[dict]:
    if not raw:
        return []

    listings: list[Any]
    if isinstance(raw, dict):
        listings = raw.get("listings", [])
        if not isinstance(listings, list):
            listings = []
    elif isinstance(raw, list):
        listings = raw
    else:
        text = str(raw).strip()
        if text.startswith("```"):
            lines = text.splitlines()
            text = "\n".join(
                line for line in lines if not line.strip().startswith("```")
            ).strip()

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
            listings = parsed.get("listings", [])
            if not isinstance(listings, list):
                for value in parsed.values():
                    if isinstance(value, list):
                        listings = value
                        break
                else:
                    listings = []
        elif isinstance(parsed, list):
            listings = parsed
        else:
            return []

    normalized: list[dict] = []
    for item in listings:
        if not isinstance(item, dict):
            continue

        price_value = _price_to_float(item.get("price", ""))
        if price_value is None or not (price_min <= price_value <= price_max):
            continue

        title = str(item.get("title", "")).strip()
        listing_url = _normalize_listing_url(
            str(item.get("listing_url", "")).strip(),
            title,
        )
        if not listing_url.startswith("http"):
            continue

        image_url = str(item.get("image_url", "")).strip()
        if not _is_valid_image_url(image_url):
            image_url = ""

        normalized.append(
            {
                "title": title,
                "price": item.get("price", ""),
                "listing_url": listing_url,
                "image_url": image_url,
            }
        )
        if len(normalized) >= max_results:
            break

    return normalized


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

LISTINGS_SEARCH_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "listings": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "title": {"type": "string"},
                    "price": {"type": "string"},
                    "listing_url": {"type": "string"},
                    "image_url": {"type": "string"},
                },
                "required": ["title", "price"],
            },
        }
    },
    "required": ["listings"],
}

REPLY_CHECK_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "seller_replied": {"type": "boolean"},
        "reply_message": {"type": ["string", "null"]},
    },
    "required": ["seller_replied", "reply_message"],
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


def _message_preview(text: str | None, limit: int = 220) -> str:
    if not text:
        return ""
    cleaned = " ".join(str(text).split())
    if len(cleaned) <= limit:
        return cleaned
    return f"{cleaned[:limit]}..."


def _build_send_message_instruction(message: str) -> str:
    quoted = json.dumps(message)
    return f"""You are on a Facebook Marketplace listing page.

1. Click the "Message" or "Message seller" button to open the chat/message box.
2. If the chat is already open, focus the message input at the bottom.
3. Type this message EXACTLY: {quoted}
4. Send it by pressing Enter or clicking Send.
5. Do NOT scroll through or read old chat history.

Stop as soon as the message is sent."""


def _build_reply_check_instruction(message: str) -> str:
    quoted = json.dumps(message)
    return f"""In the open Marketplace chat, look only at messages that appeared AFTER the buyer sent this exact message: {quoted}

Ignore all earlier chat history.

Return:
- seller_replied: true only if the seller sent a new message after that buyer message
- reply_message: the seller's newest message text after that buyer message, or null if none"""


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def search_marketplace(
    item_description: str,
    target_price: float,
    location: str,
    *,
    price_below: float = 50.0,
    price_above: float = 30.0,
    max_results: int = 10,
    context_id: str | None = None,
    model_name: str = "anthropic/claude-sonnet-4-6",
    extract_timeout_s: float = 180.0,
) -> list[dict]:
    load_dotenv()

    price_min = max(0.0, target_price - price_below)
    price_max = target_price + price_above
    url = _build_marketplace_url(item_description, location)
    instruction = _build_search_extract_instruction(
        item_description, price_min, price_max, location, max_results
    )

    resolved_context_id = context_id or os.environ.get("BROWSERBASE_CONTEXT_ID")

    bb = Browserbase(api_key=os.environ["BROWSERBASE_API_KEY"])
    session_opts: dict[str, Any] = {
        "project_id": os.environ["BROWSERBASE_PROJECT_ID"],
        "keep_alive": True,
    }
    if resolved_context_id:
        session_opts["browser_settings"] = {
            "context": {"id": resolved_context_id, "persist": True}
        }

    bb_session = bb.sessions.create(**session_opts)
    print(f"Browserbase session : {bb_session.id}")
    print(f"URL                 : {url}")
    print(f"Price range         : ${price_min:.2f} – ${price_max:.2f}")
    print(f"Location            : {location}")

    raw_data: dict[str, Any] | None = None
    listings: list[dict] = []
    dom_cards: list[dict[str, str]] = []

    async with AsyncStagehand(
        browserbase_api_key=os.environ["BROWSERBASE_API_KEY"],
        browserbase_project_id=os.environ["BROWSERBASE_PROJECT_ID"],
        _strict_response_validation=True,
        model_api_key=os.environ["MODEL_API_KEY"],
        timeout=extract_timeout_s,
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

            try:
                dom_cards = await _extract_search_dom_cards(bb_session.connect_url)
                listings = _normalize_dom_listings(
                    dom_cards, price_min, price_max, max_results
                )
                print(
                    f"DOM extracted {len(dom_cards)} card(s); "
                    f"{len(listings)} in price range"
                )
            except Exception as exc:
                print(f"DOM listing extraction failed: {exc}")

            # Fallback to Stagehand extract if DOM parsing returns no usable listings.
            if not listings:
                response = await session.extract(
                    instruction=instruction,
                    schema=LISTINGS_SEARCH_SCHEMA,
                    options={"model": model_name},
                )
                raw_data = _to_dict(response.data.result if response.data else None)
                listings = _normalize_listings(
                    raw_data, price_min, price_max, max_results
                )

        finally:
            print(f"\nSession ID: {session.id}")
            try:
                await session.end()
            except Exception:
                pass

    print("\nExtract raw output:")
    print(json.dumps(raw_data, indent=2) if raw_data else raw_data)
    if dom_cards:
        print(f"DOM raw cards count: {len(dom_cards)}")

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
    load_dotenv()

    resolved_context_id = os.environ.get("BROWSERBASE_CONTEXT_ID")

    bb = Browserbase(api_key=os.environ["BROWSERBASE_API_KEY"])
    session_opts: dict[str, Any] = {
        "project_id": os.environ["BROWSERBASE_PROJECT_ID"],
        "keep_alive": True,
    }
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

        if not session.id:
            raise RuntimeError(f"Expected session ID, got {session!r}")

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


async def initialize_browsers(
    links: list[str],
    *,
    model_name: str = "anthropic/claude-sonnet-4-6",
    max_concurrency: int = 2,
    scrape_timeout_s: float = 120,
) -> list[dict[str, Any]]:
    """
    Launches one Browserbase + Stagehand session per URL
    and scrapes all pages concurrently.
    """
    load_dotenv()

    print(f"Scraping {len(links)} listing(s)…", flush=True)

    semaphore = asyncio.Semaphore(max(1, max_concurrency))

    async def _run(link: str, index: int) -> dict[str, Any]:
        label = _listing_id(link)
        async with semaphore:
            print(f"[{index}/{len(links)}] starting {label}…", flush=True)
            try:
                return await asyncio.wait_for(
                    _scrape_listing(link=link, model_name=model_name),
                    timeout=scrape_timeout_s,
                )
            except asyncio.TimeoutError as exc:
                raise RuntimeError(
                    f"Scrape timed out after {scrape_timeout_s:.0f}s for {link}"
                ) from exc

    tasks = [_run(link, i) for i, link in enumerate(links, start=1)]
    results = await asyncio.gather(*tasks)

    print("\nFinal JSON:")
    print(json.dumps(results, indent=2))

    return results


async def send_listing_message(
    browserbase_session_id: str,
    message: str,
    *,
    model_name: str = "anthropic/claude-sonnet-4-6",
    reply_wait_ms: int = 2500,
) -> dict[str, str | None]:
    """
    Resume a Browserbase session, open the listing message box, send a message,
    wait for a reply, and return only the sent message and any seller reply.
    """
    load_dotenv()

    async with AsyncStagehand(
        browserbase_api_key=os.environ["BROWSERBASE_API_KEY"],
        browserbase_project_id=os.environ["BROWSERBASE_PROJECT_ID"],
        model_api_key=os.environ["MODEL_API_KEY"],
        _strict_response_validation=True,
    ) as client:
        try:
            session = await client.sessions.start(
                model_name=model_name,
                browserbase_session_id=browserbase_session_id,
            )
        except APIResponseValidationError as e:
            print(f"Session schema error — HTTP {e.response.status_code}")
            print(e.response.text)
            raise

        if not session.id:
            raise RuntimeError(f"Expected session ID, got {session!r}")

        _debug_print(browserbase_session_id[:8], "resumed session, opening chat…")
        _debug_print(
            browserbase_session_id[:8],
            f"OUTBOUND message: {_message_preview(message)}",
        )

        await session.execute(
            agent_config={"model": model_name},
            execute_options={
                "instruction": _build_send_message_instruction(message),
                "max_steps": 8,
            },
        )

        _debug_print(
            browserbase_session_id[:8],
            f"sent message, waiting {reply_wait_ms}ms…",
        )
        await asyncio.sleep(reply_wait_ms / 1000)

        response = await session.extract(
            instruction=_build_reply_check_instruction(message),
            schema=REPLY_CHECK_SCHEMA,
            options={"model": model_name},
        )

        reply_data = _to_dict(response.data.result if response.data else None) or {}
        reply: str | None = None
        if reply_data.get("seller_replied"):
            raw_reply = reply_data.get("reply_message")
            if isinstance(raw_reply, str):
                reply = raw_reply.strip() or None

        result = {
            "sent_message": message,
            "reply_message": reply,
        }
        _debug_print(
            browserbase_session_id[:8],
            (
                f"INBOUND reply: {_message_preview(reply)}"
                if reply
                else "INBOUND reply: <none>"
            ),
        )
        return result



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