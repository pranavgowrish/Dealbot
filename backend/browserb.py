"""
Facebook Marketplace automation via local Stagehand sessions.

Required environment variables:
    MODEL_API_KEY

Optional environment variables:
    STAGEHAND_CONTEXT_ID       (persistent profile/context ID)
    BROWSERBASE_CONTEXT_ID     (legacy fallback for context ID)
    STAGEHAND_LOCAL_HEADLESS   (defaults to false)
    STAGEHAND_KEEP_OPEN_ON_DONE (defaults to false; debugging aid)
    STAGEHAND_AUTO_RECLAIM_CONTEXT (defaults to true)
"""

from __future__ import annotations

import json
import os
import re
import urllib.parse
import asyncio
import hashlib
import errno
import shutil
from pathlib import Path
import socket
import subprocess
import time
from typing import Any

from dotenv import load_dotenv
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
_CONTEXT_ID_SAFE_RE = re.compile(r"[^a-zA-Z0-9_.-]+")
_CONTEXTS_ROOT = Path(__file__).resolve().parent / ".stagehand_contexts"


def _slugify_context_id(raw: str) -> str:
    cleaned = _CONTEXT_ID_SAFE_RE.sub("-", raw).strip("-_.")
    return cleaned[:80] or "default"


def _context_id_from_listing_url(listing_url: str | None) -> str | None:
    if not listing_url:
        return None
    item_id = _listing_item_id(listing_url)
    if item_id:
        return f"listing-{item_id}"
    digest = hashlib.sha1(listing_url.encode("utf-8")).hexdigest()[:12]
    return f"listing-{digest}"


def _resolve_stagehand_context_id(
    context_id: str | None = None,
    *,
    listing_url: str | None = None,
) -> str:
    candidate = (
        context_id
        or os.environ.get("STAGEHAND_CONTEXT_ID")
        or os.environ.get("BROWSERBASE_CONTEXT_ID")
        or _context_id_from_listing_url(listing_url)
        or "dealbot-default"
    )
    return _slugify_context_id(candidate)


def _context_user_data_dir(context_id: str) -> Path:
    _CONTEXTS_ROOT.mkdir(parents=True, exist_ok=True)
    context_dir = _CONTEXTS_ROOT / context_id
    context_dir.mkdir(parents=True, exist_ok=True)
    return context_dir


def _should_seed_listing_contexts() -> bool:
    return _env_bool("STAGEHAND_SEED_LISTING_CONTEXTS", True)


def _is_context_dir_effectively_empty(context_dir: Path) -> bool:
    for child in context_dir.iterdir():
        if child.name in {"SingletonLock", "SingletonSocket", "SingletonCookie", "chrome.pid"}:
            continue
        return False
    return True


def _seed_context_from_primary_profile(context_id: str) -> None:
    if not _should_seed_listing_contexts():
        return
    if not context_id.startswith("listing-"):
        return

    primary_raw = os.environ.get("STAGEHAND_CONTEXT_ID") or os.environ.get(
        "BROWSERBASE_CONTEXT_ID"
    )
    if not primary_raw:
        return

    primary_context_id = _slugify_context_id(primary_raw)
    if primary_context_id == context_id:
        return

    target_dir = _context_user_data_dir(context_id)
    if not _is_context_dir_effectively_empty(target_dir):
        return

    source_dir = _context_user_data_dir(primary_context_id)
    if not source_dir.exists() or _is_context_dir_effectively_empty(source_dir):
        return

    skip_names = {
        "SingletonLock",
        "SingletonSocket",
        "SingletonCookie",
        "chrome.pid",
    }
    for entry in source_dir.iterdir():
        if entry.name in skip_names:
            continue
        destination = target_dir / entry.name
        try:
            if entry.is_dir():
                shutil.copytree(entry, destination, dirs_exist_ok=True)
            else:
                shutil.copy2(entry, destination)
        except OSError:
            # Best-effort profile seeding; continue with what copied successfully.
            continue


def _env_bool(name: str, default: bool) -> bool:
    raw = os.environ.get(name)
    if raw is None:
        return default
    value = raw.split("#", 1)[0].strip().lower()
    if not value:
        return default
    return value in {"1", "true", "yes", "on"}


def _local_headless() -> bool:
    return _env_bool("STAGEHAND_LOCAL_HEADLESS", False)


def _build_local_browser_config(context_id: str) -> dict[str, Any]:
    return {
        "type": "local",
        "launch_options": {
            "headless": _local_headless(),
            "user_data_dir": str(_context_user_data_dir(context_id)),
            "preserve_user_data_dir": True,
        },
    }


def _keep_open_on_done() -> bool:
    return _env_bool("STAGEHAND_KEEP_OPEN_ON_DONE", False)


def _extract_pid_from_singleton_lock(lock_path: Path) -> int | None:
    try:
        lock_target = os.readlink(lock_path)
    except OSError:
        return None
    pid_text = lock_target.rsplit("-", 1)[-1].strip()
    return int(pid_text) if pid_text.isdigit() else None


def _context_recorded_chrome_pid(context_id: str) -> int | None:
    pid_path = _context_user_data_dir(context_id) / "chrome.pid"
    if not pid_path.exists():
        return None
    try:
        pid_text = pid_path.read_text(encoding="utf-8").strip()
    except OSError:
        return None
    return int(pid_text) if pid_text.isdigit() else None


def _is_pid_running(pid: int) -> bool:
    if pid <= 0:
        return False
    try:
        os.kill(pid, 0)
    except OSError as exc:
        if exc.errno == errno.ESRCH:
            return False
        if exc.errno == errno.EPERM:
            return True
        return False
    return True


def _terminate_pid(pid: int, timeout_s: float = 5.0) -> bool:
    if not _is_pid_running(pid):
        return True
    try:
        os.kill(pid, 15)
    except OSError:
        return False
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if not _is_pid_running(pid):
            return True
        time.sleep(0.1)
    return not _is_pid_running(pid)


def _pid_command(pid: int) -> str:
    try:
        return subprocess.check_output(
            ["ps", "-p", str(pid), "-o", "command="],
            text=True,
        ).strip()
    except Exception:
        return ""


def _pid_owns_context(pid: int, context_id: str) -> bool:
    command = _pid_command(pid)
    if not command:
        return False
    return str(_context_user_data_dir(context_id)) in command


def _context_locked_by_live_process(context_id: str) -> int | None:
    context_dir = _context_user_data_dir(context_id)
    lock_path = context_dir / "SingletonLock"
    if not lock_path.exists() and not lock_path.is_symlink():
        return None
    pid = _extract_pid_from_singleton_lock(lock_path)
    if pid and _is_pid_running(pid):
        return pid
    return None


def _cleanup_stale_context_locks(context_id: str) -> None:
    context_dir = _context_user_data_dir(context_id)
    stale_lock_files = (
        context_dir / "SingletonLock",
        context_dir / "SingletonSocket",
        context_dir / "SingletonCookie",
        context_dir / "chrome.pid",
    )
    for path in stale_lock_files:
        try:
            if path.is_symlink() or path.exists():
                path.unlink()
        except OSError:
            pass


def _prepare_context_for_launch(context_id: str) -> None:
    _seed_context_from_primary_profile(context_id)
    # Stagehand fails with ECONNREFUSED when Chrome exits early because the
    # profile has an active ProcessSingleton lock.
    locked_pid = _context_locked_by_live_process(context_id)
    if locked_pid:
        reclaim_enabled = _env_bool("STAGEHAND_AUTO_RECLAIM_CONTEXT", True)
        recorded_pid = _context_recorded_chrome_pid(context_id)
        owns_context = _pid_owns_context(locked_pid, context_id)
        if reclaim_enabled and (recorded_pid == locked_pid or owns_context):
            _debug_print(
                context_id[:12],
                f"reclaiming locked context by stopping stale pid {locked_pid}…",
            )
            if not _terminate_pid(locked_pid):
                host = socket.gethostname()
                raise RuntimeError(
                    "Failed to reclaim Stagehand context lock; lock owner is "
                    f"still alive (context_id={context_id}, pid={locked_pid}, host={host}). "
                    "Close that browser process manually or switch context IDs."
                )
        else:
            host = socket.gethostname()
            raise RuntimeError(
                "Stagehand context is already in use by a running Chrome process "
                f"(context_id={context_id}, pid={locked_pid}, host={host}). "
                "Close the existing browser using this context, or use a different "
                "STAGEHAND_CONTEXT_ID."
            )
    _cleanup_stale_context_locks(context_id)


def _stagehand_client_config(
    *,
    timeout: float | None = None,
    keep_browser_open: bool | None = None,
) -> dict[str, Any]:
    shutdown_on_close = (
        not keep_browser_open
        if keep_browser_open is not None
        else not _keep_open_on_done()
    )
    config: dict[str, Any] = {
        "server": "local",
        "_strict_response_validation": True,
        "model_api_key": os.environ["MODEL_API_KEY"],
        # AsyncStagehand defaults this to True. Set explicitly so headful mode
        # actually opens visible windows when STAGEHAND_LOCAL_HEADLESS=false.
        "local_headless": _local_headless(),
        # Useful for debugging with headful mode where you want to inspect state.
        "local_shutdown_on_close": shutdown_on_close,
    }
    if timeout is not None:
        config["timeout"] = timeout
    return config

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

MESSAGE_SEND_CHECK_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "message_sent": {"type": "boolean"},
        "observed_buyer_message": {"type": ["string", "null"]},
    },
    "required": ["message_sent", "observed_buyer_message"],
}

BUYER_MESSAGE_EXISTS_SCHEMA: dict[str, Any] = {
    "type": "object",
    "properties": {
        "buyer_has_any_message": {"type": "boolean"},
    },
    "required": ["buyer_has_any_message"],
}

_CONTEXT_SEND_LOCKS: dict[str, asyncio.Lock] = {}
_ACTIVE_LISTING_SESSIONS: dict[str, dict[str, Any]] = {}


def _context_send_lock(context_id: str) -> asyncio.Lock:
    lock = _CONTEXT_SEND_LOCKS.get(context_id)
    if lock is None:
        lock = asyncio.Lock()
        _CONTEXT_SEND_LOCKS[context_id] = lock
    return lock


def _tile_windows_enabled() -> bool:
    return _env_bool("STAGEHAND_TILE_WINDOWS", True)


def _tile_chrome_windows() -> None:
    if _local_headless() or not _tile_windows_enabled():
        return

    script = """
tell application "Finder"
    set screenBounds to bounds of window of desktop
end tell

set x0 to item 1 of screenBounds
set y0 to item 2 of screenBounds
set x1 to item 3 of screenBounds
set y1 to item 4 of screenBounds

set screenW to (x1 - x0)
set screenH to (y1 - y0)
set halfW to (screenW div 2)
set halfH to (screenH div 2)

set placements to {¬
    {x0, y0, x0 + halfW, y0 + halfH}, ¬
    {x0 + halfW, y0, x0 + screenW, y0 + halfH}, ¬
    {x0, y0 + halfH, x0 + halfW, y0 + screenH}, ¬
    {x0 + halfW, y0 + halfH, x0 + screenW, y0 + screenH}}

tell application "Google Chrome"
    set winCount to count of windows
    set targetCount to winCount
    if targetCount > 4 then set targetCount to 4
    repeat with i from 1 to targetCount
        set bounds of window i to item i of placements
    end repeat
end tell
"""
    try:
        subprocess.run(
            ["osascript", "-e", script],
            capture_output=True,
            text=True,
            check=False,
        )
    except Exception:
        pass


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


def _build_message_sent_check_instruction(message: str) -> str:
    quoted = json.dumps(message)
    return f"""Inspect the currently open Facebook Marketplace chat.

Find the most recent message sent by the buyer in this thread.

Return:
- message_sent: true only if that most recent buyer message exactly matches {quoted}
- observed_buyer_message: that most recent buyer message text, or null if no buyer message is visible"""


def _build_buyer_message_exists_instruction() -> str:
    return """Inspect the currently open Facebook Marketplace chat.

Return:
- buyer_has_any_message: true if any message from the buyer is visible in this thread, otherwise false."""


async def _buyer_message_exists(session: Any, *, model_name: str) -> bool:
    response = await session.extract(
        instruction=_build_buyer_message_exists_instruction(),
        schema=BUYER_MESSAGE_EXISTS_SCHEMA,
        options={"model": model_name},
    )
    payload = _to_dict(response.data.result if response.data else None) or {}
    return bool(payload.get("buyer_has_any_message"))


async def _send_message_via_dom(session: Any, message: str) -> tuple[bool, bool]:
    cdp_url = getattr(getattr(session, "data", None), "cdp_url", None)
    if not cdp_url:
        return False, False

    from playwright.async_api import async_playwright

    js = """
(message) => {
  const msg = String(message || "");
  const isVisible = (el) => {
    if (!el) return false;
    const style = window.getComputedStyle(el);
    return style && style.visibility !== "hidden" && style.display !== "none";
  };
  const clickByText = (tokens) => {
    const all = Array.from(document.querySelectorAll('button, [role="button"], a, div, span'));
    for (const el of all) {
      const text = (el.innerText || el.textContent || "").trim().toLowerCase();
      if (!text) continue;
      if (!tokens.some((t) => text.includes(t))) continue;
      if (!isVisible(el)) continue;
      el.click();
      return true;
    }
    return false;
  };

  const loginRequired = Boolean(
    document.querySelector('input[name="email"], input#email, input[name="pass"], input#pass')
  );

  clickByText(["message seller", "message"]);

  const selectors = [
    'div[role="textbox"][contenteditable="true"]',
    'div[contenteditable="true"][aria-label*="message" i]',
    'div[contenteditable="true"][aria-placeholder*="message" i]',
    'textarea[aria-label*="message" i]',
    'textarea[placeholder*="message" i]',
    'textarea',
  ];

  let input = null;
  for (const selector of selectors) {
    for (const node of document.querySelectorAll(selector)) {
      if (!isVisible(node)) continue;
      input = node;
      break;
    }
    if (input) break;
  }

  if (!input) return { typed: false, login_required: loginRequired };

  input.focus();
  const tag = (input.tagName || "").toLowerCase();
  if (tag === "textarea" || tag === "input") {
    input.value = msg;
    input.dispatchEvent(new Event("input", { bubbles: true }));
    input.dispatchEvent(new Event("change", { bubbles: true }));
  } else {
    input.textContent = msg;
    input.dispatchEvent(new InputEvent("input", { bubbles: true, data: msg, inputType: "insertText" }));
  }

  const enter = { key: "Enter", code: "Enter", keyCode: 13, which: 13, bubbles: true, cancelable: true };
  input.dispatchEvent(new KeyboardEvent("keydown", enter));
  input.dispatchEvent(new KeyboardEvent("keypress", enter));
  input.dispatchEvent(new KeyboardEvent("keyup", enter));

  return { typed: true, login_required: loginRequired };
}
"""

    typed = False
    login_required = False
    playwright = await async_playwright().start()
    try:
        browser = await playwright.chromium.connect_over_cdp(cdp_url)
        if not browser.contexts:
            return False, False
        context = browser.contexts[0]
        if not context.pages:
            return False, False
        page = context.pages[-1]
        result = await page.evaluate(js, message)
        if isinstance(result, dict):
            typed = bool(result.get("typed"))
            login_required = bool(result.get("login_required"))
    finally:
        await playwright.stop()

    return typed, login_required


async def _send_message_and_confirm(
    session: Any,
    *,
    message: str,
    model_name: str,
) -> bool:
    dom_typed, login_required = await _send_message_via_dom(session, message)
    if login_required:
        raise RuntimeError(
            "Facebook login is required in this listing window before sending messages."
        )

    if dom_typed:
        confirmation = await session.extract(
            instruction=_build_message_sent_check_instruction(message),
            schema=MESSAGE_SEND_CHECK_SCHEMA,
            options={"model": model_name},
        )
        payload = _to_dict(confirmation.data.result if confirmation.data else None) or {}
        return bool(payload.get("message_sent"))

    # Fallback to structured "act" steps if direct DOM send did not confirm.
    attempts: list[list[str]] = [
        [
            'Click the "Message" or "Message seller" button to open the composer.',
            "Focus the chat input box where a new message can be typed.",
            f"Type this exact message into the focused input: {json.dumps(message)}",
            "Send by pressing Enter.",
        ],
        [
            "Focus the chat input box where a new message can be typed.",
            f"Type this exact message into the focused input: {json.dumps(message)}",
            'Click the "Send" button to submit the typed message.',
        ],
    ]

    for steps in attempts:
        try:
            for step in steps:
                await session.act(
                    input=step,
                    options={"model": model_name},
                )
        except Exception:
            # Continue into confirmation check/fallback attempt.
            pass

        confirmation = await session.extract(
            instruction=_build_message_sent_check_instruction(message),
            schema=MESSAGE_SEND_CHECK_SCHEMA,
            options={"model": model_name},
        )
        payload = _to_dict(confirmation.data.result if confirmation.data else None) or {}
        if payload.get("message_sent"):
            return True

    return False


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

    resolved_context_id = _resolve_stagehand_context_id(context_id)

    print(f"Stagehand context   : {resolved_context_id}")
    print(f"URL                 : {url}")
    print(f"Price range         : ${price_min:.2f} – ${price_max:.2f}")
    print(f"Location            : {location}")

    raw_data: dict[str, Any] | None = None
    listings: list[dict] = []
    dom_cards: list[dict[str, str]] = []

    async with AsyncStagehand(**_stagehand_client_config(timeout=extract_timeout_s)) as client:
        try:
            _prepare_context_for_launch(resolved_context_id)
            session = await client.sessions.start(
                model_name=model_name,
                browser=_build_local_browser_config(resolved_context_id),
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
                connect_url = session.data.cdp_url if session.data else None
                if connect_url:
                    dom_cards = await _extract_search_dom_cards(connect_url)
                    listings = _normalize_dom_listings(
                        dom_cards, price_min, price_max, max_results
                    )
                    print(
                        f"DOM extracted {len(dom_cards)} card(s); "
                        f"{len(listings)} in price range"
                    )
                else:
                    print("No CDP URL available; skipping DOM extraction fallback path")
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
            print(f"Context ID: {resolved_context_id}")
            if _keep_open_on_done():
                print(
                    "Keeping browser open for debugging "
                    "(STAGEHAND_KEEP_OPEN_ON_DONE=true); session not ended."
                )
            else:
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
    context_id: str | None = None,
    persistent_session: bool = False,
) -> dict[str, Any]:
    """
    Creates one local Stagehand session, opens the page,
    extracts listing details, and returns the result.
    """
    load_dotenv()

    resolved_context_id = _resolve_stagehand_context_id(context_id, listing_url=link)

    if persistent_session:
        _, session = await _get_or_create_active_listing_session(
            context_id=resolved_context_id,
            model_name=model_name,
        )
        data = await _extract_listing_from_open_session(
            session=session,
            link=link,
            model_name=model_name,
        )
        return {
            "stagehand_context_id": resolved_context_id,
            # Backward compatibility for older orchestrator code paths.
            "browserbase_session_id": resolved_context_id,
            "listing_url": link,
            "data": data,
        }

    async with AsyncStagehand(**_stagehand_client_config()) as client:
        try:
            _prepare_context_for_launch(resolved_context_id)
            session = await client.sessions.start(
                model_name=model_name,
                browser=_build_local_browser_config(resolved_context_id),
            )
        except APIResponseValidationError as e:
            print(f"Session schema error — HTTP {e.response.status_code}")
            print(e.response.text)
            raise

        if not session.id:
            raise RuntimeError(f"Expected session ID, got {session!r}")

        try:
            data = await _extract_listing_from_open_session(
                session=session,
                link=link,
                model_name=model_name,
            )
            return {
                "stagehand_context_id": resolved_context_id,
                # Backward compatibility for older orchestrator code paths.
                "browserbase_session_id": resolved_context_id,
                "listing_url": link,
                "data": data,
            }
        finally:
            if _keep_open_on_done():
                _debug_print(
                    resolved_context_id[:12],
                    "keeping session/browser open for debugging (not ending session).",
                )
            else:
                try:
                    await session.end()
                except Exception:
                    pass


async def _extract_listing_from_open_session(
    *,
    session: Any,
    link: str,
    model_name: str,
) -> dict[str, Any] | None:
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
    return data


async def _start_persistent_listing_session(
    *,
    context_id: str,
    model_name: str,
) -> tuple[Any, Any]:
    client = AsyncStagehand(
        **_stagehand_client_config(
            keep_browser_open=True,
        )
    )
    await client.__aenter__()
    try:
        _prepare_context_for_launch(context_id)
        session = await client.sessions.start(
            model_name=model_name,
            browser=_build_local_browser_config(context_id),
        )
        if not session.id:
            raise RuntimeError(f"Expected session ID, got {session!r}")
        _tile_chrome_windows()
        return client, session
    except Exception:
        try:
            await client.__aexit__(None, None, None)
        except Exception:
            pass
        raise


async def _get_or_create_active_listing_session(
    *,
    context_id: str,
    model_name: str,
) -> tuple[Any, Any]:
    existing = _ACTIVE_LISTING_SESSIONS.get(context_id)
    if existing:
        return existing["client"], existing["session"]

    client, session = await _start_persistent_listing_session(
        context_id=context_id,
        model_name=model_name,
    )
    _ACTIVE_LISTING_SESSIONS[context_id] = {
        "client": client,
        "session": session,
    }
    return client, session


async def _scrape_listings_with_shared_session(
    *,
    links: list[str],
    model_name: str,
    context_id: str,
    scrape_timeout_s: float,
) -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    resolved_context_id = _resolve_stagehand_context_id(context_id)
    print(
        f"Using one shared local browser window for all listings "
        f"(context={resolved_context_id}).",
        flush=True,
    )

    async with AsyncStagehand(**_stagehand_client_config()) as client:
        try:
            _prepare_context_for_launch(resolved_context_id)
            session = await client.sessions.start(
                model_name=model_name,
                browser=_build_local_browser_config(resolved_context_id),
            )
        except APIResponseValidationError as e:
            print(f"Session schema error — HTTP {e.response.status_code}")
            print(e.response.text)
            raise

        if not session.id:
            raise RuntimeError(f"Expected session ID, got {session!r}")

        try:
            for index, link in enumerate(links, start=1):
                label = _listing_id(link)
                print(f"[{index}/{len(links)}] starting {label}…", flush=True)
                try:
                    data = await asyncio.wait_for(
                        _extract_listing_from_open_session(
                            session=session,
                            link=link,
                            model_name=model_name,
                        ),
                        timeout=scrape_timeout_s,
                    )
                except asyncio.TimeoutError as exc:
                    raise RuntimeError(
                        f"Scrape timed out after {scrape_timeout_s:.0f}s for {link}"
                    ) from exc

                results.append(
                    {
                        "stagehand_context_id": resolved_context_id,
                        "browserbase_session_id": resolved_context_id,
                        "listing_url": link,
                        "data": data,
                    }
                )
        finally:
            if _keep_open_on_done():
                _debug_print(
                    resolved_context_id[:12],
                    "keeping shared session/browser open for debugging.",
                )
            else:
                try:
                    await session.end()
                except Exception:
                    pass

    return results


async def initialize_browsers(
    links: list[str],
    *,
    model_name: str = "anthropic/claude-sonnet-4-6",
    max_concurrency: int = 2,
    scrape_timeout_s: float = 120,
    context_id: str | None = None,
    use_shared_context: bool = True,
    keep_windows_open: bool = False,
) -> list[dict[str, Any]]:
    """
    Launches one local Stagehand session per URL
    and scrapes all pages concurrently.
    """
    load_dotenv()

    print(f"Scraping {len(links)} listing(s)…", flush=True)

    shared_context_id = None
    if use_shared_context:
        shared_context_id = (
            context_id
            or os.environ.get("STAGEHAND_CONTEXT_ID")
            or os.environ.get("BROWSERBASE_CONTEXT_ID")
        )

    effective_concurrency = max(1, max_concurrency)
    if shared_context_id and effective_concurrency > 1:
        print(
            "Shared Stagehand context detected; forcing max_concurrency=1 "
            "to avoid local profile lock conflicts.",
            flush=True,
        )
        effective_concurrency = 1

    if shared_context_id:
        results = await _scrape_listings_with_shared_session(
            links=links,
            model_name=model_name,
            context_id=shared_context_id,
            scrape_timeout_s=scrape_timeout_s,
        )
        print("\nFinal JSON:")
        print(json.dumps(results, indent=2))
        return results

    semaphore = asyncio.Semaphore(effective_concurrency)

    async def _run(link: str, index: int) -> dict[str, Any]:
        label = _listing_id(link)
        run_context_id = (
            shared_context_id
            if use_shared_context
            else _context_id_from_listing_url(link)
        )
        async with semaphore:
            print(f"[{index}/{len(links)}] starting {label}…", flush=True)
            try:
                return await asyncio.wait_for(
                    _scrape_listing(
                        link=link,
                        model_name=model_name,
                        context_id=run_context_id,
                        persistent_session=keep_windows_open,
                    ),
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
    stagehand_context_id: str,
    message: str,
    *,
    listing_url: str | None = None,
    model_name: str = "anthropic/claude-sonnet-4-6",
    reply_wait_ms: int = 2500,
    skip_if_buyer_message_exists: bool = False,
) -> dict[str, str | None]:
    """
    Start a local Stagehand session with a persisted context, open the listing
    message box, send a message, wait for a reply, and return only the sent
    message and any seller reply.
    """
    load_dotenv()
    if not listing_url:
        raise ValueError(
            "listing_url is required when sending messages via local Stagehand sessions"
        )
    resolved_context_id = _resolve_stagehand_context_id(
        stagehand_context_id,
        listing_url=listing_url,
    )

    lock = _context_send_lock(resolved_context_id)
    if lock.locked():
        _debug_print(
            resolved_context_id[:12],
            "another message send is in progress; waiting for context lock…",
        )

    async with lock:
        active_bundle = _ACTIVE_LISTING_SESSIONS.get(resolved_context_id)
        client: Any
        session: Any
        ephemeral_client = False

        if active_bundle:
            client = active_bundle["client"]
            session = active_bundle["session"]
            _debug_print(
                resolved_context_id[:12],
                "reusing existing listing window/session for message send.",
            )
        else:
            client = AsyncStagehand(**_stagehand_client_config())
            ephemeral_client = True
            await client.__aenter__()
            try:
                _prepare_context_for_launch(resolved_context_id)
                session = await client.sessions.start(
                    model_name=model_name,
                    browser=_build_local_browser_config(resolved_context_id),
                )
            except APIResponseValidationError as e:
                print(f"Session schema error — HTTP {e.response.status_code}")
                print(e.response.text)
                raise

            if not session.id:
                raise RuntimeError(f"Expected session ID, got {session!r}")

        _debug_print(
            resolved_context_id[:12],
            "session started, opening listing chat…",
        )
        _debug_print(
            resolved_context_id[:12],
            f"OUTBOUND message: {_message_preview(message)}",
        )

        try:
            await session.navigate(
                url=listing_url,
                options={"wait_until": "domcontentloaded"},
            )

            if skip_if_buyer_message_exists and await _buyer_message_exists(
                session,
                model_name=model_name,
            ):
                _debug_print(
                    resolved_context_id[:12],
                    "buyer message already exists in chat; skipping duplicate send.",
                )
                return {
                    "sent_message": None,
                    "reply_message": None,
                }

            sent_ok = await _send_message_and_confirm(
                session,
                message=message,
                model_name=model_name,
            )
            if not sent_ok:
                raise RuntimeError(
                    "Unable to confirm message was sent in the listing chat."
                )

            _debug_print(
                resolved_context_id[:12],
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
                resolved_context_id[:12],
                (
                    f"INBOUND reply: {_message_preview(reply)}"
                    if reply
                    else "INBOUND reply: <none>"
                ),
            )
            return result
        finally:
            if resolved_context_id in _ACTIVE_LISTING_SESSIONS:
                _tile_chrome_windows()
            elif _keep_open_on_done():
                _debug_print(
                    resolved_context_id[:12],
                    "keeping session/browser open for debugging (not ending session).",
                )
            else:
                try:
                    await session.end()
                except Exception:
                    pass
            if ephemeral_client:
                try:
                    await client.__aexit__(None, None, None)
                except Exception:
                    pass



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