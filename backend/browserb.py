"""
Facebook Marketplace automation via Stagehand sessions.

Required environment variables:
    MODEL_API_KEY

Optional environment variables:
    STAGEHAND_CONTEXT_ID       (persistent profile/context ID)
    BROWSERBASE_CONTEXT_ID     (legacy fallback for context ID)
    STAGEHAND_USE_BROWSERBASE_CONTEXT (defaults to false; use Browserbase context mode)
    BROWSERBASE_API_KEY        (required when Browserbase context mode is enabled)
    BROWSERBASE_PROJECT_ID     (required when Browserbase context mode is enabled)
    STAGEHAND_LOCAL_HEADLESS   (defaults to false)
    STAGEHAND_KEEP_OPEN_ON_DONE (defaults to false; debugging aid)
    STAGEHAND_AUTO_RECLAIM_CONTEXT (defaults to true)
    STAGEHAND_LOCAL_CLONE_SHARED_CONTEXTS (defaults to true)
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

from browserbase import Browserbase
from dotenv import load_dotenv
from stagehand import AsyncStagehand, APIResponseValidationError


# ---------------------------------------------------------------------------
# Anthropic base URL normalization
# ---------------------------------------------------------------------------
# Some host environments (e.g. Claude Desktop) export
# ANTHROPIC_BASE_URL=https://api.anthropic.com — i.e. the bare host with no
# "/v1" path segment. The Python SDK tolerates this (it appends "/v1/messages"),
# but Stagehand's bundled Node SDK (@ai-sdk/anthropic) treats the value as the
# full base and appends only "/messages", producing
# "https://api.anthropic.com/messages" → HTTP 404 "Not Found" on every extract
# and act call. Drop the bare-host override so both SDKs fall back to their
# correct defaults (Python → host + /v1/messages, Node → host/v1 + /messages).
def _normalize_anthropic_base_url() -> None:
    raw = os.environ.get("ANTHROPIC_BASE_URL", "").strip()
    if not raw:
        return
    normalized = raw.rstrip("/")
    if normalized in {
        "https://api.anthropic.com",
        "http://api.anthropic.com",
    }:
        os.environ.pop("ANTHROPIC_BASE_URL", None)


_normalize_anthropic_base_url()


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
_BROWSERBASE_CONTEXT_PREFIX = "browserbase-context:"


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


def _resolve_browserbase_context_id(context_id: str | None = None) -> str:
    candidate = (
        context_id
        or os.environ.get("STAGEHAND_CONTEXT_ID")
        or os.environ.get("BROWSERBASE_CONTEXT_ID")
    )
    if not candidate:
        raise RuntimeError(
            "Browserbase context mode requires STAGEHAND_CONTEXT_ID "
            "(or BROWSERBASE_CONTEXT_ID) to be set to a pre-authenticated context ID."
        )
    raw = candidate.strip()
    if raw.startswith(_BROWSERBASE_CONTEXT_PREFIX):
        return raw[len(_BROWSERBASE_CONTEXT_PREFIX) :]
    return raw


def _encode_context_ref(context_id: str, *, browserbase_context_mode: bool) -> str:
    if browserbase_context_mode:
        return f"{_BROWSERBASE_CONTEXT_PREFIX}{context_id}"
    return context_id


def _decode_context_ref(context_ref: str | None) -> tuple[bool, str]:
    raw = (context_ref or "").strip()
    if raw.startswith(_BROWSERBASE_CONTEXT_PREFIX):
        return True, raw[len(_BROWSERBASE_CONTEXT_PREFIX) :]
    return False, raw


def _context_user_data_dir(context_id: str) -> Path:
    _CONTEXTS_ROOT.mkdir(parents=True, exist_ok=True)
    context_dir = _CONTEXTS_ROOT / context_id
    context_dir.mkdir(parents=True, exist_ok=True)
    return context_dir


def _should_seed_listing_contexts() -> bool:
    return _env_bool("STAGEHAND_SEED_LISTING_CONTEXTS", True)


def _should_clone_shared_local_contexts() -> bool:
    return _env_bool("STAGEHAND_LOCAL_CLONE_SHARED_CONTEXTS", True)


def _is_context_dir_effectively_empty(context_dir: Path) -> bool:
    for child in context_dir.iterdir():
        if child.name in {"SingletonLock", "SingletonSocket", "SingletonCookie", "chrome.pid"}:
            continue
        return False
    return True


# Files that are process-runtime locks/sockets, never auth state. Never copied
# (they'd point a clone at the source browser's live process) and never deleted
# from a target we're about to clear (a live browser may hold them).
_PROFILE_RUNTIME_FILES = {
    "SingletonLock",
    "SingletonSocket",
    "SingletonCookie",
    "chrome.pid",
}


def _copy_profile_contents(source_dir: Path, target_dir: Path) -> None:
    """Copy a Chromium user-data-dir's contents (auth, cookies, prefs) over."""
    target_dir.mkdir(parents=True, exist_ok=True)
    for entry in source_dir.iterdir():
        if entry.name in _PROFILE_RUNTIME_FILES:
            continue
        destination = target_dir / entry.name
        try:
            if entry.is_dir():
                shutil.copytree(entry, destination, dirs_exist_ok=True)
            else:
                shutil.copy2(entry, destination)
        except OSError:
            # Best-effort; continue with whatever copied successfully.
            continue


def _clear_profile_contents(target_dir: Path) -> None:
    """Remove a profile dir's stale contents so it can be re-cloned fresh.

    Leaves the runtime lock/socket files alone (a live browser may hold them)
    and never deletes the dir inode itself.
    """
    if not target_dir.exists():
        return
    for entry in target_dir.iterdir():
        if entry.name in _PROFILE_RUNTIME_FILES:
            continue
        try:
            if entry.is_dir():
                shutil.rmtree(entry, ignore_errors=True)
            else:
                entry.unlink(missing_ok=True)
        except OSError:
            continue


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

    source_dir = _context_user_data_dir(primary_context_id)
    if not source_dir.exists() or _is_context_dir_effectively_empty(source_dir):
        return

    # Always refresh from the (logged-in) primary profile. This previously
    # skipped whenever the target already existed, so a listing profile created
    # during an earlier signed-out run kept its stale cookies forever — opening
    # logged out even after the operator authenticated the base profile.
    target_dir = _context_user_data_dir(context_id)
    _clear_profile_contents(target_dir)
    _copy_profile_contents(source_dir, target_dir)


def _clone_local_context_from_base(base_context_id: str, clone_context_id: str) -> None:
    resolved_base_id = _slugify_context_id(base_context_id)
    resolved_clone_id = _slugify_context_id(clone_context_id)
    if resolved_base_id == resolved_clone_id:
        return

    source_dir = _context_user_data_dir(resolved_base_id)
    if not source_dir.exists() or _is_context_dir_effectively_empty(source_dir):
        raise RuntimeError(
            "Cannot clone local Stagehand context because the base profile is empty. "
            "Log in once with STAGEHAND_CONTEXT_ID set to the base context, then retry."
        )

    # Always re-clone so per-listing windows inherit the base profile's CURRENT
    # auth state. Skipping when the clone dir already existed (from a prior run)
    # left stale signed-out cookies in place — which is why the per-listing
    # browsers opened logged out even though the base/search window was signed in.
    target_dir = _context_user_data_dir(resolved_clone_id)
    _clear_profile_contents(target_dir)
    _copy_profile_contents(source_dir, target_dir)


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


def _use_browserbase_context_mode() -> bool:
    return _env_bool("STAGEHAND_USE_BROWSERBASE_CONTEXT", False)


def _browserbase_api_key() -> str:
    api_key = os.environ.get("BROWSERBASE_API_KEY", "").strip()
    if not api_key:
        raise RuntimeError(
            "BROWSERBASE_API_KEY is required when STAGEHAND_USE_BROWSERBASE_CONTEXT=true."
        )
    return api_key


def _browserbase_project_id() -> str:
    project_id = os.environ.get("BROWSERBASE_PROJECT_ID", "").strip()
    if not project_id:
        raise RuntimeError(
            "BROWSERBASE_PROJECT_ID is required when STAGEHAND_USE_BROWSERBASE_CONTEXT=true."
        )
    return project_id


def _create_browserbase_context() -> str:
    browserbase = Browserbase(api_key=_browserbase_api_key())
    created = browserbase.contexts.create(project_id=_browserbase_project_id())
    context_id = getattr(created, "id", None)
    if not context_id:
        raise RuntimeError("Browserbase context creation returned no context ID.")
    return str(context_id)


def _create_browserbase_session_id(context_id: str, *, persist: bool) -> str:
    browserbase = Browserbase(api_key=_browserbase_api_key())
    created = browserbase.sessions.create(
        project_id=_browserbase_project_id(),
        browser_settings={
            "context": {
                "id": context_id,
                "persist": persist,
            }
        },
    )
    session_id = getattr(created, "id", None)
    if not session_id:
        raise RuntimeError("Browserbase session creation returned no session ID.")
    return str(session_id)


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
    use_browserbase_context: bool = False,
) -> dict[str, Any]:
    shutdown_on_close = (
        not keep_browser_open
        if keep_browser_open is not None
        else not _keep_open_on_done()
    )
    config: dict[str, Any]
    if use_browserbase_context:
        config = {
            "server": "remote",
            "_strict_response_validation": True,
            "model_api_key": os.environ["MODEL_API_KEY"],
            "browserbase_api_key": _browserbase_api_key(),
            "browserbase_project_id": _browserbase_project_id(),
        }
    else:
        config = {
            "server": "local",
            "_strict_response_validation": True,
            "model_api_key": os.environ["MODEL_API_KEY"],
            # AsyncStagehand defaults this to True. Set explicitly so headful mode
            # actually opens visible windows when STAGEHAND_LOCAL_HEADLESS=false.
            "local_headless": _local_headless(),
            # Useful for debugging with headful mode where you want to inspect state.
            "local_shutdown_on_close": shutdown_on_close,
            # The default 10s is too tight when the laptop is busy / the SEA
            # server is cold-starting, which surfaced as "SEA server not ready".
            "local_ready_timeout_s": float(
                os.environ.get("STAGEHAND_LOCAL_READY_TIMEOUT_S", "30")
            ),
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


def _listing_extract_timeout_s() -> float:
    raw = os.environ.get("STAGEHAND_LISTING_EXTRACT_TIMEOUT_S", "35").strip()
    try:
        value = float(raw)
    except ValueError:
        value = 35.0
    return max(5.0, value)


def _listing_extract_retries() -> int:
    raw = os.environ.get("STAGEHAND_LISTING_EXTRACT_RETRIES", "2").strip()
    try:
        value = int(raw)
    except ValueError:
        value = 2
    return max(1, value)


def _minimal_listing_data(link: str) -> dict[str, str]:
    item_id = _listing_item_id(link) or "unknown"
    return {
        "title": f"Marketplace listing {item_id}",
        "price": "Unknown",
        "description": "",
        "condition": "",
        "seller_name": "seller",
        "location": "unknown",
        "date_listed": "",
        "reason_for_selling": "",
    }


def _merge_listing_data(primary: dict[str, Any] | None, link: str) -> dict[str, str]:
    merged = _minimal_listing_data(link)
    if not isinstance(primary, dict):
        return merged

    for key in (
        "title",
        "price",
        "description",
        "condition",
        "seller_name",
        "location",
        "date_listed",
        "reason_for_selling",
    ):
        value = primary.get(key)
        if isinstance(value, str):
            text = value.strip()
            if text:
                merged[key] = text
    return merged


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
        "condition": {"type": "string"},
        "seller_name": {"type": "string"},
        "location": {"type": "string"},
        "date_listed": {"type": "string"},
        "reason_for_selling": {"type": "string"},
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


def _listing_session_key(
    resolved_context_id: str,
    *,
    use_browserbase_context: bool,
    listing_url: str | None,
) -> str:
    """
    Cache key for a reusable chat session.

    Local contexts already get one window per context, so the context id is
    enough. In shared Browserbase mode every worker shares a single context, so
    we key per listing URL to keep one open chat window per listing instead of
    booting a fresh cloud session for every message.
    """
    if use_browserbase_context and listing_url:
        return f"bb::{resolved_context_id}::{listing_url}"
    return resolved_context_id


async def _acquire_listing_session(
    *,
    session_key: str,
    resolved_context_id: str,
    model_name: str,
    use_browserbase_context: bool,
) -> tuple[dict[str, Any], bool]:
    """
    Return ``(bundle, created)`` for a chat session, reusing a cached one when
    present. New bundles are stored in ``_ACTIVE_LISTING_SESSIONS`` so the open
    chat window survives across messages; ``created`` is True only when a brand
    new session was started (so the caller knows it still needs to navigate).
    """
    bundle = _ACTIVE_LISTING_SESSIONS.get(session_key)
    if bundle is not None:
        return bundle, False

    client = AsyncStagehand(
        **_stagehand_client_config(use_browserbase_context=use_browserbase_context)
    )
    await client.__aenter__()
    try:
        session = await _start_stagehand_session(
            client=client,
            model_name=model_name,
            context_id=resolved_context_id,
            use_browserbase_context=use_browserbase_context,
        )
    except APIResponseValidationError as e:
        print(f"Session schema error — HTTP {e.response.status_code}")
        print(e.response.text)
        try:
            await client.__aexit__(None, None, None)
        except Exception:
            pass
        raise
    except BaseException:
        try:
            await client.__aexit__(None, None, None)
        except Exception:
            pass
        raise

    bundle = {"client": client, "session": session, "url": None}
    _ACTIVE_LISTING_SESSIONS[session_key] = bundle
    return bundle, True


async def _close_listing_bundle(session_key: str) -> bool:
    """End and discard a cached chat session bundle. Returns True if one existed."""
    bundle = _ACTIVE_LISTING_SESSIONS.pop(session_key, None)
    if not bundle:
        return False
    session = bundle.get("session")
    client = bundle.get("client")
    try:
        if session is not None:
            await session.end()
    except Exception:
        pass
    try:
        if client is not None:
            await client.__aexit__(None, None, None)
    except Exception:
        pass
    _CONTEXT_SEND_LOCKS.pop(session_key, None)
    return True


async def close_all_active_listing_sessions() -> int:
    """
    Close every cached chat session. Call at the end of a negotiation job so
    reused (especially Browserbase) sessions never leak past the run.
    """
    closed = 0
    for session_key in list(_ACTIVE_LISTING_SESSIONS.keys()):
        if await _close_listing_bundle(session_key):
            closed += 1
    return closed


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
set fifthW to ((screenW * 7) div 10)
set fifthH to ((screenH * 7) div 10)
set fifthLeft to x0 + ((screenW - fifthW) div 2)
set fifthTop to y0 + ((screenH - fifthH) div 2)

set placements to {¬
    {x0, y0, x0 + halfW, y0 + halfH}, ¬
    {x0 + halfW, y0, x0 + screenW, y0 + halfH}, ¬
    {x0, y0 + halfH, x0 + halfW, y0 + screenH}, ¬
    {x0 + halfW, y0 + halfH, x0 + screenW, y0 + screenH}, ¬
    {fifthLeft, fifthTop, fifthLeft + fifthW, fifthTop + fifthH}}

tell application "Google Chrome"
    set winCount to count of windows
    set targetCount to winCount
    if targetCount > 5 then set targetCount to 5
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


async def _send_message_via_dom(session: Any, message: str) -> tuple[bool, bool, bool]:
    """
    Inject and send a chat message via direct DOM manipulation.

    Returns (typed, login_required, sent_likely):
      - typed:        the message text was placed into the composer.
      - login_required: a Facebook login form is blocking the chat.
      - sent_likely:  after pressing Enter/Send the composer cleared, which is a
                      strong signal the message actually went out — lets the
                      caller skip the slow LLM confirmation on the happy path.
    """
    cdp_url = getattr(getattr(session, "data", None), "cdp_url", None)
    if not cdp_url:
        return False, False, False

    from playwright.async_api import async_playwright

    # The composer does not exist until the "Message" button is clicked and
    # Facebook finishes mounting the chat, so this waits for it to appear and
    # inserts text the way the editor (Lexical/Draft) actually registers it.
    js = """
(message) => {
  const msg = String(message || "");
  const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
  const isVisible = (el) => {
    if (!el) return false;
    const style = window.getComputedStyle(el);
    if (!style || style.visibility === "hidden" || style.display === "none") return false;
    const rect = el.getBoundingClientRect();
    return rect.width > 0 && rect.height > 0;
  };
  const loginRequired = () => Boolean(
    document.querySelector('input[name="email"], input#email, input[name="pass"], input#pass')
  );
  const findComposer = () => {
    const selectors = [
      'div[role="textbox"][contenteditable="true"][aria-label*="message" i]',
      'div[contenteditable="true"][aria-label*="message" i]',
      'div[contenteditable="true"][aria-placeholder*="message" i]',
      'div[role="textbox"][contenteditable="true"]',
      'textarea[aria-label*="message" i]',
      'textarea[placeholder*="message" i]',
      'textarea',
    ];
    for (const selector of selectors) {
      for (const node of document.querySelectorAll(selector)) {
        if (isVisible(node)) return node;
      }
    }
    return null;
  };
  const labelOf = (el) => (
    (el.getAttribute && el.getAttribute("aria-label")) || el.innerText || el.textContent || ""
  ).trim().toLowerCase();
  const clickMessageButton = () => {
    const candidates = Array.from(
      document.querySelectorAll('div[role="button"], button, [aria-label]')
    );
    for (const el of candidates) {
      const label = labelOf(el);
      if (!label) continue;
      const exact = label === "message" || label === "message seller" || label === "send message";
      const prefixed = label.startsWith("message ") && label.length < 24;
      if (!exact && !prefixed) continue;
      if (!isVisible(el)) continue;
      el.click();
      return true;
    }
    return false;
  };
  const clickSend = () => {
    for (const el of document.querySelectorAll('div[role="button"], button')) {
      if (labelOf(el) === "send" && isVisible(el)) { el.click(); return true; }
    }
    return false;
  };
  const fireEnter = (el) => {
    const init = { key: "Enter", code: "Enter", keyCode: 13, which: 13, bubbles: true, cancelable: true };
    el.dispatchEvent(new KeyboardEvent("keydown", init));
    el.dispatchEvent(new KeyboardEvent("keypress", init));
    el.dispatchEvent(new KeyboardEvent("keyup", init));
  };
  const contentOf = (el) => (el.value != null ? el.value : (el.innerText || el.textContent || ""));

  return (async () => {
    if (loginRequired()) return { typed: false, login_required: true, sent_likely: false };

    let input = findComposer();
    if (!input) clickMessageButton();
    for (let i = 0; i < 25 && !input; i++) {  // poll up to ~5s for the composer
      await sleep(200);
      input = findComposer();
    }
    if (!input) return { typed: false, login_required: loginRequired(), sent_likely: false };

    // Tag the composer so Playwright can drive it with REAL keystrokes from
    // Python. execCommand-based clears do not work reliably on Facebook's
    // Lexical editor (that caused messages to concatenate); real key events do.
    document.querySelectorAll('[data-dealbot-composer]').forEach((n) => n.removeAttribute('data-dealbot-composer'));
    input.setAttribute('data-dealbot-composer', '1');
    input.focus();
    return {
      typed: false,
      login_required: false,
      sent_likely: false,
      composer_found: true,
    };
  })();
}
"""

    typed = False
    login_required = False
    sent_likely = False

    def _norm(value: str) -> str:
        return " ".join((value or "").split())

    select_all_js = """
() => {
  const el = document.querySelector('[data-dealbot-composer="1"]');
  if (!el) return false;
  el.focus();
  const sel = window.getSelection();
  const range = document.createRange();
  range.selectNodeContents(el);
  sel.removeAllRanges();
  sel.addRange(range);
  return true;
}
"""
    read_js = """
() => {
  const el = document.querySelector('[data-dealbot-composer="1"]');
  if (!el) return "";
  return (el.value != null ? el.value : (el.innerText || el.textContent || ""));
}
"""

    playwright = await async_playwright().start()
    try:
        browser = await playwright.chromium.connect_over_cdp(cdp_url)
        if not browser.contexts:
            return False, False, False
        context = browser.contexts[0]
        if not context.pages:
            return False, False, False
        page = context.pages[-1]

        # The JS blob opens the composer (clicking "Message" if needed), waits for
        # it to mount, and tags it with data-dealbot-composer="1".
        prep = await page.evaluate(js, message)
        if not isinstance(prep, dict):
            return False, False, False
        if prep.get("login_required"):
            return False, True, False
        if not prep.get("composer_found"):
            return False, False, False

        composer = page.locator('[data-dealbot-composer="1"]').last
        try:
            await composer.click(timeout=4000)
        except Exception:
            pass

        async def _clear_and_type() -> bool:
            # Select all existing content via the Selection API, then delete it
            # with a REAL Backspace keystroke (Lexical honors real key events, not
            # execCommand) so retries never concatenate onto leftover text.
            await page.evaluate(select_all_js)
            await page.keyboard.press("Backspace")
            await asyncio.sleep(0.05)
            await page.keyboard.insert_text(message)
            await asyncio.sleep(0.1)
            current = await page.evaluate(read_js)
            return _norm(current) == _norm(message)

        typed = await _clear_and_type()
        if not typed:
            typed = await _clear_and_type()
        if not typed:
            return False, False, False

        await page.keyboard.press("Enter")
        await asyncio.sleep(0.3)
        after = await page.evaluate(read_js)
        sent_likely = _norm(message) not in _norm(after)
        if not sent_likely:
            # Enter did not submit — click the Send button WITHOUT re-typing.
            try:
                send_btn = page.get_by_role(
                    "button", name=re.compile(r"^send$", re.I)
                )
                if await send_btn.count() > 0:
                    await send_btn.first.click(timeout=3000)
                    await asyncio.sleep(0.3)
                    after = await page.evaluate(read_js)
                    sent_likely = _norm(message) not in _norm(after)
            except Exception:
                pass
    finally:
        await playwright.stop()

    return typed, login_required, sent_likely


async def _send_message_and_confirm(
    session: Any,
    *,
    message: str,
    model_name: str,
) -> bool:
    dom_typed, login_required, sent_likely = await _send_message_via_dom(session, message)
    if login_required:
        raise RuntimeError(
            "Facebook login is required in this listing window before sending messages."
        )

    # Happy path: the composer cleared right after we pressed Enter/Send, so the
    # message almost certainly went out. Trust it and skip the LLM confirm to
    # save a slow extract round-trip on every negotiation message.
    if dom_typed and sent_likely:
        return True

    if dom_typed:
        # Typed cleanly but couldn't confirm the send locally — verify with one
        # extract. We do NOT re-type via act(): re-typing into a composer that
        # may still hold the text is what produced the concatenated "bulk"
        # messages. If it isn't confirmed, soft-fail and let the next round retry
        # (the DOM path always clears the composer before typing).
        confirmation = await session.extract(
            instruction=_build_message_sent_check_instruction(message),
            schema=MESSAGE_SEND_CHECK_SCHEMA,
            options={"model": model_name},
        )
        payload = _to_dict(confirmation.data.result if confirmation.data else None) or {}
        return bool(payload.get("message_sent"))

    # DOM send could not even place the text — soft-fail; the worker retries next
    # round. Avoid LLM act() re-typing, which concatenates and burns credits.
    return False


async def _start_stagehand_session(
    *,
    client: AsyncStagehand,
    model_name: str,
    context_id: str,
    use_browserbase_context: bool,
    persist_context: bool = False,
) -> Any:
    if use_browserbase_context:
        browserbase_session_id = await asyncio.to_thread(
            _create_browserbase_session_id,
            context_id,
            persist=persist_context,
        )
        session = await client.sessions.start(
            model_name=model_name,
            browserbase_session_id=browserbase_session_id,
        )
    else:
        _prepare_context_for_launch(context_id)
        session = await client.sessions.start(
            model_name=model_name,
            browser=_build_local_browser_config(context_id),
        )
    if not session.id:
        raise RuntimeError(f"Expected session ID, got {session!r}")
    return session


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

async def generate_browserbase_login_context(
    *,
    context_id: str | None = None,
    model_name: str = "anthropic/claude-sonnet-4-6",
    login_url: str = "https://www.facebook.com/login",
) -> str:
    """
    One-time setup helper:
      1) create (or reuse) a Browserbase context
      2) start a persist=true session
      3) navigate to login page and wait for manual login confirmation
      4) close session so cookies/tokens are saved to context storage
    """
    load_dotenv()
    resolved_context_id = (
        context_id.strip()
        if context_id and context_id.strip()
        else await asyncio.to_thread(_create_browserbase_context)
    )
    print(
        "Browserbase auth context ready. "
        f"Use this in workers: {resolved_context_id}",
        flush=True,
    )

    async with AsyncStagehand(
        **_stagehand_client_config(use_browserbase_context=True)
    ) as client:
        session = await _start_stagehand_session(
            client=client,
            model_name=model_name,
            context_id=resolved_context_id,
            use_browserbase_context=True,
            persist_context=True,
        )
        try:
            await session.navigate(
                url=login_url,
                options={"wait_until": "domcontentloaded"},
            )
            print(
                "Complete login in this session, then press Enter to persist auth.",
                flush=True,
            )
            try:
                await asyncio.to_thread(input)
            except EOFError:
                # Non-interactive environments can still persist whatever state exists.
                pass
        finally:
            try:
                await session.end()
            except Exception:
                pass
    return resolved_context_id


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
    use_browserbase_context: bool = False,
) -> dict[str, Any]:
    """
    Creates one local Stagehand session, opens the page,
    extracts listing details, and returns the result.
    """
    load_dotenv()

    resolved_context_id = (
        _resolve_browserbase_context_id(context_id)
        if use_browserbase_context
        else _resolve_stagehand_context_id(context_id, listing_url=link)
    )
    encoded_context_ref = _encode_context_ref(
        resolved_context_id,
        browserbase_context_mode=use_browserbase_context,
    )

    if persistent_session and use_browserbase_context:
        raise RuntimeError(
            "persistent_session is not supported in Browserbase context mode."
        )

    if persistent_session:
        _, session = await _get_or_create_active_listing_session(
            context_id=resolved_context_id,
            model_name=model_name,
        )
        try:
            data = await _extract_listing_from_open_session(
                session=session,
                link=link,
                model_name=model_name,
            )
        except Exception as exc:
            _debug_print(_listing_id(link), f"listing scrape failed, using fallback: {exc}")
            data = _minimal_listing_data(link)
        return {
            "stagehand_context_id": encoded_context_ref,
            # Backward compatibility for older orchestrator code paths.
            "browserbase_session_id": encoded_context_ref,
            "listing_url": link,
            "data": data,
        }

    async with AsyncStagehand(
        **_stagehand_client_config(use_browserbase_context=use_browserbase_context)
    ) as client:
        try:
            session = await _start_stagehand_session(
                client=client,
                model_name=model_name,
                context_id=resolved_context_id,
                use_browserbase_context=use_browserbase_context,
            )
        except APIResponseValidationError as e:
            print(f"Session schema error — HTTP {e.response.status_code}")
            print(e.response.text)
            raise

        try:
            try:
                data = await _extract_listing_from_open_session(
                    session=session,
                    link=link,
                    model_name=model_name,
                )
            except Exception as exc:
                _debug_print(_listing_id(link), f"listing scrape failed, using fallback: {exc}")
                data = _minimal_listing_data(link)
            return {
                "stagehand_context_id": encoded_context_ref,
                # Backward compatibility for older orchestrator code paths.
                "browserbase_session_id": encoded_context_ref,
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

    extract_timeout_s = _listing_extract_timeout_s()
    max_attempts = _listing_extract_retries()
    for attempt in range(1, max_attempts + 1):
        try:
            response = await asyncio.wait_for(
                session.extract(
                    instruction=(
                        "Extract the Facebook Marketplace listing details visible on this page, "
                        "including title, price, description, condition, seller name, and location. "
                        "If visible, also extract date_listed text (for example: 'listed 2 weeks ago') "
                        "and reason_for_selling from the listing description or seller notes; "
                        "otherwise use an empty string for optional fields."
                    ),
                    schema=LISTING_SCHEMA,
                    options={"model": model_name},
                ),
                timeout=extract_timeout_s,
            )
            raw_data = _to_dict(response.data.result if response.data else None)
            data = _merge_listing_data(raw_data, link)
            _debug_print(
                label,
                f"{data.get('title', '?')} — {data.get('price', '?')} "
                f"({data.get('location', '?')})",
            )
            return data
        except Exception as exc:
            _debug_print(
                label,
                f"extract attempt {attempt}/{max_attempts} failed: {exc}",
            )
            if attempt < max_attempts:
                await asyncio.sleep(0.4)

    _debug_print(label, "extract failed; using minimal fallback listing payload.")
    return _minimal_listing_data(link)


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


async def close_active_listing_browser(
    stagehand_context_id: str,
    *,
    listing_url: str | None = None,
) -> bool:
    """
    Close the reusable chat session for a worker. In shared Browserbase mode the
    session is keyed per listing, so ``listing_url`` is required to target it;
    local contexts are keyed by context id. Returns True when a live bundle
    existed and was closed.
    """
    browserbase_mode, decoded_context_id = _decode_context_ref(stagehand_context_id)
    if browserbase_mode:
        if not listing_url:
            return False
        resolved_context_id = _resolve_browserbase_context_id(decoded_context_id)
    else:
        resolved_context_id = _resolve_stagehand_context_id(decoded_context_id)

    session_key = _listing_session_key(
        resolved_context_id,
        use_browserbase_context=browserbase_mode,
        listing_url=listing_url,
    )
    return await _close_listing_bundle(session_key)


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
                    _debug_print(
                        label,
                        f"shared scrape timed out after {scrape_timeout_s:.0f}s; using fallback.",
                    )
                    data = _minimal_listing_data(link)
                except Exception as exc:
                    _debug_print(label, f"shared scrape failed; using fallback: {exc}")
                    data = _minimal_listing_data(link)

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
    Launches one Stagehand session per URL
    and scrapes all pages concurrently.
    """
    load_dotenv()

    print(f"Scraping {len(links)} listing(s)…", flush=True)

    shared_context_id: str | None = None
    if use_shared_context:
        shared_context_id = (
            context_id
            or os.environ.get("STAGEHAND_CONTEXT_ID")
            or os.environ.get("BROWSERBASE_CONTEXT_ID")
        )
    per_link_context_overrides: dict[str, str] = {}
    browserbase_shared_context_mode = bool(
        shared_context_id and _use_browserbase_context_mode()
    )
    if browserbase_shared_context_mode:
        print(
            "Using Browserbase shared context mode (persist=false workers) "
            "for concurrent listings.",
            flush=True,
        )

    effective_concurrency = max(1, max_concurrency)
    if (
        shared_context_id
        and not browserbase_shared_context_mode
        and effective_concurrency > 1
    ):
        if _should_clone_shared_local_contexts():
            resolved_shared_context_id = _resolve_stagehand_context_id(shared_context_id)
            print(
                "Shared local context detected; cloning it into isolated "
                "per-listing profiles for concurrent runs.",
                flush=True,
            )
            for index, link in enumerate(links, start=1):
                listing_context = _context_id_from_listing_url(link) or f"listing-{index}"
                cloned_context_id = _slugify_context_id(
                    f"{resolved_shared_context_id}-{listing_context}"
                )
                _clone_local_context_from_base(
                    resolved_shared_context_id,
                    cloned_context_id,
                )
                per_link_context_overrides[link] = cloned_context_id
        else:
            print(
                "Shared Stagehand context detected; forcing max_concurrency=1 "
                "to avoid local profile lock conflicts.",
                flush=True,
            )
            effective_concurrency = 1

    if (
        shared_context_id
        and not browserbase_shared_context_mode
        and not per_link_context_overrides
        and not keep_windows_open
    ):
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
        run_context_id = per_link_context_overrides.get(link) or (
            shared_context_id
            if use_shared_context
            else _context_id_from_listing_url(link)
        )
        run_browserbase_context_mode = bool(
            browserbase_shared_context_mode and run_context_id
        )
        resolved_run_context_id = (
            _resolve_browserbase_context_id(run_context_id)
            if run_browserbase_context_mode
            else _resolve_stagehand_context_id(run_context_id, listing_url=link)
        )
        encoded_run_context_ref = _encode_context_ref(
            resolved_run_context_id,
            browserbase_context_mode=run_browserbase_context_mode,
        )
        async with semaphore:
            print(f"[{index}/{len(links)}] starting {label}…", flush=True)
            try:
                return await asyncio.wait_for(
                    _scrape_listing(
                        link=link,
                        model_name=model_name,
                        context_id=run_context_id,
                        persistent_session=keep_windows_open
                        and not run_browserbase_context_mode,
                        use_browserbase_context=run_browserbase_context_mode,
                    ),
                    timeout=scrape_timeout_s,
                )
            except asyncio.TimeoutError as exc:
                _debug_print(
                    label,
                    f"scrape timed out after {scrape_timeout_s:.0f}s; using fallback payload.",
                )
                return {
                    "stagehand_context_id": encoded_run_context_ref,
                    "browserbase_session_id": encoded_run_context_ref,
                    "listing_url": link,
                    "data": _minimal_listing_data(link),
                }
            except Exception as exc:
                _debug_print(label, f"scrape failed; using fallback payload: {exc}")
                return {
                    "stagehand_context_id": encoded_run_context_ref,
                    "browserbase_session_id": encoded_run_context_ref,
                    "listing_url": link,
                    "data": _minimal_listing_data(link),
                }

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
    Start a Stagehand session with a persisted context, open the listing
    message box, send a message, wait for a reply, and return only the sent
    message and any seller reply.
    """
    load_dotenv()
    if not listing_url:
        raise ValueError(
            "listing_url is required when sending messages via Stagehand sessions"
        )
    browserbase_mode_from_ref, decoded_context_id = _decode_context_ref(
        stagehand_context_id
    )
    configured_shared_context_id = (
        os.environ.get("STAGEHAND_CONTEXT_ID")
        or os.environ.get("BROWSERBASE_CONTEXT_ID")
        or ""
    ).strip()
    use_browserbase_context = browserbase_mode_from_ref or (
        _use_browserbase_context_mode()
        and bool(configured_shared_context_id)
        and decoded_context_id == configured_shared_context_id
    )
    resolved_context_id = (
        _resolve_browserbase_context_id(decoded_context_id)
        if use_browserbase_context
        else _resolve_stagehand_context_id(decoded_context_id, listing_url=listing_url)
    )

    session_key = _listing_session_key(
        resolved_context_id,
        use_browserbase_context=use_browserbase_context,
        listing_url=listing_url,
    )
    log_label = session_key[:24]

    # Serialize sends to the same chat window (both modes) so concurrent rounds
    # don't fight over one composer.
    lock = _context_send_lock(session_key)
    if lock.locked():
        _debug_print(log_label, "another message send is in progress; waiting…")

    async with lock:
        bundle, created = await _acquire_listing_session(
            session_key=session_key,
            resolved_context_id=resolved_context_id,
            model_name=model_name,
            use_browserbase_context=use_browserbase_context,
        )
        session = bundle["session"]
        _debug_print(
            log_label,
            "started new listing chat session, opening chat…"
            if created
            else "reusing open listing chat session for message send.",
        )
        _debug_print(log_label, f"OUTBOUND message: {_message_preview(message)}")

        try:
            # Only (re)load the listing when the chat window isn't already on it.
            # Skipping this on reuse keeps the composer open and makes the send
            # fast and reliable instead of remounting the page every message.
            if bundle.get("url") != listing_url:
                await session.navigate(
                    url=listing_url,
                    options={"wait_until": "domcontentloaded"},
                )
                bundle["url"] = listing_url

            if skip_if_buyer_message_exists and await _buyer_message_exists(
                session,
                model_name=model_name,
            ):
                _debug_print(
                    log_label,
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
                # Soft-fail: don't crash the whole negotiation job over one
                # unsent message — the worker can retry on its next round.
                _debug_print(
                    log_label,
                    "unable to confirm message was sent; skipping this round.",
                )
                return {
                    "sent_message": None,
                    "reply_message": None,
                    "send_failed": True,
                }

            _debug_print(log_label, f"sent message, waiting {reply_wait_ms}ms…")
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
                log_label,
                (
                    f"INBOUND reply: {_message_preview(reply)}"
                    if reply
                    else "INBOUND reply: <none>"
                ),
            )
            return result
        except Exception:
            # Drop the (possibly broken) session so the next round rebuilds a
            # clean chat window rather than reusing a wedged one.
            await _close_listing_bundle(session_key)
            raise
        finally:
            if created and not use_browserbase_context:
                _tile_chrome_windows()


async def poll_listing_reply(
    stagehand_context_id: str,
    *,
    sent_message: str,
    listing_url: str | None = None,
    model_name: str = "anthropic/claude-sonnet-4-6",
) -> str | None:
    """
    Poll for a seller reply that appeared after the given buyer message.
    Reuses active persistent local sessions when available.
    """
    load_dotenv()
    if not sent_message or not sent_message.strip():
        return None

    browserbase_mode_from_ref, decoded_context_id = _decode_context_ref(
        stagehand_context_id
    )
    configured_shared_context_id = (
        os.environ.get("STAGEHAND_CONTEXT_ID")
        or os.environ.get("BROWSERBASE_CONTEXT_ID")
        or ""
    ).strip()
    use_browserbase_context = browserbase_mode_from_ref or (
        _use_browserbase_context_mode()
        and bool(configured_shared_context_id)
        and decoded_context_id == configured_shared_context_id
    )
    resolved_context_id = (
        _resolve_browserbase_context_id(decoded_context_id)
        if use_browserbase_context
        else _resolve_stagehand_context_id(decoded_context_id, listing_url=listing_url)
    )

    session_key = _listing_session_key(
        resolved_context_id,
        use_browserbase_context=use_browserbase_context,
        listing_url=listing_url,
    )

    async with _context_send_lock(session_key):
        bundle, created = await _acquire_listing_session(
            session_key=session_key,
            resolved_context_id=resolved_context_id,
            model_name=model_name,
            use_browserbase_context=use_browserbase_context,
        )
        session = bundle["session"]

        try:
            # Only navigate when this is a freshly created window; a reused chat
            # is already sitting on the listing with the thread open.
            if listing_url and bundle.get("url") != listing_url:
                await session.navigate(
                    url=listing_url,
                    options={"wait_until": "domcontentloaded"},
                )
                bundle["url"] = listing_url
            response = await session.extract(
                instruction=_build_reply_check_instruction(sent_message),
                schema=REPLY_CHECK_SCHEMA,
                options={"model": model_name},
            )
            reply_data = _to_dict(response.data.result if response.data else None) or {}
            if not reply_data.get("seller_replied"):
                return None
            raw_reply = reply_data.get("reply_message")
            if isinstance(raw_reply, str):
                return raw_reply.strip() or None
            return None
        except Exception:
            await _close_listing_bundle(session_key)
            raise



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