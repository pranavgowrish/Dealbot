"""
One-time Facebook login for the local Stagehand context.

Opens a single real Chromium window on the *primary* local context profile —
the exact user-data-dir that listing windows are seeded/cloned from during a
run. Log in by hand; the script detects the Facebook login cookie, persists the
profile, and copies it into any already-created listing-* windows so they start
logged in too.

Run:  ../.venv/bin/python login_facebook.py
"""

from __future__ import annotations

import shutil
import time
from pathlib import Path

from dotenv import load_dotenv
from playwright.sync_api import sync_playwright

from browserb import (
    _CONTEXTS_ROOT,
    _cleanup_stale_context_locks,
    _context_user_data_dir,
    _resolve_stagehand_context_id,
)

FACEBOOK_URL = "https://www.facebook.com/"
LOGIN_COOKIE = "c_user"  # set only once a Facebook account session exists
LOGIN_TIMEOUT_S = 15 * 60
_SKIP = {"SingletonLock", "SingletonSocket", "SingletonCookie", "chrome.pid"}


def _log(message: str) -> None:
    print(f"[fb-login] {message}", flush=True)


def _is_logged_in(context) -> bool:
    for cookie in context.cookies("https://www.facebook.com"):
        if cookie.get("name") == LOGIN_COOKIE and cookie.get("value"):
            return True
    return False


def _force_clone(base_dir: Path, target_dir: Path) -> None:
    target_dir.mkdir(parents=True, exist_ok=True)
    for entry in base_dir.iterdir():
        if entry.name in _SKIP:
            continue
        destination = target_dir / entry.name
        try:
            if entry.is_dir():
                shutil.copytree(entry, destination, dirs_exist_ok=True)
            else:
                shutil.copy2(entry, destination)
        except OSError:
            continue


def _clone_into_existing_listing_windows(base_id: str) -> int:
    base_dir = _context_user_data_dir(base_id)
    cloned = 0
    if not _CONTEXTS_ROOT.exists():
        return 0
    for child in _CONTEXTS_ROOT.iterdir():
        if not child.is_dir() or not child.name.startswith("listing-"):
            continue
        _cleanup_stale_context_locks(child.name)
        _force_clone(base_dir, child)
        cloned += 1
    return cloned


def main() -> None:
    load_dotenv()
    base_id = _resolve_stagehand_context_id()
    user_data_dir = _context_user_data_dir(base_id)
    _cleanup_stale_context_locks(base_id)

    _log(f"primary context: {base_id}")
    _log(f"profile dir:     {user_data_dir}")
    _log("opening a Chromium window to Facebook — log in there, then leave it.")

    with sync_playwright() as p:
        context = p.chromium.launch_persistent_context(
            user_data_dir=str(user_data_dir),
            headless=False,
            args=["--no-first-run", "--no-default-browser-check"],
        )
        page = context.pages[0] if context.pages else context.new_page()
        try:
            page.goto(FACEBOOK_URL, wait_until="domcontentloaded")
        except Exception as exc:
            _log(f"initial navigation hiccup (continuing): {exc}")

        if _is_logged_in(context):
            _log("already logged in — refreshing the saved profile.")
        else:
            _log("waiting for you to finish logging in…")
            deadline = time.time() + LOGIN_TIMEOUT_S
            while time.time() < deadline:
                if _is_logged_in(context):
                    break
                time.sleep(2)
            else:
                _log("timed out waiting for login. Re-run when ready. Nothing saved.")
                context.close()
                return

        _log("login detected. Letting the session settle, then saving…")
        time.sleep(4)
        context.close()  # flushes cookies/storage to the profile dir

    cloned = _clone_into_existing_listing_windows(base_id)
    _log(f"saved login to primary profile; refreshed {cloned} existing listing window(s).")
    _log("done — execution windows seeded from this profile will start logged in.")


if __name__ == "__main__":
    main()
