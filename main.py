#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import re
import sys
import time
import json
import traceback
import html
import random
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional, List, Dict, Any

import requests
from dotenv import load_dotenv

load_dotenv()

# =========================
# ENVs
# =========================
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
CHANNEL_ID    = os.getenv("CHANNEL_ID", "").strip()

QUOTE = os.getenv("QUOTE", "USDT").strip().upper()

# 3Commas Webhook (Signal Bot - Custom Signal)
TC_WEBHOOK_URL = os.getenv("TC_WEBHOOK_URL", "").strip()
TC_SECRET      = os.getenv("TC_SECRET", "").strip()
TC_BOT_UUID    = os.getenv("TC_BOT_UUID", "").strip()
TC_MAX_LAG     = int(os.getenv("TC_MAX_LAG", "300"))

# TradingView Mapping
TV_EXCHANGE     = os.getenv("TV_EXCHANGE", "BINANCE").strip()
TV_INSTR_SUFFIX = os.getenv("TV_INSTR_SUFFIX", ".P").strip()

# Poll-Steuerung (Discord)
POLL_BASE_SECONDS   = int(os.getenv("POLL_BASE_SECONDS", "15"))
POLL_OFFSET_SECONDS = int(os.getenv("POLL_OFFSET_SECONDS", "0"))
POLL_JITTER_MAX     = int(os.getenv("POLL_JITTER_MAX", "3"))
DISCORD_FETCH_LIMIT = int(os.getenv("DISCORD_FETCH_LIMIT", "50"))

STATE_FILE          = Path(os.getenv("STATE_FILE", "state.json"))
COOLDOWN_SECONDS    = int(os.getenv("COOLDOWN_SECONDS", "0"))  # 0 = aus

TEST_MODE           = os.getenv("TEST_MODE", "false").lower() == "true"

# =========================
# Startup Checks
# =========================
if not DISCORD_TOKEN or not CHANNEL_ID or not TC_WEBHOOK_URL:
    print("❌ ENV fehlt: DISCORD_TOKEN, CHANNEL_ID oder TC_WEBHOOK_URL")
    sys.exit(1)

if not TC_SECRET or not TC_BOT_UUID:
    print("❌ ENV fehlt: TC_SECRET oder TC_BOT_UUID")
    sys.exit(1)

HEADERS = {
    "Authorization": DISCORD_TOKEN,   # z.B. "Bot xxxxx"
    "User-Agent": "DiscordTo3Commas/1.0"
}

# =========================
# State-Handling
# =========================
def load_state() -> Dict[str, Any]:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except Exception:
            pass
    # default
    return {"last_id": None, "last_trade_ts": 0.0}

def save_state(st: Dict[str, Any]) -> None:
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(st), encoding="utf-8")
    tmp.replace(STATE_FILE)

# =========================
# Timing / Polling
# =========================
def sleep_until_next_tick() -> None:
    """
    Poll alle POLL_BASE_SECONDS mit leichtem Jitter, um
    Discord-Rate-Limits harmonisch zu halten.
    """
    now = time.time()
    period_start = (now // POLL_BASE_SECONDS) * POLL_BASE_SECONDS
    next_tick = period_start + POLL_BASE_SECONDS + POLL_OFFSET_SECONDS
    if now < period_start + POLL_OFFSET_SECONDS:
        next_tick = period_start + POLL_OFFSET_SECONDS
    jitter = random.uniform(0, max(0, POLL_JITTER_MAX))
    sleep_for = max(0, next_tick - now + jitter)
    time.sleep(sleep_for)

# =========================
# Discord Fetch
# =========================
def fetch_messages_after(channel_id: str, after_id: Optional[str], limit: int = 50) -> List[Dict[str, Any]]:
    collected: List[Dict[str, Any]] = []
    params: Dict[str, Any] = {"limit": max(1, min(limit, 100))}
    if after_id:
        params["after"] = str(after_id)

    while True:
        r = requests.get(
            f"https://discord.com/api/v10/channels/{channel_id}/messages",
            headers=HEADERS,
            params=params,
            timeout=15
        )
        if r.status_code == 429:
            retry = 5
            try:
                if r.headers.get("Content-Type", "").startswith("application/json"):
                    retry = float(r.json().get("retry_after", 5))
            except Exception:
                pass
            print(f"⚠️ Discord Rate Limit, warte {retry} Sekunden...")
            time.sleep(retry + 0.5)
            continue

        r.raise_for_status()
        page = r.json() or []
        collected.extend(page)

        if len(page) < params["limit"]:
            break

        # Für Pagination
        max_id = max(int(m.get("id", "0")) for m in page if "id" in m)
        params["after"] = str(max_id)

    return collected

# =========================
# Text Processing
# =========================
MD_LINK   = re.compile(r"\[([^\]]+)\]\((?:[^)]+)\)")
MD_MARK   = re.compile(r"[*_`~]+")
MULTI_WS  = re.compile(r"[ \t\u00A0]+")
NUM       = r"([0-9][0-9,]*\.?[0-9]*)"

def clean_markdown(s: str) -> str:
    if not s:
        return ""
    s = s.replace("\r", "")
    s = html.unescape(s)
    s = MD_LINK.sub(r"\1", s)
    s = MD_MARK.sub("", s)
    s = MULTI_WS.sub(" ", s)
    s = "\n".join(line.strip() for line in s.split("\n"))
    return s.strip()

def to_price(s: str) -> float:
    return float(s.replace(",", ""))

def message_text(m: Dict[str, Any]) -> str:
    parts: List[str] = []

    # Normaler Content
    parts.append(m.get("content") or "")

    # Embeds
    embeds = m.get("embeds") or []
    for e in embeds:
        if not isinstance(e, dict):
            continue
        if e.get("title"):
            parts.append(str(e.get("title")))
        if e.get("description"):
            parts.append(str(e.get("description")))
        fields = e.get("fields") or []
        for f in fields:
            if not isinstance(f, dict):
                continue
            n = f.get("name") or ""
            v = f.get("value") or ""
            if n:
                parts.append(str(n))
            if v:
                parts.append(str(v))
        footer = (e.get("footer") or {}).get("text")
        if footer:
            parts.append(str(footer))

    return clean_markdown("\n".join([p for p in parts if p]))

# =========================
# Signal Parsing (Pair, Side, Entry)
# =========================
PAIR_LINE_OLD   = re.compile(r"(^|\n)\s*([A-Z0-9]+)\s+(LONG|SHORT)\s+Signal\s*(\n|$)", re.I)
HDR_SLASH_PAIR  = re.compile(r"([A-Z0-9]+)\s*/\s*[A-Z0-9]+\b.*\b(LONG|SHORT)\b", re.I)
HDR_COIN_DIR    = re.compile(r"Coin\s*:\s*([A-Z0-9]+).*?Direction\s*:\s*(LONG|SHORT)", re.I | re.S)

ENTER_ON_TRIGGER = re.compile(r"Enter\s+on\s+Trigger\s*:\s*\$?\s*" + NUM, re.I)
ENTRY_COLON      = re.compile(r"\bEntry\s*:\s*\$?\s*" + NUM, re.I)
ENTRY_SECTION    = re.compile(r"\bENTRY\b\s*\n\s*\$?\s*" + NUM, re.I)

def find_base_side(txt: str):
    """
    Versucht, Coin und Richtung (long/short) aus dem Signaltext zu lesen.
    """
    mh = HDR_SLASH_PAIR.search(txt)
    if mh:
        return mh.group(1).upper(), ("long" if mh.group(2).upper() == "LONG" else "short")

    mo = PAIR_LINE_OLD.search(txt)
    if mo:
        return mo.group(2).upper(), ("long" if mo.group(3).upper() == "LONG" else "short")

    mc = HDR_COIN_DIR.search(txt)
    if mc:
        return mc.group(1).upper(), ("long" if mc.group(2).upper() == "LONG" else "short")

    return None, None

def find_entry(txt: str) -> Optional[float]:
    """
    Sucht Entry-Preis (Enter on Trigger / Entry: / ENTRY-Block).
    """
    for rx in (ENTER_ON_TRIGGER, ENTRY_COLON, ENTRY_SECTION):
        m = rx.search(txt)
        if m:
            return to_price(m.group(1))
    return None

def parse_signal_from_text(txt: str) -> Optional[Dict[str, Any]]:
    base, side = find_base_side(txt)
    if not base or not side:
        return None
    entry = find_entry(txt)
    if entry is None:
        return None
    return {
        "base": base,
        "side": side,   # "long" oder "short"
        "entry": entry
    }

# =========================
# 3Commas Payload (Custom Signal)
# =========================
def build_3commas_payload(sig: Dict[str, Any]) -> Dict[str, Any]:
    base  = sig["base"]
    side  = sig["side"]
    entry = sig["entry"]

    # 3Commas Actions: enter_long / enter_short / exit_long / exit_short
    action = "enter_long" if side == "long" else "enter_short"

    # Instrument-String wie TradingView: z.B. "BTCUSDT.P"
    tv_instrument = f"{base}{QUOTE}{TV_INSTR_SUFFIX}"

    # ISO8601 UTC Timestamp
    timestamp = datetime.now(timezone.utc).isoformat(timespec="seconds")

    payload: Dict[str, Any] = {
        "secret": TC_SECRET,
        "max_lag": str(TC_MAX_LAG),
        "timestamp": timestamp,
        "trigger_price": f"{entry:.8f}",
        "tv_exchange": TV_EXCHANGE,
        "tv_instrument": tv_instrument,
        "action": action,
        "bot_uuid": TC_BOT_UUID
        # Order-Sizing etc. macht der Bot per UI, kein "order" notwendig
    }

    print(f"\n📊 {base} {side.upper()} | {tv_instrument} | Entry {entry}")
    print(f"   → action={action}, trigger_price={entry:.8f}")
    return payload

def send_to_3commas(payload: Dict[str, Any]) -> None:
    if TEST_MODE:
        print("🧪 TEST_MODE aktiv – Payload NICHT gesendet:")
        print(json.dumps(payload, indent=2))
        return

    print("   📤 Sende an 3Commas Webhook ...")
    for attempt in range(3):
        try:
            r = requests.post(TC_WEBHOOK_URL, json=payload, timeout=20)

            if r.status_code == 429:
                delay = 2.0
                try:
                    if r.headers.get("Content-Type", "").startswith("application/json"):
                        delay = float(r.json().get("retry_after", 2.0))
                except Exception:
                    pass
                print(f"   ⚠️ 3Commas Rate Limit, warte {delay} Sekunden...")
                time.sleep(delay + 0.25)
                continue

            if not r.ok:
                print(f"   ❌ 3Commas Antwort {r.status_code}: {r.text}")
                r.raise_for_status()

            print("   ✅ Erfolg!")
            return
        except Exception as e:
            if attempt == 2:
                print(f"   ❌ Fehler beim Senden an 3Commas: {e}")
                raise
            wait = 1.5 * (attempt + 1)
            print(f"   ⚠️ Retry in {wait:.1f}s wegen: {e}")
            time.sleep(wait)

# =========================
# Main Loop
# =========================
def main() -> None:
    print("=" * 60)
    print("🚀 Discord → 3Commas Signal Bot (Custom Signal, Entry-only)")
    print("=" * 60)
    print(f"Exchange: {TV_EXCHANGE} | Quote: {QUOTE}")
    print(f"POLL_BASE_SECONDS={POLL_BASE_SECONDS}, JITTER_MAX={POLL_JITTER_MAX}")
    if TEST_MODE:
        print("⚠️ TEST_MODE aktiv – keine realen Trades.")
    print("-" * 60)

    state = load_state()
    last_id = state.get("last_id")
    last_trade_ts = float(state.get("last_trade_ts", 0.0))

    # Erststart: baseline = aktuellste Message -> keine Retro-Trades
    if last_id is None:
        try:
            page = fetch_messages_after(CHANNEL_ID, None, limit=1)
            if page:
                last_id = str(page[0]["id"])
                state["last_id"] = last_id
                save_state(state)
                print(f"🏁 Initialisiere last_id mit {last_id}")
        except Exception as e:
            print(f"⚠️ Konnte initiale last_id nicht setzen: {e}")

    print("👀 Überwache Discord-Channel...\n")

    while True:
        try:
            msgs = fetch_messages_after(CHANNEL_ID, last_id, limit=DISCORD_FETCH_LIMIT)
            msgs_sorted = sorted(msgs, key=lambda m: int(m.get("id", "0")))
            max_seen = int(last_id or 0)

            if not msgs_sorted:
                ts = datetime.now().strftime('%H:%M:%S')
                print(f"[{ts}] Keine neuen Signale...")
            else:
                for m in msgs_sorted:
                    mid = int(m.get("id", "0"))
                    raw = message_text(m)

                    # einfacher Cooldown (optional)
                    if COOLDOWN_SECONDS > 0 and (time.time() - last_trade_ts) < COOLDOWN_SECONDS:
                        max_seen = max(max_seen, mid)
                        continue

                    if raw:
                        sig = parse_signal_from_text(raw)
                        if sig:
                            payload = build_3commas_payload(sig)
                            send_to_3commas(payload)
                            last_trade_ts = time.time()
                            state["last_trade_ts"] = last_trade_ts

                    max_seen = max(max_seen, mid)

                last_id = str(max_seen)
                state["last_id"] = last_id
                save_state(state)

        except KeyboardInterrupt:
            print("\n👋 Manuell beendet.")
            break
        except Exception as e:
            print(f"❌ Unerwarteter Fehler: {e}")
            traceback.print_exc()
            time.sleep(10)
        finally:
            sleep_until_next_tick()

if __name__ == "__main__":
    main()
