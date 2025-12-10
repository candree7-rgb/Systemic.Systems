#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, sys, time, json, traceback, html, random
from datetime import datetime
from pathlib import Path
from typing import Optional, List, Tuple
import requests
from dotenv import load_dotenv

load_dotenv()

# =========================
# ENVs
# =========================
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
CHANNEL_ID    = os.getenv("CHANNEL_ID", "").strip()

# Webhook #1
ALTRADY_WEBHOOK_URL   = os.getenv("ALTRADY_WEBHOOK_URL", "").strip()
ALTRADY_API_KEY       = os.getenv("ALTRADY_API_KEY", "").strip()
ALTRADY_API_SECRET    = os.getenv("ALTRADY_API_SECRET", "").strip()
ALTRADY_EXCHANGE      = os.getenv("ALTRADY_EXCHANGE", "BYBI").strip()

# Optionaler Webhook #2 (eigene Creds/Exchange)
ALTRADY_WEBHOOK_URL_2 = os.getenv("ALTRADY_WEBHOOK_URL_2", "").strip()
ALTRADY_API_KEY_2     = os.getenv("ALTRADY_API_KEY_2", "").strip()
ALTRADY_API_SECRET_2  = os.getenv("ALTRADY_API_SECRET_2", "").strip()
ALTRADY_EXCHANGE_2    = os.getenv("ALTRADY_EXCHANGE_2", "").strip()

QUOTE = os.getenv("QUOTE", "USDT").strip().upper()

# Getrennte Hebel je Webhook
LEVERAGE_1 = int(os.getenv("LEVERAGE_1", "5"))
LEVERAGE_2 = int(os.getenv("LEVERAGE_2", "10"))

# Limit-Order Ablauf (Zeit – generelle Max-Dauer)
ENTRY_EXPIRATION_MIN        = int(os.getenv("ENTRY_EXPIRATION_MIN", "180"))

# Entry-Condition
ENTRY_WAIT_MINUTES          = int(os.getenv("ENTRY_WAIT_MINUTES", "0"))           # 0 = keine Zeit-Bedingung
ENTRY_TRIGGER_BUFFER_PCT    = float(os.getenv("ENTRY_TRIGGER_BUFFER_PCT", "0.0")) # z.B. 0.0 oder 0.8
ENTRY_EXPIRATION_PRICE_PCT  = float(os.getenv("ENTRY_EXPIRATION_PRICE_PCT", "0.0"))

TEST_MODE           = os.getenv("TEST_MODE", "false").lower() == "true"    # Für Tests

# Poll-Steuerung
POLL_BASE_SECONDS   = int(os.getenv("POLL_BASE_SECONDS", "60"))
POLL_OFFSET_SECONDS = int(os.getenv("POLL_OFFSET_SECONDS", "3"))
POLL_JITTER_MAX     = int(os.getenv("POLL_JITTER_MAX", "7"))

DISCORD_FETCH_LIMIT = int(os.getenv("DISCORD_FETCH_LIMIT", "50"))
STATE_FILE          = Path(os.getenv("STATE_FILE", "state.json"))

# Cooldown nach Order-Open
COOLDOWN_SECONDS    = int(os.getenv("COOLDOWN_SECONDS", "0"))  # 0 = aus

# =========================
# Startup Checks
# =========================
if not DISCORD_TOKEN or not CHANNEL_ID or not ALTRADY_WEBHOOK_URL:
    print("❌ ENV fehlt: DISCORD_TOKEN, CHANNEL_ID, ALTRADY_WEBHOOK_URL")
    sys.exit(1)

if not ALTRADY_API_KEY or not ALTRADY_API_SECRET:
    print("❌ API Keys fehlen: ALTRADY_API_KEY, ALTRADY_API_SECRET")
    sys.exit(1)

HEADERS = {
    "Authorization": DISCORD_TOKEN,
    "User-Agent": "DiscordToAltrady/3.0-entry-only"
}

# =========================
# Utils
# =========================
def load_state():
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text(encoding="utf-8"))
        except:
            pass
    return {"last_id": None, "last_trade_ts": 0.0}

def save_state(st: dict):
    tmp = STATE_FILE.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(st), encoding="utf-8")
    tmp.replace(STATE_FILE)

def sleep_until_next_tick():
    now = time.time()
    period_start = (now // POLL_BASE_SECONDS) * POLL_BASE_SECONDS
    next_tick = period_start + POLL_BASE_SECONDS + POLL_OFFSET_SECONDS
    if now < period_start + POLL_OFFSET_SECONDS:
        next_tick = period_start + POLL_OFFSET_SECONDS
    jitter = random.uniform(0, max(0, POLL_JITTER_MAX))
    time.sleep(max(0, next_tick - now + jitter))

def fetch_messages_after(channel_id: str, after_id: Optional[str], limit: int = 50):
    collected = []
    params = {"limit": max(1, min(limit, 100))}
    if after_id:
        params["after"] = str(after_id)

    while True:
        r = requests.get(
            f"https://discord.com/api/v10/channels/{channel_id}/messages",
            headers=HEADERS, params=params, timeout=15
        )
        if r.status_code == 429:
            retry = 5
            try:
                if r.headers.get("Content-Type","").startswith("application/json"):
                    retry = float(r.json().get("retry_after", 5))
            except:
                pass
            print(f"⚠️ Rate Limit, warte {retry} Sekunden...")
            time.sleep(retry + 0.5)
            continue
        r.raise_for_status()
        page = r.json() or []
        collected.extend(page)
        if len(page) < params["limit"]:
            break
        max_id = max(int(m.get("id","0")) for m in page if "id" in m)
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
    if not s: return ""
    s = s.replace("\r", "")
    s = html.unescape(s)
    s = MD_LINK.sub(r"\1", s)
    s = MD_MARK.sub("", s)
    s = MULTI_WS.sub(" ", s)
    s = "\n".join(line.strip() for line in s.split("\n"))
    return s.strip()

def to_price(s: str) -> float:
    return float(s.replace(",", ""))

def message_text(m: dict) -> str:
    parts = []
    parts.append(m.get("content") or "")
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
# Signal Parsing (nur Pair, Side, Entry)
# =========================
PAIR_LINE_OLD   = re.compile(r"(^|\n)\s*([A-Z0-9]+)\s+(LONG|SHORT)\s+Signal\s*(\n|$)", re.I)
HDR_SLASH_PAIR  = re.compile(r"([A-Z0-9]+)\s*/\s*[A-Z0-9]+\b.*\b(LONG|SHORT)\b", re.I)
HDR_COIN_DIR    = re.compile(r"Coin\s*:\s*([A-Z0-9]+).*?Direction\s*:\s*(LONG|SHORT)", re.I | re.S)

ENTER_ON_TRIGGER = re.compile(r"Enter\s+on\s+Trigger\s*:\s*\$?\s*"+NUM, re.I)
ENTRY_COLON      = re.compile(r"\bEntry\s*:\s*\$?\s*"+NUM, re.I)
ENTRY_SECTION    = re.compile(r"\bENTRY\b\s*\n\s*\$?\s*"+NUM, re.I)

def find_base_side(txt: str):
    mh = HDR_SLASH_PAIR.search(txt)
    if mh:
        return mh.group(1).upper(), ("long" if mh.group(2).upper()=="LONG" else "short")
    mo = PAIR_LINE_OLD.search(txt)
    if mo:
        return mo.group(2).upper(), ("long" if mo.group(3).upper()=="LONG" else "short")
    mc = HDR_COIN_DIR.search(txt)
    if mc:
        return mc.group(1).upper(), ("long" if mc.group(2).upper()=="LONG" else "short")
    return None, None

def find_entry(txt: str) -> Optional[float]:
    for rx in (ENTER_ON_TRIGGER, ENTRY_COLON, ENTRY_SECTION):
        m = rx.search(txt)
        if m:
            return to_price(m.group(1))
    return None

def parse_signal_from_text(txt: str):
    base, side = find_base_side(txt)
    if not base or not side:
        return None
    entry = find_entry(txt)
    if entry is None:
        return None
    return {
        "base": base,
        "side": side,
        "entry": entry
    }

# =========================
# Altrady Payload (nur Entry)
# =========================
def build_altrady_open_payload(sig: dict, exchange: str, api_key: str, api_secret: str, leverage: int) -> dict:
    base  = sig["base"]
    side  = sig["side"]
    entry = sig["entry"]

    symbol = f"{exchange}_{QUOTE}_{base}"

    # Entry-Trigger & Expiration-Price
    if side == "long":
        trigger_price = entry * (1.0 - ENTRY_TRIGGER_BUFFER_PCT/100.0)
        expire_price  = (
            entry * (1.0 + ENTRY_EXPIRATION_PRICE_PCT/100.0)
            if ENTRY_EXPIRATION_PRICE_PCT > 0 else None
        )
    else:
        trigger_price = entry * (1.0 + ENTRY_TRIGGER_BUFFER_PCT/100.0)
        expire_price  = (
            entry * (1.0 - ENTRY_EXPIRATION_PRICE_PCT/100.0)
            if ENTRY_EXPIRATION_PRICE_PCT > 0 else None
        )

    payload = {
        "api_key": api_key,
        "api_secret": api_secret,
        "exchange": exchange,
        "action": "open",
        "symbol": symbol,
        "side": side,
        "order_type": "limit",
        "signal_price": entry,
        "leverage": leverage,
        "entry_condition": {
            "price": float(f"{trigger_price:.10f}")
        },
        "entry_expiration": {
            "time": ENTRY_EXPIRATION_MIN
        }
        # KEINE take_profit, stop_loss, dca_orders – das macht der Bot
    }

    if expire_price is not None:
        payload["entry_expiration"]["price"] = float(f"{expire_price:.10f}")

    if ENTRY_WAIT_MINUTES > 0:
        payload["entry_condition"]["time"] = ENTRY_WAIT_MINUTES
        payload["entry_condition"]["operator"] = "OR"

    if TEST_MODE:
        payload["test"] = True

    # Kurz-Log
    print(f"\n📊 {base} {side.upper()}  |  {symbol}  |  Entry {entry}")
    print(
        f"   Trigger @ {trigger_price:.6f}  |  Expire in {ENTRY_EXPIRATION_MIN} min"
        + (f" oder Preis {expire_price:.6f}" if expire_price else "")
    )

    return payload

# =========================
# HTTP (pro Webhook)
# =========================
def _post_one(url: str, payload: dict):
    print(f"   📤 Sende an {url} ...")
    for attempt in range(3):
        try:
            r = requests.post(url, json=payload, timeout=20)

            if r.status_code == 429:
                delay = 2.0
                try:
                    if r.headers.get("Content-Type","").startswith("application/json"):
                        delay = float(r.json().get("retry_after", 2.0))
                except:
                    pass
                print(f"   ⚠️ Rate Limit, warte {delay} Sekunden...")
                time.sleep(delay + 0.25)
                continue

            if r.status_code == 204:
                print("   ✅ Erfolg! Pending order angelegt (wartet auf Trigger).")
                return r

            if not r.ok:
                # Debug: zeig uns die Server-Antwort (z.B. bei 422)
                print(f"   ❌ Server-Antwort {r.status_code}: {r.text}")
                r.raise_for_status()

            print("   ✅ Erfolg!")
            return r
        except Exception as e:
            if attempt == 2:
                print(f"   ❌ Fehler bei {url}: {e}")
                raise
            wait = 1.5 * (attempt + 1)
            print(f"   ⚠️ Retry in {wait:.1f}s wegen: {e}")
            time.sleep(wait)

def post_to_all_webhooks(payloads_and_urls: List[Tuple[str, dict]]):
    last_resp = None
    for i, (url, payload) in enumerate(payloads_and_urls, 1):
        print(f"→ Webhook #{i} von {len(payloads_and_urls)}")
        try:
            last_resp = _post_one(url, payload)
        except Exception as e:
            print(f"   ⚠️ Weiter mit nächstem Webhook (Fehler: {e})")
            continue
    return last_resp

# =========================
# Main
# =========================
def main():
    print("="*50)
    print("🚀 Discord → Altrady Bot v3.0 (Entry-only, TPs/DCA/SL im Bot)")
    print("="*50)
    print(f"Exchange #1: {ALTRADY_EXCHANGE} | Leverage: {LEVERAGE_1}x")
    if ALTRADY_WEBHOOK_URL_2 and ALTRADY_API_KEY_2 and ALTRADY_API_SECRET_2 and ALTRADY_EXCHANGE_2:
        print(f"Exchange #2: {ALTRADY_EXCHANGE_2} | Leverage: {LEVERAGE_2}x")
    print(
        f"Entry: Buffer {ENTRY_TRIGGER_BUFFER_PCT}% | Expire {ENTRY_EXPIRATION_MIN} min"
        + (f" + Expire-Price ±{ENTRY_EXPIRATION_PRICE_PCT}% (Gewinnrichtung)" if ENTRY_EXPIRATION_PRICE_PCT>0 else "")
    )
    if ENTRY_WAIT_MINUTES > 0:
        print(f"Entry-Condition Time: {ENTRY_WAIT_MINUTES} min (OR)")
    if COOLDOWN_SECONDS > 0:
        print(f"Cooldown: {COOLDOWN_SECONDS}s")
    if TEST_MODE:
        print("⚠️ TEST MODE aktiv")

    active_webhooks = 1 + int(bool(ALTRADY_WEBHOOK_URL_2 and ALTRADY_API_KEY_2 and ALTRADY_API_SECRET_2 and ALTRADY_EXCHANGE_2))
    print(f"Webhooks aktiv: {active_webhooks}")
    print("-"*50)

    state = load_state()
    last_id = state.get("last_id")
    last_trade_ts = float(state.get("last_trade_ts", 0.0))

    # Erststart: baseline auf aktuellste Message setzen (nicht rückwirkend)
    if last_id is None:
        try:
            page = fetch_messages_after(CHANNEL_ID, None, limit=1)
            if page:
                last_id = str(page[0]["id"])
                state["last_id"] = last_id
                save_state(state)
        except:
            pass

    print("👀 Überwache Channel...\n")

    while True:
        try:
            msgs = fetch_messages_after(CHANNEL_ID, last_id, limit=DISCORD_FETCH_LIMIT)
            msgs_sorted = sorted(msgs, key=lambda m: int(m.get("id","0")))
            max_seen = int(last_id or 0)

            if not msgs_sorted:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Warte auf Signale...")
            else:
                for m in msgs_sorted:
                    mid = int(m.get("id","0"))
                    raw = message_text(m)

                    # Cooldown: blocke neue Orders kurz nach dem letzten Open
                    if COOLDOWN_SECONDS > 0 and (time.time() - last_trade_ts) < COOLDOWN_SECONDS:
                        max_seen = max(max_seen, mid)
                        continue

                    if raw:
                        sig = parse_signal_from_text(raw)
                        if sig:
                            # Payload #1
                            p1 = build_altrady_open_payload(
                                sig, ALTRADY_EXCHANGE, ALTRADY_API_KEY, ALTRADY_API_SECRET, LEVERAGE_1
                            )
                            jobs = [(ALTRADY_WEBHOOK_URL, p1)]

                            # Payload #2 (optional)
                            if ALTRADY_WEBHOOK_URL_2 and ALTRADY_API_KEY_2 and ALTRADY_API_SECRET_2 and ALTRADY_EXCHANGE_2:
                                p2 = build_altrady_open_payload(
                                    sig, ALTRADY_EXCHANGE_2, ALTRADY_API_KEY_2, ALTRADY_API_SECRET_2, LEVERAGE_2
                                )
                                jobs.append((ALTRADY_WEBHOOK_URL_2, p2))

                            post_to_all_webhooks(jobs)
                            last_trade_ts = time.time()
                            state["last_trade_ts"] = last_trade_ts

                    max_seen = max(max_seen, mid)

                last_id = str(max_seen)
                state["last_id"] = last_id
                save_state(state)

        except KeyboardInterrupt:
            print("\n👋 Beendet")
            break
        except Exception as e:
            print(f"❌ Fehler: {e}")
            traceback.print_exc()
            time.sleep(10)
        finally:
            sleep_until_next_tick()

if __name__ == "__main__":
    main()
