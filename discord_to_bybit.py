#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os, re, sys, time, json, traceback, html, random
from datetime import datetime
from pathlib import Path
from typing import Optional, List

import requests
from dotenv import load_dotenv

# Bybit SDK (pybit v2 unified)
from pybit.unified_trading import HTTP

load_dotenv()

# =========================
# ENVs
# =========================
DISCORD_TOKEN = os.getenv("DISCORD_TOKEN", "").strip()
CHANNEL_ID    = os.getenv("CHANNEL_ID", "").strip()

BYBIT_API_KEY    = os.getenv("BYBIT_API_KEY", "").strip()
BYBIT_API_SECRET = os.getenv("BYBIT_API_SECRET", "").strip()
BYBIT_TESTNET    = os.getenv("BYBIT_TESTNET", "true").lower() == "true"

QUOTE            = os.getenv("QUOTE", "USDT").strip().upper()
LEVERAGE         = float(os.getenv("LEVERAGE", "5"))
RISK_USDT        = float(os.getenv("RISK_USDT", "10"))   # dein gewünschtes Positions-Risiko pro Trade

STOP_LOSS_PCT    = float(os.getenv("STOP_LOSS_PCT", "19.0"))

ENTRY_EXPIRATION_MIN        = int(os.getenv("ENTRY_EXPIRATION_MIN", "180"))
ENTRY_WAIT_MINUTES          = int(os.getenv("ENTRY_WAIT_MINUTES", "0"))
ENTRY_TRIGGER_BUFFER_PCT    = float(os.getenv("ENTRY_TRIGGER_BUFFER_PCT", "0.0"))
ENTRY_EXPIRATION_PRICE_PCT  = float(os.getenv("ENTRY_EXPIRATION_PRICE_PCT", "0.0"))

TEST_MODE           = os.getenv("TEST_MODE", "false").lower() == "true"

POLL_BASE_SECONDS   = int(os.getenv("POLL_BASE_SECONDS", "60"))
POLL_OFFSET_SECONDS = int(os.getenv("POLL_OFFSET_SECONDS", "3"))
POLL_JITTER_MAX     = int(os.getenv("POLL_JITTER_MAX", "7"))

DISCORD_FETCH_LIMIT = int(os.getenv("DISCORD_FETCH_LIMIT", "50"))
STATE_FILE          = Path(os.getenv("STATE_FILE", "state.json"))

COOLDOWN_SECONDS    = int(os.getenv("COOLDOWN_SECONDS", "0"))  # 0 = aus

# =========================
# Startup Checks
# =========================
if not DISCORD_TOKEN or not CHANNEL_ID:
    print("❌ ENV fehlt: DISCORD_TOKEN oder CHANNEL_ID")
    sys.exit(1)

if not BYBIT_API_KEY or not BYBIT_API_SECRET:
    print("❌ ENV fehlt: BYBIT_API_KEY oder BYBIT_API_SECRET")
    sys.exit(1)

HEADERS = {
    "Authorization": DISCORD_TOKEN,
    "User-Agent": "DiscordToBybit/1.0"
}

# Bybit Session
bybit = HTTP(
    testnet=BYBIT_TESTNET,
    api_key=BYBIT_API_KEY,
    api_secret=BYBIT_API_SECRET,
)

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
# Signal Parsing (inkl. TP/DCAs)
# =========================
PAIR_LINE_OLD   = re.compile(r"(^|\n)\s*([A-Z0-9]+)\s+(LONG|SHORT)\s+Signal\s*(\n|$)", re.I)
HDR_SLASH_PAIR  = re.compile(r"([A-Z0-9]+)\s*/\s*[A-Z0-9]+\b.*\b(LONG|SHORT)\b", re.I)
HDR_COIN_DIR    = re.compile(r"Coin\s*:\s*([A-Z0-9]+).*?Direction\s*:\s*(LONG|SHORT)", re.I | re.S)

ENTER_ON_TRIGGER = re.compile(r"Enter\s+on\s+Trigger\s*:\s*\$?\s*"+NUM, re.I)
ENTRY_COLON      = re.compile(r"\bEntry\s*:\s*\$?\s*"+NUM, re.I)
ENTRY_SECTION    = re.compile(r"\bENTRY\b\s*\n\s*\$?\s*"+NUM, re.I)

TP1_LINE  = re.compile(r"\bTP\s*1\s*:\s*\$?\s*"+NUM, re.I)
TP2_LINE  = re.compile(r"\bTP\s*2\s*:\s*\$?\s*"+NUM, re.I)
TP3_LINE  = re.compile(r"\bTP\s*3\s*:\s*\$?\s*"+NUM, re.I)
DCA1_LINE = re.compile(r"\bDCA\s*#?\s*1\s*:\s*\$?\s*"+NUM, re.I)
DCA2_LINE = re.compile(r"\bDCA\s*#?\s*2\s*:\s*\$?\s*"+NUM, re.I)
DCA3_LINE = re.compile(r"\bDCA\s*#?\s*3\s*:\s*\$?\s*"+NUM, re.I)

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

def find_tp_dca(txt: str):
    tps = []
    for rx in (TP1_LINE, TP2_LINE, TP3_LINE):
        m = rx.search(txt)
        tps.append(to_price(m.group(1)) if m else None)
    dcas = []
    for rx in (DCA1_LINE, DCA2_LINE, DCA3_LINE):
        m = rx.search(txt)
        dcas.append(to_price(m.group(1)) if m else None)
    return tps, dcas

def parse_signal_from_text(txt: str):
    base, side = find_base_side(txt)
    if not base or not side:
        return None
    entry = find_entry(txt)
    if entry is None:
        return None
    (tp1, tp2, tp3), (d1, d2, d3) = find_tp_dca(txt)
    return {
        "base": base,
        "side": side,   # "long" / "short"
        "entry": entry,
        "tp1": tp1,
        "tp2": tp2,
        "tp3": tp3,
        "dca1": d1,
        "dca2": d2,
        "dca3": d3,
        "raw": txt,
    }

# =========================
# Bybit Helper
# =========================
def calc_qty_from_risk(entry: float) -> float:
    """
    Sehr simple Version:
    - du willst RISK_USDT Notional / LEVERAGE Margin riskieren
    - qty (Coin) = (RISK_USDT * LEVERAGE) / entry
    """
    if entry <= 0:
        return 0.0
    qty = (RISK_USDT * LEVERAGE) / entry
    return max(qty, 0.0)

def calc_stop_price(side: str, entry: float) -> float:
    """
    SL-Preis aus STOP_LOSS_PCT (Abstand vom Entry).
    """
    pct = STOP_LOSS_PCT / 100.0
    if side == "long":
        return entry * (1.0 - pct)
    else:
        return entry * (1.0 + pct)

def build_and_place_bybit_order(sig: dict):
    base  = sig["base"]
    side  = sig["side"]  # "long" / "short"
    entry = sig["entry"]
    tp1   = sig["tp1"]

    if tp1 is None:
        print("⚠️ Kein TP1 im Signal gefunden – überspringe Trade.")
        return

    symbol = f"{base}{QUOTE}"   # z.B. BERAUSDT
    bybit_side = "Buy" if side == "long" else "Sell"

    # Trigger und Expiration-Price
    if side == "long":
        trigger_price = entry * (1.0 - ENTRY_TRIGGER_BUFFER_PCT/100.0)
        trigger_dir   = 2  # Preis fällt auf triggerPrice
        expire_price  = (
            entry * (1.0 + ENTRY_EXPIRATION_PRICE_PCT/100.0)
            if ENTRY_EXPIRATION_PRICE_PCT > 0 else None
        )
    else:
        trigger_price = entry * (1.0 + ENTRY_TRIGGER_BUFFER_PCT/100.0)
        trigger_dir   = 1  # Preis steigt auf triggerPrice
        expire_price  = (
            entry * (1.0 - ENTRY_EXPIRATION_PRICE_PCT/100.0)
            if ENTRY_EXPIRATION_PRICE_PCT > 0 else None
        )

    qty = calc_qty_from_risk(entry)
    if qty <= 0:
        print("⚠️ Qty <= 0 – check RISK_USDT / LEVERAGE / Entry.")
        return

    sl_price = calc_stop_price(side, entry)

    print(f"\n📊 {base} {side.upper()} | {symbol}")
    print(f"   Entry: {entry:.6f} | TP1: {tp1:.6f} | SL: {sl_price:.6f}")
    print(f"   Trigger @ {trigger_price:.6f} (dir={trigger_dir}) | Qty ≈ {qty}")

    if TEST_MODE:
        print("⚠️ TEST_MODE: überspringe echten Order-Call.")
        return

    try:
        resp = bybit.place_order(
            category="linear",
            symbol=symbol,
            side=bybit_side,
            orderType="Limit",
            qty=f"{qty:.6f}",
            price=f"{entry:.6f}",
            timeInForce="GTC",
            triggerPrice=f"{trigger_price:.6f}",
            triggerDirection=trigger_dir,
            triggerBy="LastPrice",
            # Bracket (ein TP + ein SL, Entire Position):
            takeProfit=f"{tp1:.6f}",
            stopLoss=f"{sl_price:.6f}",
            tpslMode="Full",
            tpOrderType="Market",
            slOrderType="Market",
            positionIdx=0,          # One-way Mode
        )
        print(f"   ✅ Bybit-Antwort: {resp}")
    except Exception as e:
        print(f"   ❌ Fehler beim Bybit-Order-Call: {e}")
        traceback.print_exc()

# =========================
# Main Loop
# =========================
def main():
    print("="*60)
    print("🚀 Discord → Bybit Bot v1.0 (native Conditional + TP/SL)")
    print("="*60)
    print(f"Bybit Testnet: {BYBIT_TESTNET}")
    print(f"Leverage: {LEVERAGE}x | Risk/Trade: {RISK_USDT} {QUOTE}")
    print(f"SL Abstand: {STOP_LOSS_PCT}%")
    print(
        f"Entry Trigger-Buffer: {ENTRY_TRIGGER_BUFFER_PCT}% | Expire {ENTRY_EXPIRATION_MIN} min"
        + (f" + Expire-Price ±{ENTRY_EXPIRATION_PRICE_PCT}% (Gewinnrichtung)" if ENTRY_EXPIRATION_PRICE_PCT>0 else "")
    )
    if TEST_MODE:
        print("⚠️ TEST MODE AKTIV – keine echten Orders!")
    print("-"*60)

    state = load_state()
    last_id = state.get("last_id")
    last_trade_ts = float(state.get("last_trade_ts", 0.0))

    # Erststart: baseline auf aktuellste Message setzen
    if last_id is None:
        try:
            page = fetch_messages_after(CHANNEL_ID, None, limit=1)
            if page:
                last_id = str(page[0]["id"])
                state["last_id"] = last_id
                save_state(state)
        except:
            pass

    print("👀 Überwache Discord-Channel...\n")

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

                    # Cooldown
                    if COOLDOWN_SECONDS > 0 and (time.time() - last_trade_ts) < COOLDOWN_SECONDS:
                        max_seen = max(max_seen, mid)
                        continue

                    if raw:
                        sig = parse_signal_from_text(raw)
                        if sig and sig.get("entry"):
                            build_and_place_bybit_order(sig)
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
            print(f"❌ Fehler im Main-Loop: {e}")
            traceback.print_exc()
            time.sleep(10)
        finally:
            sleep_until_next_tick()

if __name__ == "__main__":
    main()
