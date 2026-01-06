#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ProductionSkyMonitor (military-focused, data-preserving)

Key goals (per your request):
- Preserve *all relevant military-operation data* (do not silently ignore).
- Fix concurrency/collection bugs and hard-crash risks.
- Improve reliability: partial API failures, dedup/cooldowns, message chunking.
- Keep raw records for later investigation (JSONL snapshots + events).

Notes:
- We still do *some* filtering for alert logic (e.g., refueling requires lat/lon),
  BUT we do NOT discard the raw aircraft objects. Incomplete records are stored too.
"""

import os
import time
import math
import json
import html
import signal
import logging
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple
from datetime import datetime, timezone
from collections import deque
from concurrent.futures import ThreadPoolExecutor, as_completed

import numpy as np
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# ================== CONFIG ==================
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
    raise ValueError("Set TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID env vars.")

# Region: Iran + Persian Gulf + surroundings
REGION_BBOX = {"south": 20.0, "north": 42.0, "west": 38.0, "east": 68.0}

# Endpoints Config (2026)
CENTER_LAT, CENTER_LON = 32.0, 53.0
RADIUS_KM = 3000  # پوشش وسیع طبق درخواست کاربر (API ممکن است محدودیت ناتیکال مایل داشته باشد)

# Feature Toggles (env vars – 1 = enabled)
ZSCORE_ENABLED = os.getenv("ZSCORE_ENABLED", "0") == "1"
CORRIDOR_ENABLED = os.getenv("CORRIDOR_ENABLED", "0") == "1"

# Z-Score Config
ZSCORE_THRESHOLD = float(os.getenv("ZSCORE_THRESHOLD", "2.5"))  # |Z| > this → alert
ZSCORE_MIN_SAMPLES = int(os.getenv("ZSCORE_MIN_SAMPLES", "20"))

# Corridors (strategic paths – edit as needed)
CORRIDORS = [
    {"name": "Persian Gulf Tanker Route", "south": 24.0, "north": 27.0, "west": 50.0, "east": 56.0},
    {"name": "Strait of Hormuz Approach", "south": 25.0, "north": 27.5, "west": 55.0, "east": 57.0},
    {"name": "Turkey-Iran Transit", "south": 36.0, "north": 40.0, "west": 40.0, "east": 48.0},
]

# Monitoring
CHECK_INTERVAL_SEC = 300            # 5 minutes
FAILURE_THRESHOLD = 3               # consecutive failures before connectivity alert
MOVING_AVG_WINDOW = 20
COMMERCIAL_DROP_THRESHOLD = 0.40    # alert if current < avg*(1-0.40)
COMMERCIAL_ALERT_COOLDOWN_SEC = 3600

# Military identification (callsign prefixes)
MILITARY_CALLSIGNS = [
    "RCH", "CNV", "NATO", "LAGR", "FORTE", "HOMER", "DUKE", "RRR", "IAM", "PLF", "K35R",
    "VV", "GAF", "USA", "IRIAF", "FORCE", "RSF", "SYR", "IAF", "UAF"
]

# Refueling heuristics (alert logic; raw data is still stored regardless)
REFUEL_MAX_DIST_KM = 5.0
REFUEL_MAX_ALT_DIFF_FT = 1500
REFUEL_MAX_SPEED_DIFF_KT = 20
REFUEL_PAIR_COOLDOWN_SEC = 3600     # avoid spamming same tanker/receiver pair

# Telegram message size safety (Telegram hard limit exists; keep margin)
TELEGRAM_MAX_CHARS = 3500

# Persistence (raw snapshots + events)
DATA_DIR = os.getenv("SKYMONITOR_DATA_DIR", "sky_monitor_data")
SNAPSHOT_KEEP_DAYS = 2  # (simple retention; optional cleanup)

# API endpoints (adsb.lol)
ALL_URL = f"https://api.adsb.lol/v2/point/{CENTER_LAT}/{CENTER_LON}/{RADIUS_KM}"
MIL_URL = "https://api.adsb.lol/v2/mil"
# Note: TANKER_URL is no longer separate; we filter from MIL or ALL

# ================== Utilities ==================

def utc_iso() -> str:
    return datetime.now(timezone.utc).isoformat()

def ensure_dir(path: str) -> None:
    os.makedirs(path, exist_ok=True)

def safe_float(x: Any) -> Optional[float]:
    if isinstance(x, (int, float)):
        return float(x)
    return None

def safe_str(x: Any) -> str:
    if x is None:
        return ""
    return str(x)

def html_escape(s: str) -> str:
    return html.escape(s, quote=False)

def today_key_utc() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")

def bbox_contains(lat: float, lon: float) -> bool:
    return (REGION_BBOX["south"] <= lat <= REGION_BBOX["north"] and
            REGION_BBOX["west"] <= lon <= REGION_BBOX["east"])

def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2 +
         math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c

# ================== Persistence ==================

class JsonlStore:
    def __init__(self, base_dir: str):
        self.base_dir = base_dir
        ensure_dir(self.base_dir)

    def _path(self, name: str) -> str:
        ensure_dir(self.base_dir)
        return os.path.join(self.base_dir, f"{name}-{today_key_utc()}.jsonl")

    def append(self, name: str, record: Dict[str, Any]) -> None:
        path = self._path(name)
        try:
            with open(path, "a", encoding="utf-8") as f:
                f.write(json.dumps(record, ensure_ascii=False) + "\n")
        except Exception as e:
            logging.error(f"Failed writing {path}: {e}")

    def purge_old(self, keep_days: int) -> None:
        """Removes files older than keep_days based on the date in the filename."""
        try:
            now = datetime.now(timezone.utc)
            for filename in os.listdir(self.base_dir):
                if not filename.endswith(".jsonl"):
                    continue
                # Filename pattern: name-YYYY-MM-DD.jsonl
                parts = filename.rsplit("-", 3)
                if len(parts) < 2:
                    continue
                date_str = parts[-3] + "-" + parts[-2] + "-" + parts[-1].replace(".jsonl", "")
                try:
                    file_date = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                    if (now - file_date).days >= keep_days:
                        os.remove(os.path.join(self.base_dir, filename))
                        logging.info(f"Purged old data file: {filename}")
                except ValueError:
                    continue
        except Exception as e:
            logging.error(f"Error during data purge: {e}")

# ================== Telegram ==================

class TelegramClient:
    def __init__(self, session: requests.Session, token: str, chat_id: str):
        self.session = session
        self.token = token
        self.chat_id = chat_id

    def send_html(self, text: str) -> None:
        url = f"https://api.telegram.org/bot{self.token}/sendMessage"
        payload = {
            "chat_id": self.chat_id,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": True,
        }
        self.session.post(url, json=payload, timeout=15)

    def send_chunked_html(self, text: str) -> None:
        # Split by lines to preserve formatting
        if len(text) <= TELEGRAM_MAX_CHARS:
            self.send_html(text)
            return

        lines = text.split("\n")
        buf = []
        size = 0
        for line in lines:
            add = (line + "\n")
            if size + len(add) > TELEGRAM_MAX_CHARS and buf:
                self.send_html("".join(buf).rstrip())
                buf, size = [], 0
            buf.append(add)
            size += len(add)
        if buf:
            self.send_html("".join(buf).rstrip())

# ================== Monitor ==================

@dataclass
class FetchResult:
    ok: bool
    aircraft: List[Dict[str, Any]]
    error: Optional[str] = None
    latency_ms: Optional[int] = None

class ProductionSkyMonitor:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            "User-Agent": "ProductionSkyMonitor/1.0 (+https://example.invalid)"
        })

        # Retries (HTTP-level). Connection errors + some 5xx
        retries = Retry(
            total=3,
            backoff_factor=1,
            status_forcelist=(500, 502, 503, 504),
            allowed_methods=frozenset(["GET", "POST"])
        )
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        self.tg = TelegramClient(self.session, TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID)
        self.store = JsonlStore(DATA_DIR)

        # Z-Score histories
        if ZSCORE_ENABLED:
            self.zscore_histories = {
                "commercial": deque(maxlen=50),
                "tanker": deque(maxlen=50),
                "military": deque(maxlen=50),
            }
            if CORRIDOR_ENABLED:
                self.corridor_z_histories = {corr["name"]: deque(maxlen=50) for corr in CORRIDORS}

        # Corridor counts
        if CORRIDOR_ENABLED:
            self.corridor_counts = {corr["name"]: 0 for corr in CORRIDORS}

        self.commercial_history = deque(maxlen=MOVING_AVG_WINDOW)
        self.last_commercial_alert_ts = 0.0
        self.consecutive_failures = 0

        # State for enter/exit detection
        self.active_tankers: Dict[str, Dict[str, Any]] = {}
        self.active_military: Dict[str, Dict[str, Any]] = {}

        # Cooldowns
        self.last_refuel_alert: Dict[str, float] = {}  # key = "tankerHex|recvHex"

        logging.info("SkyMonitor initialized.")

    # ---------- Identification helpers ----------

    def is_military_callsign(self, callsign: Optional[str]) -> bool:
        if not callsign:
            return False
        c = callsign.strip().upper()
        return any(c.startswith(prefix) for prefix in MILITARY_CALLSIGNS)

    def is_tanker(self, ac: Dict[str, Any]) -> bool:
        """
        Identify tankers by keywords in desc/type or military callsigns.
        Requires 'mil' flag to be true.
        """
        if not ac.get("mil"):
            return False
        
        desc = safe_str(ac.get("desc")).lower()
        t = safe_str(ac.get("t")).lower()
        flight = safe_str(ac.get("flight")).strip().upper()
        
        match_keywords = any(k in desc or k in t for k in ["tanker", "kc-135", "kc-46", "a330mrtt", "kc-10"])
        return match_keywords or flight.startswith("K35R")

    def in_corridor(self, ac: Dict[str, Any], corridor: Dict[str, float]) -> bool:
        lat = safe_float(ac.get("lat"))
        lon = safe_float(ac.get("lon"))
        if lat is None or lon is None:
            return False
        return (corridor["south"] <= lat <= corridor["north"] and
                corridor["west"] <= lon <= corridor["east"])

    def in_region_or_unknown(self, ac: Dict[str, Any]) -> Tuple[bool, bool]:
        """
        Returns (in_region, has_location).
        If lat/lon missing -> (False, False). We still keep/store the record.
        """
        lat = safe_float(ac.get("lat"))
        lon = safe_float(ac.get("lon"))
        if lat is None or lon is None:
            return False, False
        return bbox_contains(lat, lon), True

    def is_airborne(self, ac: Dict[str, Any]) -> bool:
        """
        Used for some alert logic only.
        Raw records are stored regardless.
        """
        alt = ac.get("alt_baro")
        gs = safe_float(ac.get("gs"))
        if alt in (None, "ground"):
            return False
        if not isinstance(alt, (int, float)):
            return False
        if alt <= 1000:
            return False
        if gs is not None and gs < 50:
            return False
        return True

    def get_hex(self, ac: Dict[str, Any]) -> str:
        return safe_str(ac.get("hex")).strip().lower()

    def flight_id(self, ac: Dict[str, Any]) -> str:
        # For display; prefer callsign/flight
        flt = safe_str(ac.get("flight")).strip()
        return flt if flt else "Unknown"

    # ---------- Networking ----------

    def fetch_url(self, name: str, url: str) -> FetchResult:
        t0 = time.time()
        try:
            resp = self.session.get(url, timeout=25)
            resp.raise_for_status()
            js = resp.json()
            aircraft = js.get("ac", [])
            if not isinstance(aircraft, list):
                aircraft = []
            dt = int((time.time() - t0) * 1000)
            return FetchResult(ok=True, aircraft=aircraft, latency_ms=dt)
        except Exception as e:
            dt = int((time.time() - t0) * 1000)
            return FetchResult(ok=False, aircraft=[], error=f"{name} fetch error: {e}", latency_ms=dt)

    def get_data(self) -> Tuple[Optional[List[Dict[str, Any]]], Optional[List[Dict[str, Any]]], Dict[str, FetchResult]]:
        """
        Fetch in parallel. Return (all, mil, debug_results)
        Simplified to only 2 endpoints.
        """
        tasks = [("all", ALL_URL), ("mil", MIL_URL)]
        results: Dict[str, FetchResult] = {}

        with ThreadPoolExecutor(max_workers=2) as ex:
            future_to_key = {ex.submit(self.fetch_url, k, u): k for (k, u) in tasks}
            for fut in as_completed(future_to_key):
                key = future_to_key[fut]
                results[key] = fut.result()

        all_ok = results.get("all", FetchResult(False, [])).ok
        mil_ok = results.get("mil", FetchResult(False, [])).ok

        if not (all_ok or mil_ok):
            self.consecutive_failures += 1
            if self.consecutive_failures >= FAILURE_THRESHOLD:
                self._alert_connectivity(results)
                self.consecutive_failures = 0
            return None, None, results

        self.consecutive_failures = 0
        all_ac = results["all"].aircraft if all_ok else None
        mil_ac = results["mil"].aircraft if mil_ok else None
        return all_ac, mil_ac, results

    def _alert_connectivity(self, results: Dict[str, FetchResult]) -> None:
        lines = ["<b>⚠️ API connectivity problem</b>"]
        for k in ("all", "mil"):
            r = results.get(k)
            if not r:
                lines.append(f"• {k}: no result")
                continue
            status = "OK" if r.ok else "FAIL"
            extra = f" ({r.latency_ms}ms)" if r.latency_ms is not None else ""
            err = f" — {html_escape(r.error)}" if r.error else ""
            lines.append(f"• {k}: <code>{status}</code>{extra}{err}")
        try:
            self.tg.send_chunked_html("\n".join(lines))
        except Exception as e:
            logging.error(f"Telegram connectivity alert failed: {e}")

    # ---------- Military operation signals ----------

    def build_interest_sets(
        self,
        all_ac: Optional[List[Dict[str, Any]]],
        mil_ac: Optional[List[Dict[str, Any]]]
    ) -> Dict[str, Any]:
        """
        We preserve data.
        - mil_hex_set: from MIL endpoint + calls/mil from ALL
        """
        mil_hex_set = set()

        if mil_ac:
            for a in mil_ac:
                hx = self.get_hex(a)
                if hx:
                    mil_hex_set.add(hx)

        if all_ac:
            for a in all_ac:
                if self.is_military_callsign(a.get("flight")) or bool(a.get("mil")):
                    hx = self.get_hex(a)
                    if hx:
                        mil_hex_set.add(hx)

        return {
            "mil_hex_set": mil_hex_set
        }

    def detect_enter_exit(
        self,
        current_map: Dict[str, Dict[str, Any]],
        active_map: Dict[str, Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]]]:
        new_items = []
        gone_items = []

        for hx, ac in current_map.items():
            if hx and hx not in active_map:
                new_items.append(ac)

        for hx, ac in active_map.items():
            if hx and hx not in current_map:
                gone_items.append(ac)

        return new_items, gone_items

    def check_refueling_events(
        self,
        tankers_in_region_airborne: List[Dict[str, Any]],
        military_in_region_airborne: List[Dict[str, Any]]
    ) -> List[str]:
        """
        Alert logic requires lat/lon/alt/gs.
        Raw records are already stored separately even if missing fields.
        """
        events: List[str] = []

        for t in tankers_in_region_airborne:
            t_hex = self.get_hex(t)
            t_lat = safe_float(t.get("lat"))
            t_lon = safe_float(t.get("lon"))
            t_alt = safe_float(t.get("alt_baro"))
            t_gs = safe_float(t.get("gs"))
            if not (t_hex and t_lat is not None and t_lon is not None and t_alt is not None):
                continue

            for m in military_in_region_airborne:
                m_hex = self.get_hex(m)
                if not m_hex or m_hex == t_hex:
                    continue

                m_lat = safe_float(m.get("lat"))
                m_lon = safe_float(m.get("lon"))
                m_alt = safe_float(m.get("alt_baro"))
                m_gs = safe_float(m.get("gs"))

                if m_lat is None or m_lon is None or m_alt is None:
                    continue

                dist = haversine_km(t_lat, t_lon, m_lat, m_lon)
                alt_diff = abs(t_alt - m_alt)

                speed_diff = 0.0
                if t_gs is not None and m_gs is not None:
                    speed_diff = abs(t_gs - m_gs)

                if dist <= REFUEL_MAX_DIST_KM and alt_diff <= REFUEL_MAX_ALT_DIFF_FT and speed_diff <= REFUEL_MAX_SPEED_DIFF_KT:
                    key = f"{t_hex}|{m_hex}"
                    now = time.time()
                    last = self.last_refuel_alert.get(key, 0.0)
                    if now - last < REFUEL_PAIR_COOLDOWN_SEC:
                        continue
                    self.last_refuel_alert[key] = now

                    t_call = html_escape(self.flight_id(t))
                    m_call = html_escape(self.flight_id(m))
                    adsb_link = f"https://globe.adsb.lol/?hex={t_hex}"
                    gmap_link = f"https://www.google.com/maps?q={t_lat},{t_lon}"

                    events.append(
                        "<b>⛽ Possible Aerial Refueling</b>\n"
                        f"• Tanker: <code>{t_call}</code>\n"
                        f"• Receiver: <code>{m_call}</code>\n"
                        f"• Dist: <b>{dist:.1f} km</b> | Alt Δ: <b>{alt_diff:.0f} ft</b> | Speed Δ: <b>{speed_diff:.0f} kt</b>\n"
                        f"• ADS-B: {html_escape(adsb_link)}\n"
                        f"• Map: {html_escape(gmap_link)}"
                    )

                    # Persist event detail
                    self.store.append("events", {
                        "ts": utc_iso(),
                        "type": "refueling_possible",
                        "tanker_hex": t_hex,
                        "receiver_hex": m_hex,
                        "dist_km": dist,
                        "alt_diff_ft": alt_diff,
                        "speed_diff_kt": speed_diff,
                        "tanker": t,
                        "receiver": m,
                    })

        return events

    # ---------- Main analysis cycle ----------

    def analyze_once(self) -> None:
        all_ac, mil_ac, debug = self.get_data()

        # Build interest sets (mil hex sets)
        interest = self.build_interest_sets(all_ac, mil_ac)
        mil_hex_set = interest["mil_hex_set"]

        # Optimize snapshots: only store "interesting" aircraft to save space.
        # Interesting = military, tanker, or within our target region (Iran/Gulf).
        filtered_all = []
        if all_ac:
            for ac in all_ac:
                hx = self.get_hex(ac)
                is_mil = (hx in mil_hex_set) or bool(ac.get("mil")) or self.is_military_callsign(ac.get("flight"))
                in_reg, _ = self.in_region_or_unknown(ac)
                if is_mil or in_reg:
                    filtered_all.append(ac)

        self.store.append("snapshots", {
            "ts": utc_iso(),
            "region_bbox": REGION_BBOX,
            "fetch": {
                k: {"ok": v.ok, "latency_ms": v.latency_ms, "error": v.error}
                for k, v in debug.items()
            },
            "counts": {
                "all_raw": len(all_ac) if all_ac is not None else None,
                "all_stored": len(filtered_all),
                "mil_raw": len(mil_ac) if mil_ac is not None else None,
            },
            "aircraft": filtered_all,
        })

        # Build region subsets (for operational signals)
        flights_in_region: List[Dict[str, Any]] = []
        military_in_region: List[Dict[str, Any]] = []

        # From ALL: we catch military-looking flights + tankers
        if all_ac:
            for ac in all_ac:
                in_reg, has_loc = self.in_region_or_unknown(ac)
                if not in_reg:
                    continue
                flights_in_region.append(ac)

                hx = self.get_hex(ac)
                is_mil = (hx in mil_hex_set) or bool(ac.get("mil")) or self.is_military_callsign(ac.get("flight"))

                # Preserve and classify
                if is_mil:
                    military_in_region.append(ac)

        # From MIL endpoint
        if mil_ac:
            for ac in mil_ac:
                in_reg, has_loc = self.in_region_or_unknown(ac)
                if in_reg:
                    military_in_region.append(ac)

        # Deduplicate
        def dedup_by_hex(lst: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
            m: Dict[str, Dict[str, Any]] = {}
            for a in lst:
                hx = self.get_hex(a)
                if hx:
                    m[hx] = a
            return m

        mil_map = dedup_by_hex(military_in_region)
        
        # New Tanker Filter logic: filter from deduplicated military map
        tanker_map = {hx: ac for hx, ac in mil_map.items() if self.is_tanker(ac)}

        # Enter/Exit detection (military + tankers)
        new_mil, gone_mil = self.detect_enter_exit(mil_map, self.active_military)
        new_tank, gone_tank = self.detect_enter_exit(tanker_map, self.active_tankers)

        # Commercial count (only if ALL is available)
        commercial_count = None
        if flights_in_region:
            # commercial = in region AND NOT flagged mil/tanker/callsign-mil
            commercial = []
            for ac in flights_in_region:
                hx = self.get_hex(ac)
                is_mil = (hx in mil_hex_set) or bool(ac.get("mil")) or self.is_military_callsign(ac.get("flight"))
                is_tank = self.is_tanker(ac)
                if not is_mil and not is_tank:
                    # For traffic volume, typically airborne; but keep option:
                    if self.is_airborne(ac):
                        commercial.append(ac)
            commercial_count = len(commercial)

        # Moving average / drop alert
        drop_alert_text = None
        if commercial_count is not None:
            self.commercial_history.append(commercial_count)
            if len(self.commercial_history) == MOVING_AVG_WINDOW:
                avg = sum(self.commercial_history) / MOVING_AVG_WINDOW
                if avg > 0 and commercial_count < avg * (1 - COMMERCIAL_DROP_THRESHOLD):
                    now = time.time()
                    if now - self.last_commercial_alert_ts > COMMERCIAL_ALERT_COOLDOWN_SEC:
                        drop_pct = int(((avg - commercial_count) / avg) * 100)
                        drop_alert_text = (
                            "<b>📉 Significant Commercial Traffic Drop</b>\n"
                            f"• Drop: <b>{drop_pct}%</b>\n"
                            f"• Avg: <b>{int(avg)}</b> | Now: <b>{commercial_count}</b>\n"
                            "• Possible airspace restriction/clearance signal."
                        )
                        self.last_commercial_alert_ts = now
                        self.store.append("events", {
                            "ts": utc_iso(),
                            "type": "commercial_drop",
                            "avg": avg,
                            "current": commercial_count,
                            "drop_pct": drop_pct,
                        })

        # ----- Z-Score Extension -----
        z_alerts = []
        if ZSCORE_ENABLED:
            # Update histories
            self.zscore_histories["commercial"].append(commercial_count or 0)
            self.zscore_histories["tanker"].append(len(tanker_map))
            self.zscore_histories["military"].append(len(mil_map))
            
            for cat, hist in self.zscore_histories.items():
                if len(hist) >= ZSCORE_MIN_SAMPLES:
                    arr = np.array(hist)
                    mean = np.mean(arr)
                    std = np.std(arr)
                    if std == 0: std = 1
                    current = arr[-1]
                    z = (current - mean) / std
                    if abs(z) > ZSCORE_THRESHOLD:
                        direction = "Spike 🔥" if z > 0 else "Drop 📉"
                        z_alerts.append(
                            f"<b>Z-Score Alert {direction}</b>\n"
                            f"• {cat.capitalize()}: Z = <b>{z:.2f}</b>\n"
                            f"• Current: <b>{int(current)}</b> | Mean: <b>{mean:.1f}</b>"
                        )
                        self.store.append("events", {
                            "ts": utc_iso(),
                            "type": "zscore_anomaly",
                            "category": cat,
                            "z": z,
                            "current": int(current),
                            "mean": mean,
                        })

        # ----- Corridor Extension -----
        corridor_alerts = []
        if CORRIDOR_ENABLED:
            # Reset counts
            for name in self.corridor_counts:
                self.corridor_counts[name] = 0
            
            # Count airborne in each corridor (commercial + mil/tanker)
            for ac in flights_in_region:
                if not self.is_airborne(ac):
                    continue
                for corr in CORRIDORS:
                    if self.in_corridor(ac, corr):
                        self.corridor_counts[corr["name"]] += 1

            # Update histories and Calculate Z-Score per corridor
            for corr in CORRIDORS:
                name = corr["name"]
                count = self.corridor_counts[name]
                hist = self.corridor_z_histories[name]
                hist.append(count)
                
                if len(hist) >= ZSCORE_MIN_SAMPLES:
                    arr = np.array(hist)
                    mean = np.mean(arr)
                    std = np.std(arr)
                    if std == 0: std = 1
                    z = (count - mean) / std
                    if abs(z) > ZSCORE_THRESHOLD:
                        direction = "Activity Spike 🍒" if z > 0 else "Clearance 📉"
                        corridor_alerts.append(
                            f"<b>Corridor Alert: {name}</b>\n"
                            f"• {direction} Z = <b>{z:.2f}</b>\n"
                            f"• Count: <b>{count}</b>"
                        )
                        self.store.append("events", {
                            "ts": utc_iso(),
                            "type": "corridor_anomaly",
                            "corridor": name,
                            "z": z,
                            "count": count,
                        })

        # Refueling (airborne-only for calculation)
        tankers_airborne = [a for a in tanker_map.values() if self.is_airborne(a)]
        mil_airborne = [a for a in mil_map.values() if self.is_airborne(a)]
        refuel_events = self.check_refueling_events(tankers_airborne, mil_airborne)

        # Compose alert message (military operation visibility)
        msg_lines: List[str] = []
        msg_lines.append(f"<b>🛰 SkyMonitor Scan</b> <code>{html_escape(utc_iso())}</code>")

        # Summaries (keep it readable, but data is stored in snapshots)
        msg_lines.append(
            "• Region counts — "
            f"Military: <b>{len(mil_map)}</b>, Tankers: <b>{len(tanker_map)}</b>"
            + (f", Commercial(airborne): <b>{commercial_count}</b>" if commercial_count is not None else ", Commercial: <code>n/a</code>")
        )

        if drop_alert_text:
            msg_lines.append("")
            msg_lines.append(drop_alert_text)

        if z_alerts:
            msg_lines.append("")
            msg_lines.extend(z_alerts)
        
        if corridor_alerts:
            msg_lines.append("")
            msg_lines.append("<b>🔥 Corridor Hotspots</b>")
            msg_lines.extend(corridor_alerts)
            # Summary counts
            msg_lines.append("\n<b>Corridor Counts</b>")
            for name, count in self.corridor_counts.items():
                msg_lines.append(f"• {name}: <b>{count}</b> airborne")

        # New / gone tankers
        if new_tank:
            msg_lines.append("")
            msg_lines.append("<b>🆕 New Tankers in Region</b>")
            for t in new_tank[:25]:
                hx = self.get_hex(t)
                call = html_escape(self.flight_id(t))
                typ = html_escape(safe_str(t.get("t") or t.get("desc") or ""))
                lat = safe_float(t.get("lat"))
                lon = safe_float(t.get("lon"))
                adsb = f"https://globe.adsb.lol/?hex={hx}" if hx else ""
                gmap = f"https://www.google.com/maps?q={lat},{lon}" if (lat is not None and lon is not None) else ""
                msg_lines.append(f"• <code>{call}</code> {typ} — {html_escape(adsb)} {html_escape(gmap)}")
                self.store.append("events", {"ts": utc_iso(), "type": "tanker_enter", "hex": hx, "aircraft": t})

        if gone_tank:
            msg_lines.append("")
            msg_lines.append("<b>🚪 Tankers Exited Region</b>")
            for t in gone_tank[:25]:
                hx = self.get_hex(t)
                call = html_escape(self.flight_id(t))
                msg_lines.append(f"• <code>{call}</code> ({html_escape(hx)})")
                self.store.append("events", {"ts": utc_iso(), "type": "tanker_exit", "hex": hx, "aircraft": t})

        # New / gone military
        if new_mil:
            msg_lines.append("")
            msg_lines.append("<b>🆕 New Military Aircraft in Region</b>")
            for m in new_mil[:40]:
                hx = self.get_hex(m)
                call = html_escape(self.flight_id(m))
                alt = safe_str(m.get("alt_baro"))
                gs = safe_str(m.get("gs"))
                lat = safe_float(m.get("lat"))
                lon = safe_float(m.get("lon"))
                adsb = f"https://globe.adsb.lol/?hex={hx}" if hx else ""
                gmap = f"https://www.google.com/maps?q={lat},{lon}" if (lat is not None and lon is not None) else ""
                airborne = "airborne" if self.is_airborne(m) else "ground/unknown"
                msg_lines.append(f"• <code>{call}</code> ({html_escape(hx)}) — {html_escape(airborne)} — alt:{html_escape(alt)} gs:{html_escape(gs)} — {html_escape(adsb)} {html_escape(gmap)}")
                self.store.append("events", {"ts": utc_iso(), "type": "mil_enter", "hex": hx, "aircraft": m})

        if gone_mil:
            msg_lines.append("")
            msg_lines.append("<b>🚪 Military Aircraft Exited Region</b>")
            for m in gone_mil[:40]:
                hx = self.get_hex(m)
                call = html_escape(self.flight_id(m))
                msg_lines.append(f"• <code>{call}</code> ({html_escape(hx)})")
                self.store.append("events", {"ts": utc_iso(), "type": "mil_exit", "hex": hx, "aircraft": m})

        # Refueling
        if refuel_events:
            msg_lines.append("")
            msg_lines.extend(refuel_events)

        # If nothing notable, avoid spamming Telegram but still log/store snapshots.
        notable = bool(drop_alert_text or new_tank or gone_tank or new_mil or gone_mil or refuel_events)
        if notable:
            try:
                self.tg.send_chunked_html("\n".join(msg_lines))
            except Exception as e:
                logging.error(f"Telegram send failed: {e}")

        # Update state
        self.active_tankers = tanker_map
        self.active_military = mil_map

        logging.info(
            f"Scan done | mil={len(mil_map)} tanker={len(tanker_map)} commercial={commercial_count if commercial_count is not None else 'n/a'}"
        )

# ================== Graceful shutdown ==================

STOP = False

def _handle_shutdown(signum, frame):
    global STOP
    STOP = True
    logging.info(f"Shutdown signal {signum} received. Will stop after current cycle.")

# ================== Run ==================

def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[logging.FileHandler("sky_monitor.log"), logging.StreamHandler()]
    )

    ensure_dir(DATA_DIR)
    
    # Run initial cleanup
    store = JsonlStore(DATA_DIR)
    store.purge_old(SNAPSHOT_KEEP_DAYS)

    signal.signal(signal.SIGTERM, _handle_shutdown)
    signal.signal(signal.SIGINT, _handle_shutdown)

    monitor = ProductionSkyMonitor()

    # Startup message
    try:
        monitor.tg.send_chunked_html("<b>🛰 SkyMonitor started</b>\n• Military-focused mode\n• Raw snapshots + events are being stored.")
    except Exception as e:
        logging.error(f"Startup telegram failed: {e}")

    while not STOP:
        try:
            monitor.analyze_once()
        except Exception as e:
            logging.exception(f"Unexpected error in analyze_once: {e}")
            # still continue loop; this is a long-running service
        # Sleep in small steps to react faster to shutdown
        for _ in range(CHECK_INTERVAL_SEC):
            if STOP:
                break
            time.sleep(1)

    logging.info("SkyMonitor stopped cleanly.")

if __name__ == "__main__":
    main()
