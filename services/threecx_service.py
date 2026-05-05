"""
3CX Call Control Service — XAPI v1 Polling

Funktioniert mit:
  POST /connect/token        → OAuth2 client_credentials
  GET  /xapi/v1/activecalls  → OData, alle aktiven Anrufe (alle 3s gepollt)

ActiveCall-Felder: Id, Caller, Callee, Status, LastChangeStatus, EstablishedAt
"""
import json
import logging
import os
import queue
import re
import threading
import time
from datetime import datetime

import requests
from requests.packages.urllib3.exceptions import InsecureRequestWarning

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

logger = logging.getLogger(__name__)

THREECX_HOST = os.getenv("THREECX_HOST", "")
THREECX_PORT = os.getenv("THREECX_PORT", "5001")
THREECX_CLIENT_ID = os.getenv("THREECX_CLIENT_ID", "")
THREECX_CLIENT_SECRET = os.getenv("THREECX_CLIENT_SECRET", "")
THREECX_EXTENSION = os.getenv("THREECX_EXTENSION", "")

_event_queue: queue.Queue = queue.Queue(maxsize=200)
_call_log: list = []
_raw_poll_log: list = []   # letzte 30 rohe activecalls-Antworten (inkl. leere)
_current_call: dict | None = None
_phone_index: dict = {}
_phonebook_index: dict = {}
_status: dict = {
    "connected": False,
    "mode": "starting",
    "last_error": None,
    "last_event": None,
    "permissions_ok": None,
}
_lock = threading.Lock()
_token_cache: dict = {"token": None, "expires_at": 0}
_known_call_ids: set = set()  # IDs aktuell aktiver Anrufe auf Extension 94
_tracked_calls: dict = {}    # Alle aktiven Anrufe für Statistik-Persistenz


def _matches_extension(field: str, ext: str) -> bool:
    """Prüft ob field die Extension ext enthält.
    Matcht '94', '94 Hohagen, Ralph', 'sip:94@host' — aber nicht '940' oder '194'."""
    return bool(re.search(rf'(?<!\d){re.escape(ext)}(?!\d)', field))


def _extract_caller_phone(caller: str) -> str:
    """Extrahiert die Rufnummer aus '10001 OB-Trunk 06051-53830 0-99 (015208784566)'.
    Gibt die Nummer in Klammern zurück, sonst die längste Ziffernfolge, sonst caller."""
    m = re.search(r'\((\d[\d\s\-/+]+)\)\s*$', caller.strip())
    if m:
        return m.group(1).strip()
    m = re.search(r'\d{7,}', caller)
    if m:
        return m.group(0)
    return caller


def _extract_short_number(field: str) -> str | None:
    """Extrahiert Nebenstellen-Nummer aus '94 Hohagen, Ralph' → '94'.
    Gibt None zurück wenn kein Kurznummer-Muster (Amtsleitungen haben Wert ≥ 500)."""
    m = re.match(r'^(\d{1,4})\b', field.strip())
    if m and int(m.group(1)) < 500:
        return m.group(1)
    return None


def _classify_call(caller: str, callee: str, status: str) -> tuple:
    """Gibt (direction, extension, phone) zurück."""
    if callee == "ROUTER":
        return "inbound", None, _extract_caller_phone(caller)
    callee_ext = _extract_short_number(callee)
    caller_ext = _extract_short_number(caller)
    if callee_ext and not caller_ext:
        return "inbound", callee_ext, _extract_caller_phone(caller)
    if caller_ext and not callee_ext:
        return "outbound", caller_ext, callee.strip()
    if caller_ext and callee_ext:
        return "internal", caller_ext, callee_ext
    return "unknown", None, caller


def _persist_call_to_supabase(entry: dict):
    """Speichert abgeschlossenen Anruf in Supabase call_log."""
    try:
        from core.database import supabase as _sb
        if not _sb:
            return
        if entry.get("had_voicemail"):
            outcome = "voicemail"
        elif entry.get("answered_at"):
            outcome = "answered"
        else:
            outcome = "missed"
        customer = _lookup_customer(entry.get("phone", ""))
        caller_name = (customer or {}).get("name") or None
        _sb.table("call_log").insert({
            "call_id": entry["call_id"],
            "extension": entry.get("extension"),
            "phone": entry.get("phone"),
            "direction": entry.get("direction"),
            "outcome": outcome,
            "started_at": entry["started_at"],
            "answered_at": entry.get("answered_at"),
            "ended_at": entry.get("ended_at"),
            "caller_raw": (entry.get("caller_raw") or "")[:200],
            "callee_raw": (entry.get("callee_raw") or "")[:200],
            "caller_name": caller_name,
        }).execute()
        logger.info(f"[3CX] Persistiert: call={entry['call_id']}, ext={entry.get('extension')}, outcome={outcome}, name={caller_name or '?'}")
    except Exception as e:
        logger.warning(f"[3CX] Persist fehlgeschlagen für {entry.get('call_id')}: {e}")


# ─── Phone index ─────────────────────────────────────────────────────────────

def _normalize_phone(phone: str) -> str:
    if not phone:
        return ""
    p = str(phone).strip()
    p = p.replace(" ", "").replace("-", "").replace("/", "").replace("(", "").replace(")", "").replace(".", "")
    if p.startswith("+49"):
        p = "0" + p[3:]
    elif p.startswith("0049"):
        p = "0" + p[4:]
    return p


def _build_phone_index():
    global _phone_index
    index: dict = {}
    try:
        from core.config import SYSCARA_BASE
        from core.database import get_cached_or_fetch, iter_items

        raw = get_cached_or_fetch("sale/vehicles", f"{SYSCARA_BASE}/sale/vehicles/")
        vehicles = iter_items(raw) if raw else []

        for v in vehicles:
            customer = v.get("customer") or {}
            if isinstance(customer, list):
                customer = {}

            name = f"{customer.get('firstname', '')} {customer.get('lastname', '')}".strip()
            ident = v.get("identifier") or {}
            if isinstance(ident, list):
                ident = {}
            model_obj = v.get("model") or {}
            if isinstance(model_obj, list):
                model_obj = {}

            info = {
                "name": name,
                "email": customer.get("email", ""),
                "mobile": customer.get("mobile", ""),
                "telephone": customer.get("telephone", ""),
                "vehicle_internal": ident.get("internal", ""),
                "vehicle_make": model_obj.get("make", ""),
                "vehicle_model": model_obj.get("name", ""),
            }

            for field in ("mobile", "telephone"):
                raw_phone = customer.get(field, "")
                if raw_phone:
                    norm = _normalize_phone(str(raw_phone))
                    if norm and len(norm) >= 5:
                        index[norm] = info

        logger.info(f"[3CX] Phone index: {len(index)} entries from {len(vehicles)} vehicles")
    except Exception as e:
        logger.warning(f"[3CX] Phone index build failed: {e}")

    with _lock:
        _phone_index = index


def _build_phonebook_index():
    global _phonebook_index
    index: dict = {}
    try:
        from core.database import supabase as _sb
        if not _sb:
            return
        res = _sb.table("phonebook").select("*").execute()
        for entry in (res.data or []):
            info = {
                "name": entry.get("name", ""),
                "email": "",
                "mobile": entry.get("mobile", ""),
                "telephone": entry.get("phone", ""),
                "phone2": entry.get("phone2", ""),
                "vehicle_internal": "",
                "vehicle_make": "",
                "vehicle_model": "",
                "source": "phonebook",
            }
            for field in ("phone", "mobile", "phone2"):
                raw = entry.get(field, "")
                if raw:
                    norm = _normalize_phone(str(raw))
                    if norm and len(norm) >= 5:
                        index[norm] = info
        logger.info(f"[3CX] Phonebook index: {len(index)} entries")
    except Exception as e:
        logger.warning(f"[3CX] Phonebook index build failed: {e}")
    with _lock:
        _phonebook_index = index


def rebuild_phonebook_index():
    threading.Thread(target=_build_phonebook_index, daemon=True, name="phonebook-rebuild").start()


def _lookup_customer(phone: str) -> dict | None:
    norm = _normalize_phone(phone)
    if not norm:
        return None
    with _lock:
        result = _phone_index.get(norm)
        if result:
            return result
        return _phonebook_index.get(norm)


# ─── Auth ─────────────────────────────────────────────────────────────────────

def _get_token() -> str | None:
    global _token_cache
    now = time.time()
    if _token_cache["token"] and _token_cache["expires_at"] > now + 60:
        return _token_cache["token"]

    url = f"https://{THREECX_HOST}:{THREECX_PORT}/connect/token"
    try:
        r = requests.post(
            url,
            data={
                "grant_type": "client_credentials",
                "client_id": THREECX_CLIENT_ID,
                "client_secret": THREECX_CLIENT_SECRET,
                "scope": "ReadCalls",
            },
            timeout=15,
            verify=False,
        )
        r.raise_for_status()
        data = r.json()
        token = data.get("access_token")
        expires_in = data.get("expires_in", 3600)
        _token_cache = {"token": token, "expires_at": now + expires_in}
        logger.info("[3CX] Token acquired")
        return token
    except Exception as e:
        logger.error(f"[3CX] Token error: {e}")
        return None


# ─── Events ───────────────────────────────────────────────────────────────────

def _push_event(event_type: str, payload: dict):
    global _current_call
    data = {
        "type": event_type,
        "payload": payload,
        "timestamp": datetime.now().isoformat(),
    }
    with _lock:
        _status["last_event"] = event_type
        try:
            _event_queue.put_nowait(data)
        except queue.Full:
            try:
                _event_queue.get_nowait()
            except queue.Empty:
                pass
            _event_queue.put_nowait(data)

        if event_type == "call_incoming":
            _current_call = payload
            _call_log.insert(0, {**data, "id": f"{int(time.time() * 1000)}"})
            if len(_call_log) > 100:
                _call_log.pop()
        elif event_type in ("call_ended",):
            _current_call = None


def handle_webhook_payload(payload: dict):
    """Webhook-Empfänger als zusätzlicher Kanal (optional in 3CX konfigurierbar)."""
    logger.info(f"[3CX] Webhook: {json.dumps(payload)[:400]}")
    phone = (
        payload.get("caller_id") or payload.get("CallerID")
        or payload.get("from") or payload.get("Caller") or ""
    )
    event = payload.get("event_type") or payload.get("type") or payload.get("Status") or ""
    state = payload.get("state") or payload.get("Status") or ""

    if phone and state in ("Ringing", "Dialing"):
        customer = _lookup_customer(phone)
        _push_event("call_incoming", {"phone": phone, "customer": customer, "state": state})
    elif state in ("Idle", "Hung Up", "Disconnected"):
        _push_event("call_ended", {"state": state})
    else:
        _push_event("raw_event", {"raw": payload, "event": event})


def handle_incoming_call(phone: str, state: str = "Ringing", raw_event: str = ""):
    customer = _lookup_customer(phone)
    _push_event("call_incoming", {"phone": phone, "customer": customer, "state": state, "event": raw_event})


# ─── XAPI Polling ─────────────────────────────────────────────────────────────

def _poll_activecalls(token: str) -> bool:
    """
    Poll /xapi/v1/activecalls, filter für Extension 94.
    Gibt False zurück wenn HTTP-Fehler aufgetreten ist.
    """
    global _known_call_ids

    url = f"https://{THREECX_HOST}:{THREECX_PORT}/xapi/v1/activecalls"
    try:
        r = requests.get(
            url,
            headers={"Authorization": f"Bearer {token}"},
            timeout=10,
            verify=False,
        )

        if r.status_code == 401:
            _token_cache["token"] = None  # Force token refresh
            return False

        if r.status_code != 200:
            with _lock:
                _status["last_error"] = f"HTTP {r.status_code} auf /xapi/v1/activecalls"
                _status["permissions_ok"] = r.status_code != 403
            logger.warning(f"[3CX] Poll: HTTP {r.status_code}")
            return False

        with _lock:
            _status["permissions_ok"] = True
            _status["connected"] = True
            _status["last_error"] = None

        calls = r.json().get("value", [])

        # Rohdaten immer speichern (auch leere Polls) für Debug
        with _lock:
            _raw_poll_log.insert(0, {
                "ts": datetime.now().isoformat(),
                "count": len(calls),
                "calls": calls[:10],  # max 10 Einträge
            })
            if len(_raw_poll_log) > 30:
                _raw_poll_log.pop()

        if calls:
            logger.info(f"[3CX] Aktive Anrufe ({len(calls)}): {json.dumps(calls)[:800]}")
            print(f"[3CX] Aktive Anrufe: {json.dumps(calls)[:800]}", flush=True)

        # Nur Anrufe die Extension 94 betreffen — interne VoiceMail/Transfer-Anrufe ausschließen
        def _is_my_call(c: dict) -> bool:
            caller = str(c.get("Caller", ""))
            callee = str(c.get("Callee", ""))
            if "VoiceMail" in caller or "VoiceMail" in callee:
                return False
            # Eingehender Anruf in Routing-Phase: 3CX trägt "ROUTER" als Callee ein
            if callee == "ROUTER" and c.get("Status") == "Routing":
                return True
            return _matches_extension(callee, THREECX_EXTENSION) or _matches_extension(caller, THREECX_EXTENSION)

        my_calls = {c["Id"]: c for c in calls if _is_my_call(c)}

        current_ids = set(my_calls.keys())

        with _lock:
            known = set(_known_call_ids)

        # Neue Anrufe
        for call_id in current_ids - known:
            call = my_calls[call_id]
            caller = call.get("Caller", "")
            callee = call.get("Callee", "")
            status = call.get("Status", "Routing")

            if callee == "ROUTER":
                # Routing-Phase: Rufnummer steckt in Klammern im Caller-Feld
                # z.B. "10001 OB-Trunk 06051-53830 0-99 (015208784566)"
                phone = _extract_caller_phone(caller)
            else:
                # Etablierter Anruf: Callee = 94, Caller = externe Nummer
                phone = caller if _matches_extension(callee, THREECX_EXTENSION) else callee

            logger.info(f"[3CX] Neuer Anruf #{call_id}: {caller} → {callee} ({status}) → phone={phone}")
            customer = _lookup_customer(phone)
            _push_event("call_incoming", {
                "phone": phone,
                "customer": customer,
                "state": status,
                "call_id": call_id,
            })

        # Beendete Anrufe
        for call_id in known - current_ids:
            logger.info(f"[3CX] Anruf #{call_id} beendet")
            _push_event("call_ended", {"call_id": call_id, "state": "Idle"})

        with _lock:
            _known_call_ids = current_ids

        # ── Statistik-Tracking: alle Anrufe (nicht nur ext THREECX_EXTENSION) ──
        all_current_ids = {c["Id"] for c in calls}

        for c in calls:
            cid = c["Id"]
            caller = str(c.get("Caller", ""))
            callee = str(c.get("Callee", ""))
            status_val = str(c.get("Status", ""))

            if cid not in _tracked_calls:
                direction, extension, phone = _classify_call(caller, callee, status_val)
                _tracked_calls[cid] = {
                    "call_id": cid,
                    "extension": extension,
                    "phone": phone,
                    "caller_raw": caller,
                    "callee_raw": callee,
                    "direction": direction,
                    "started_at": datetime.now().isoformat(),
                    "answered_at": None,
                    "ended_at": None,
                    "had_voicemail": False,
                }
            else:
                entry = _tracked_calls[cid]
                if status_val == "Talking" and not entry["answered_at"]:
                    entry["answered_at"] = datetime.now().isoformat()
                if "VoiceMail" in callee or "VoiceMail" in caller:
                    entry["had_voicemail"] = True
                # Extension aus ROUTER-Phase nachträglich auflösen
                if entry["extension"] is None and callee not in ("ROUTER", ""):
                    ext = _extract_short_number(callee)
                    if ext:
                        entry["extension"] = ext

        ended_stat_ids = set(_tracked_calls.keys()) - all_current_ids
        for cid in ended_stat_ids:
            entry = _tracked_calls.pop(cid)
            entry["ended_at"] = datetime.now().isoformat()
            threading.Thread(
                target=_persist_call_to_supabase, args=(entry,), daemon=True, name="call-persist"
            ).start()

        return True

    except Exception as e:
        logger.error(f"[3CX] Poll error: {e}")
        with _lock:
            _status["last_error"] = str(e)
        return False


def _polling_loop():
    logger.info("[3CX] XAPI polling gestartet (/xapi/v1/activecalls, alle 1s)")
    with _lock:
        _status["mode"] = "polling"

    consecutive_errors = 0

    while True:
        token = _get_token()
        if not token:
            time.sleep(30)
            continue

        ok = _poll_activecalls(token)
        if ok:
            consecutive_errors = 0
        else:
            consecutive_errors += 1
            if consecutive_errors >= 5:
                with _lock:
                    _status["connected"] = False
                logger.warning("[3CX] 5 aufeinanderfolgende Fehler, warte 30s...")
                time.sleep(30)
                consecutive_errors = 0
                continue

        time.sleep(1)


# ─── Background threads ───────────────────────────────────────────────────────

def _index_refresh_loop():
    time.sleep(30)
    while True:
        _build_phone_index()
        time.sleep(1800)


def _phonebook_refresh_loop():
    time.sleep(15)
    while True:
        _build_phonebook_index()
        time.sleep(600)


# ─── Public API ───────────────────────────────────────────────────────────────

def start_threecx_service():
    if not THREECX_HOST or not THREECX_CLIENT_ID:
        logger.warning("[3CX] Nicht konfiguriert (THREECX_HOST + THREECX_CLIENT_ID fehlen)")
        return

    threading.Thread(target=_build_phone_index, daemon=True, name="threecx-init").start()
    threading.Thread(target=_build_phonebook_index, daemon=True, name="phonebook-init").start()
    threading.Thread(target=_index_refresh_loop, daemon=True, name="threecx-index").start()
    threading.Thread(target=_phonebook_refresh_loop, daemon=True, name="phonebook-index").start()
    threading.Thread(target=_polling_loop, daemon=True, name="threecx-poll").start()
    logger.info("[3CX] Service gestartet (XAPI Polling)")


def get_event_stream():
    while True:
        try:
            event = _event_queue.get(timeout=25)
            yield f"data: {json.dumps(event)}\n\n"
        except queue.Empty:
            yield 'data: {"type":"heartbeat"}\n\n'


def get_status() -> dict:
    with _lock:
        return {
            **_status,
            "extension": THREECX_EXTENSION,
            "host": THREECX_HOST,
            "phone_index_size": len(_phone_index),
            "phonebook_index_size": len(_phonebook_index),
            "current_call": _current_call,
            "configured": bool(THREECX_HOST and THREECX_CLIENT_ID),
        }


def get_call_log() -> list:
    with _lock:
        return list(_call_log)


def get_raw_poll_log() -> list:
    with _lock:
        return list(_raw_poll_log)


def get_current_call() -> dict | None:
    with _lock:
        return _current_call


def lookup_customer_by_phone(phone: str) -> dict | None:
    return _lookup_customer(phone)
