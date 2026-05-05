from flask import Response, jsonify, request, stream_with_context
from core.database import supabase
from services.threecx_service import (
    get_call_log,
    get_current_call,
    get_event_stream,
    get_raw_poll_log,
    get_status,
    handle_webhook_payload,
    lookup_customer_by_phone,
    rebuild_phonebook_index,
)


def register_telephony_routes(app):

    @app.route("/api/telephony/stream")
    def telephony_stream():
        def generate():
            yield from get_event_stream()

        return Response(
            stream_with_context(generate()),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache, no-store, must-revalidate",
                "X-Accel-Buffering": "no",
                "Connection": "keep-alive",
            },
        )

    @app.route("/api/telephony/status")
    def telephony_status():
        return jsonify({"success": True, "status": get_status()})

    @app.route("/api/telephony/current")
    def telephony_current():
        return jsonify({"success": True, "call": get_current_call()})

    @app.route("/api/telephony/log")
    def telephony_log():
        if supabase:
            try:
                res = (
                    supabase.table("call_log")
                    .select("*")
                    .order("started_at", desc=True)
                    .limit(200)
                    .execute()
                )
                rows = res.data or []
                log = []
                for row in rows:
                    name = row.get("caller_name") or ""
                    if not name and row.get("phone"):
                        customer = lookup_customer_by_phone(row["phone"])
                        name = (customer or {}).get("name", "")
                    log.append({
                        "type": "call_incoming",
                        "timestamp": row.get("started_at", ""),
                        "payload": {
                            "phone": row.get("phone") or "",
                            "customer": {"name": name, "source": "stored"} if name else None,
                            "outcome": row.get("outcome"),
                            "extension": row.get("extension"),
                        },
                    })
                return jsonify({"success": True, "log": log})
            except Exception as e:
                import logging
                logging.getLogger(__name__).warning(f"[3CX] Log from Supabase failed: {e}")
        return jsonify({"success": True, "log": get_call_log()})

    @app.route("/api/telephony/webhook", methods=["POST"])
    def telephony_webhook():
        """
        Webhook-Empfänger für 3CX-Anrufereignisse.
        In 3CX Admin → Settings → Telephony → Webhook konfigurieren:
        URL: http://<dieser-server>:5000/api/telephony/webhook
        """
        payload = request.get_json(silent=True) or request.form.to_dict() or {}
        handle_webhook_payload(payload)
        return jsonify({"success": True})

    @app.route("/api/telephony/test-call", methods=["POST"])
    def telephony_test_call():
        """Testanruf simulieren (nur für Entwicklung)."""
        body = request.get_json(silent=True) or {}
        phone = body.get("phone", "+49 6051 12345")
        from services.threecx_service import handle_incoming_call
        handle_incoming_call(phone, "Ringing", "test")
        return jsonify({"success": True, "phone": phone})

    @app.route("/api/telephony/stats")
    def telephony_stats():
        """Anruf-Statistiken aus Supabase call_log, gruppiert nach Nebenstelle."""
        if not supabase:
            return jsonify({"success": False, "error": "Supabase nicht konfiguriert"}), 503
        from_date = request.args.get("from", "")
        to_date = request.args.get("to", "")
        extension = request.args.get("extension", "")
        try:
            q = supabase.table("call_log").select("*").order("started_at", desc=True)
            if from_date:
                q = q.gte("started_at", from_date)
            if to_date:
                q = q.lte("started_at", to_date + "T23:59:59")
            if extension:
                q = q.eq("extension", extension)
            res = q.limit(500).execute()
            rows = res.data or []

            from collections import defaultdict
            by_ext: dict = defaultdict(lambda: {"answered": 0, "missed": 0, "voicemail": 0, "outbound": 0, "total": 0})
            for row in rows:
                ext = row.get("extension") or "?"
                direction = row.get("direction") or ""
                outcome = row.get("outcome") or ""
                by_ext[ext]["total"] += 1
                if direction == "outbound":
                    by_ext[ext]["outbound"] += 1
                elif outcome in ("answered", "missed", "voicemail"):
                    by_ext[ext][outcome] += 1

            return jsonify({"success": True, "total": len(rows), "by_extension": dict(by_ext), "rows": rows})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/telephony/debug")
    def telephony_debug():
        """Rohdaten der letzten 30 activecalls-Polls — zeigt was 3CX wirklich zurückgibt."""
        return jsonify({"success": True, "polls": get_raw_poll_log()})

    @app.route("/api/telephony/phonebook", methods=["GET"])
    def phonebook_list():
        if not supabase:
            return jsonify({"success": False, "error": "Supabase nicht konfiguriert"}), 503
        try:
            res = supabase.table("phonebook").select("*").order("name").execute()
            return jsonify({"success": True, "entries": res.data or []})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/telephony/phonebook", methods=["POST"])
    def phonebook_create():
        if not supabase:
            return jsonify({"success": False, "error": "Supabase nicht konfiguriert"}), 503
        body = request.get_json(silent=True) or {}
        name = (body.get("name") or "").strip()
        if not name:
            return jsonify({"success": False, "error": "Name erforderlich"}), 400
        try:
            res = supabase.table("phonebook").insert({
                "name": name,
                "phone": (body.get("phone") or "").strip(),
                "mobile": (body.get("mobile") or "").strip(),
                "phone2": (body.get("phone2") or "").strip(),
            }).execute()
            rebuild_phonebook_index()
            return jsonify({"success": True, "entry": res.data[0] if res.data else {}})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/telephony/phonebook/<entry_id>", methods=["PUT"])
    def phonebook_update(entry_id):
        if not supabase:
            return jsonify({"success": False, "error": "Supabase nicht konfiguriert"}), 503
        body = request.get_json(silent=True) or {}
        try:
            res = supabase.table("phonebook").update({
                "name": (body.get("name") or "").strip(),
                "phone": (body.get("phone") or "").strip(),
                "mobile": (body.get("mobile") or "").strip(),
                "phone2": (body.get("phone2") or "").strip(),
            }).eq("id", entry_id).execute()
            rebuild_phonebook_index()
            return jsonify({"success": True, "entry": res.data[0] if res.data else {}})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route("/api/telephony/phonebook/<entry_id>", methods=["DELETE"])
    def phonebook_delete(entry_id):
        if not supabase:
            return jsonify({"success": False, "error": "Supabase nicht konfiguriert"}), 503
        try:
            supabase.table("phonebook").delete().eq("id", entry_id).execute()
            rebuild_phonebook_index()
            return jsonify({"success": True})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500
