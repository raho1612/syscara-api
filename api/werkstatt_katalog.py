"""
Werkstatt-Leistungskatalog – geräteübergreifende Persistenz in Supabase.

Tabelle (einmalig anlegen):
  CREATE TABLE IF NOT EXISTS werkstatt_katalog (
    id         TEXT        PRIMARY KEY,
    name       TEXT        NOT NULL DEFAULT '',
    selbstkosten NUMERIC   NOT NULL DEFAULT 0,
    brutto     NUMERIC     NOT NULL DEFAULT 0,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
  );
"""

from datetime import datetime, timezone

from core.database import supabase
from flask import jsonify, request


def register_werkstatt_katalog_routes(app):

    @app.route("/api/werkstatt_katalog", methods=["GET"])
    def get_werkstatt_katalog():
        if not supabase:
            return jsonify({"eintraege": []}), 200
        try:
            res = supabase.table("werkstatt_katalog").select("*").order("name").execute()
            eintraege = res.data or []
            return jsonify({"eintraege": eintraege}), 200
        except Exception as e:
            print(f"[WARN] werkstatt_katalog GET Fehler: {e}", flush=True)
            return jsonify({"eintraege": []}), 200

    @app.route("/api/werkstatt_katalog", methods=["POST"])
    def upsert_werkstatt_katalog():
        """Eintrag anlegen oder aktualisieren (upsert über id)."""
        if not supabase:
            return jsonify({"ok": False, "error": "Supabase nicht konfiguriert"}), 503
        try:
            body = request.get_json(force=True) or {}
            entry_id = str(body.get("id") or "").strip()
            if not entry_id:
                return jsonify({"ok": False, "error": "id fehlt"}), 400
            supabase.table("werkstatt_katalog").upsert(
                {
                    "id": entry_id,
                    "name": str(body.get("name") or ""),
                    "selbstkosten": float(body.get("selbstkosten") or 0),
                    "brutto": float(body.get("brutto") or 0),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                },
                on_conflict="id",
            ).execute()
            return jsonify({"ok": True}), 200
        except Exception as e:
            print(f"[ERROR] werkstatt_katalog POST Fehler: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500

    @app.route("/api/werkstatt_katalog/<entry_id>", methods=["DELETE"])
    def delete_werkstatt_katalog(entry_id):
        if not supabase:
            return jsonify({"ok": False, "error": "Supabase nicht konfiguriert"}), 503
        try:
            supabase.table("werkstatt_katalog").delete().eq("id", entry_id).execute()
            return jsonify({"ok": True}), 200
        except Exception as e:
            print(f"[ERROR] werkstatt_katalog DELETE Fehler: {e}", flush=True)
            return jsonify({"ok": False, "error": str(e)}), 500
