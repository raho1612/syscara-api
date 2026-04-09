"""
API-Routen für Deckungsbeitrag-Analyse.
Modularisierte Version – nutzt shared/kosten_engine.py.
"""

from flask import jsonify, request
from shared.kosten_engine import get_kosten_fahrzeuge, calculate_deckungsbeitrag, _safe_float

def register_kosten_routes(app):

    @app.route("/api/kosten/fahrzeuge", methods=["GET"])
    def api_kosten_fahrzeuge():
        """
        Liefert verkaufte Fahrzeuge inkl. VK/EK-Preise für die DB-Kalkulation.
        """
        try:
            von_monat = (request.args.get("von") or "").strip()[:7]
            bis_monat = (request.args.get("bis") or "").strip()[:7]
            art_filter = (request.args.get("art") or "alle").lower()
            top_n = max(0, int(_safe_float(request.args.get("top_n") or 0)))

            result = get_kosten_fahrzeuge(
                von_monat=von_monat,
                bis_monat=bis_monat,
                art_filter=art_filter,
                top_n=top_n
            )

            return jsonify({"success": True, "count": len(result), "vehicles": result})
        except Exception as exc:
            import traceback
            print(f"[ERROR] api_kosten_fahrzeuge: {exc}", flush=True)
            traceback.print_exc()
            return jsonify({"success": False, "error": f"Interner Fehler: {exc}", "vehicles": []}), 500

    @app.route("/api/kosten/deckungsbeitrag", methods=["POST"])
    def api_kosten_deckungsbeitrag():
        """
        Berechnet den Deckungsbeitrag für ein Fahrzeug anhand der übergebenen Positionen.
        """
        try:
            body = request.get_json(silent=True) or {}
            vehicle_id = body.get("vehicle_id")
            filters = body.get("filters", {})
            settings = body.get("settings", {})
            positionen = body.get("positionen", [])

            res = calculate_deckungsbeitrag(
                vehicle_id=vehicle_id,
                filters=filters,
                settings=settings,
                positionen=positionen
            )

            if "error" in res:
                return jsonify({"success": False, "error": res["error"]}), 404

            return jsonify({"success": True, **res})
        except Exception as exc:
            import traceback
            print(f"[ERROR] api_kosten_deckungsbeitrag: {exc}", flush=True)
            traceback.print_exc()
            return jsonify({"success": False, "error": f"Interner Fehler: {exc}"}), 500
