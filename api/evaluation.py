import os

from core.config import HAS_CLAUDE, HAS_GEMINI, HAS_OPENAI
from flask import jsonify, request


def register_evaluation_routes(app):

    @app.route('/api/evaluate', methods=['POST'])
    def api_evaluate():
        if not HAS_OPENAI:
            return jsonify({"success": False, "error": "OpenAI nicht installiert."}), 503

        body = request.get_json(silent=True) or {}
        data = str(body.get('data', ''))[:5000]
        instruction = str(body.get('instruction', ''))[:3000] or "Agiere als Fahrzeugexperte."

        import openai
        client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        try:
            res = client.chat.completions.create(
                model="gpt-4o",
                messages=[
                    {"role": "system", "content": instruction},
                    {"role": "user", "content": data}
                ]
            )
            return jsonify({"success": True, "text": res.choices[0].message.content})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500

    @app.route('/api/evaluate-claude', methods=['POST'])
    def api_evaluate_claude():
        if not HAS_CLAUDE:
            return jsonify({"success": False, "error": "Claude nicht installiert."}), 503

        body = request.get_json(silent=True) or {}
        data = str(body.get('data', ''))[:5000]
        instruction = str(body.get('instruction', ''))[:3000] or "Agiere als Fahrzeugexperte."

        req_model = body.get('model', 'sonnet')

        # Reale Modelle mit Fallback-Reihenfolge (neueste zuerst)
        if req_model == 'haiku':
            candidates = [
                "claude-3-5-haiku-latest",
                "claude-3-5-haiku-20241022",
                "claude-3-haiku-20240307"
            ]
        else:
            candidates = [
                "claude-3-7-sonnet-latest",
                "claude-3-7-sonnet-20250219",
                "claude-3-5-sonnet-latest",
                "claude-3-5-sonnet-20241022",
                "claude-3-5-sonnet-20240620",  # Ursprüngliche 3.5 Sonnet
                "claude-3-sonnet-20240229",    # Claude 3.0 Sonnet
                "claude-3-opus-20240229"       # Claude 3.0 Opus (Fallback)
            ]

        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))

        last_error = None
        for model_id in candidates:
            try:
                res = client.messages.create(
                    model=model_id,
                    max_tokens=4000,
                    system=instruction,
                    messages=[{"role": "user", "content": data}]
                )
                return jsonify({
                    "success": True,
                    "text": res.content[0].text,
                    "model": model_id
                })
            except Exception as e:
                last_error = str(e)
                # Fallback bei 404 (nicht gefunden)
                if "not_found" in last_error.lower() or "404" in last_error:
                    continue
                return jsonify({"success": False, "error": last_error}), 500

        return jsonify({
            "success": False,
            "error": f"Alle Claude Modelle fehlgeschlagen: {last_error}"
        }), 500

    @app.route('/api/evaluate-gemini', methods=['POST'])
    def api_evaluate_gemini():
        if not HAS_GEMINI:
            return jsonify({"success": False, "error": "Gemini nicht installiert."}), 503

        body = request.get_json(silent=True) or {}
        data = str(body.get('data', ''))[:5000]
        instruction = str(body.get('instruction', ''))[:3000] or "Agiere als Fahrzeugexperte."

        import google.generativeai as genai
        genai.configure(
            api_key=os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY")
        )
        try:
            model = genai.GenerativeModel(
                "gemini-2.0-flash",
                system_instruction=instruction
            )
            res = model.generate_content(data)
            return jsonify({"success": True, "text": res.text})
        except Exception as e:
            return jsonify({"success": False, "error": str(e)}), 500
