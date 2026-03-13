import os
from flask import jsonify, request
from core.config import HAS_OPENAI, HAS_CLAUDE, HAS_GEMINI

def register_evaluation_routes(app):

    @app.route('/api/evaluate', methods=['POST'])
    def api_evaluate():
        if not HAS_OPENAI: return jsonify({"success": False, "error": "OpenAI nicht installiert."}), 503
        body = request.get_json(silent=True) or {}
        data = str(body.get('data', ''))[:5000]
        instruction = str(body.get('instruction', ''))[:3000] or "Agiere als Fahrzeugexperte."
        
        import openai
        client = openai.OpenAI(api_key=os.getenv("OPENAI_API_KEY"))
        try:
            res = client.chat.completions.create(model="gpt-4o", messages=[{"role": "system", "content": instruction}, {"role": "user", "content": data}])
            return jsonify({"success": True, "text": res.choices[0].message.content})
        except Exception as e: return jsonify({"success": False, "error": str(e)}), 500

    @app.route('/api/evaluate-claude', methods=['POST'])
    def api_evaluate_claude():
        if not HAS_CLAUDE: return jsonify({"success": False, "error": "Claude nicht installiert."}), 503
        body = request.get_json(silent=True) or {}
        data = str(body.get('data', ''))[:5000]
        instruction = str(body.get('instruction', ''))[:3000] or "Agiere als Fahrzeugexperte."
        
        import anthropic
        client = anthropic.Anthropic(api_key=os.getenv("ANTHROPIC_API_KEY"))
        try:
            res = client.messages.create(model="claude-3-7-sonnet-20250219", max_tokens=4000, system=instruction, messages=[{"role": "user", "content": data}])
            return jsonify({"success": True, "text": res.content[0].text})
        except Exception as e: return jsonify({"success": False, "error": str(e)}), 500

    @app.route('/api/evaluate-gemini', methods=['POST'])
    def api_evaluate_gemini():
        if not HAS_GEMINI: return jsonify({"success": False, "error": "Gemini nicht installiert."}), 503
        body = request.get_json(silent=True) or {}
        data = str(body.get('data', ''))[:5000]
        instruction = str(body.get('instruction', ''))[:3000] or "Agiere als Fahrzeugexperte."
        
        import google.generativeai as genai
        genai.configure(api_key=os.getenv("GEMINI_API_KEY") or os.getenv("GOOGLE_API_KEY"))
        try:
            model = genai.GenerativeModel("gemini-2.0-flash", system_instruction=instruction)
            res = model.generate_content(data)
            return jsonify({"success": True, "text": res.text})
        except Exception as e: return jsonify({"success": False, "error": str(e)}), 500
