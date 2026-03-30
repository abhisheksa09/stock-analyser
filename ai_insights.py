import os
import json
import requests

def get_ai_setup_insight(setup: dict, macro_ctx: dict | None = None) -> dict | None:
    api_key = os.environ.get("OPENAI_API_KEY", "").strip()
    model   = os.environ.get("OPENAI_MODEL", "").strip() or "gpt-4.1-mini"

    if not api_key:
        return None

    schema = {
        "name": "trade_setup_insight",
        "schema": {
            "type": "object",
            "additionalProperties": False,
            "properties": {
                "quality": {
                    "type": "string",
                    "enum": ["high", "medium", "low"]
                },
                "trade_style": {
                    "type": "string",
                    "enum": ["breakout", "mean_reversion", "unclear"]
                },
                "supports": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "risks": {
                    "type": "array",
                    "items": {"type": "string"}
                },
                "confidence_adjustment": {
                    "type": "integer",
                    "minimum": -5,
                    "maximum": 5
                },
                "summary": {"type": "string"}
            },
            "required": ["quality", "trade_style", "supports", "risks", "confidence_adjustment", "summary"]
        }
    }

    payload = {
        "model": model,
        "messages": [
            {
                "role": "system",
                "content": (
                    "You are a conservative intraday trade quality reviewer. "
                    "Use only the supplied structured setup and macro context. "
                    "Do not invent prices or facts. Return strict JSON only."
                )
            },
            {
                "role": "user",
                "content": json.dumps({
                    "setup": setup,
                    "macro_ctx": macro_ctx or {}
                })
            }
        ],
        "response_format": {
            "type": "json_schema",
            "json_schema": schema
        },
        "temperature": 0.2
    }

    try:
        r = requests.post(
            "https://api.openai.com/v1/chat/completions",
            headers={
                "Authorization": f"Bearer {api_key}",
                "Content-Type": "application/json"
            },
            json=payload,
            timeout=25
        )
        r.raise_for_status()
        raw = r.json()["choices"][0]["message"]["content"]
        return json.loads(raw)
    except Exception:
        return None