# analyzer_gemini.py
import os
import json
from typing import Any, Dict, List, Optional

from google import genai
from google.genai import types

from utils import utc_now_iso

# --- Catálogo fijo de razones ---
PRIMARY_REASON_ENUM = [
    "NO_RESPONSE",
    "LOST_INTEREST",
    "TIME_CONSTRAINT",
    "SALARY_MISMATCH",
    "LOCATION_MISMATCH",
    "NOT_QUALIFIED",
    "QUALIFIED_BUT_NOT_INTERESTED",
    "PROCESS_CONFUSION",
    "TECH_ISSUES",
    "DUPLICATE",
    "SPAM",
    "OTHER",
    "UNKNOWN",
]

OUTCOME_ENUM = ["applied", "not_applied", "unknown"]
STAGE_ENUM = ["new", "engaged", "screening", "scheduled", "applied", "hired", "unknown"]
SENTIMENT_ENUM = ["positive", "neutral", "negative", "mixed", "unknown"]
LANG_ENUM = ["es", "en", "other", "unknown"]
EXP_ENUM = ["junior", "mid", "senior", "unknown"]
AVAIL_ENUM = ["immediate", "2_weeks", "1_month", "unknown"]

def _schema() -> Dict[str, Any]:
    # JSON schema compatible con structured outputs
    return {
        "type": "object",
        "properties": {
            "applicant_id": {"type": "string"},
            "contact": {
                "type": "object",
                "properties": {
                    "name": {"type": ["string", "null"]},
                    "phone": {"type": ["string", "null"]},
                    "email": {"type": ["string", "null"]},
                },
                "required": ["name", "phone", "email"],
            },
            "campaign": {
                "type": "object",
                "properties": {
                    "campaign_id": {"type": ["string", "null"]},
                    "source": {"type": "string"},
                    "channel": {"type": ["string", "null"]},
                },
                "required": ["campaign_id", "source", "channel"],
            },
            "funnel": {
                "type": "object",
                "properties": {
                    "outcome": {"type": "string", "enum": OUTCOME_ENUM},
                    "stage_reached": {"type": "string", "enum": STAGE_ENUM},
                    "dropoff_stage": {"type": ["string", "null"], "enum": STAGE_ENUM + [None]},
                },
                "required": ["outcome", "stage_reached", "dropoff_stage"],
            },
            "reasoning": {
                "type": "object",
                "properties": {
                    "primary_reason_code": {"type": "string", "enum": PRIMARY_REASON_ENUM},
                    "secondary_reason_codes": {"type": "array", "items": {"type": "string"}},
                    "reason_text": {"type": "string"},
                },
                "required": ["primary_reason_code", "secondary_reason_codes", "reason_text"],
            },
            "profile": {
                "type": "object",
                "properties": {
                    "skills_summary": {"type": "string"},
                    "skills": {"type": "array", "items": {"type": "string"}},
                    "experience_level": {"type": "string", "enum": EXP_ENUM},
                    "role_interest": {"type": "array", "items": {"type": "string"}},
                    "availability": {"type": "string", "enum": AVAIL_ENUM},
                    "location": {"type": ["string", "null"]},
                },
                "required": ["skills_summary", "skills", "experience_level", "role_interest", "availability", "location"],
            },
            "conversation": {
                "type": "object",
                "properties": {
                    "language": {"type": "string", "enum": LANG_ENUM},
                    "sentiment": {"type": "string", "enum": SENTIMENT_ENUM},
                    "last_message_ts": {"type": ["string", "null"]},
                    "message_count": {"type": "integer", "minimum": 0},
                },
                "required": ["language", "sentiment", "last_message_ts", "message_count"],
            },
            "quality": {
                "type": "object",
                "properties": {
                    "confidence": {"type": "number", "minimum": 0.0, "maximum": 1.0},
                    "evidence_quotes": {"type": "array", "items": {"type": "string"}, "maxItems": 3},
                    "needs_human_review": {"type": "boolean"},
                },
                "required": ["confidence", "evidence_quotes", "needs_human_review"],
            },
            "meta": {
                "type": "object",
                "properties": {
                    "model": {"type": "string"},
                    "analysis_ts": {"type": "string"},
                },
                "required": ["model", "analysis_ts"],
            },
        },
        "required": ["applicant_id","contact","campaign","funnel","reasoning","profile","conversation","quality","meta"],
        "additionalProperties": False,
    }

def _flatten_messages(messages_json: Any) -> str:
    """
    Convierte mensajes en texto plano. Es defensivo porque no sabemos el shape exacto.
    """
    msgs = []
    if isinstance(messages_json, dict):
        maybe = messages_json.get("messages")
        if isinstance(maybe, list):
            msgs = maybe
        elif isinstance(messages_json.get("data"), list):
            msgs = messages_json["data"]
    elif isinstance(messages_json, list):
        msgs = messages_json

    lines: List[str] = []
    for m in msgs:
        if not isinstance(m, dict):
            continue
        who = m.get("from") or m.get("role") or m.get("sender") or "unknown"
        text = m.get("text") or m.get("message") or m.get("content") or ""
        ts = m.get("created_at") or m.get("timestamp") or m.get("date") or ""
        text = str(text).replace("\n", " ").strip()
        if not text:
            continue
        lines.append(f"[{ts}] {who}: {text}")
    return "\n".join(lines)[:120_000]  # corta para evitar prompts gigantes

class GeminiAnalyzer:
    def __init__(self, model: str = "gemini-1.5-flash"):
        api_key = os.getenv("GEMINI_API_KEY")
        if not api_key:
            raise RuntimeError("Falta GEMINI_API_KEY en variables de entorno.")
        self.client = genai.Client(api_key=api_key)
        self.model = model
        self.schema = _schema()

    def analyze(self, job: Dict[str, Any], messages_json: Any) -> Dict[str, Any]:
        convo = _flatten_messages(messages_json)

        # Heurística mínima para message_count/last_ts (ayuda a completar campos)
        msg_count = 0
        last_ts: Optional[str] = None
        if isinstance(messages_json, dict) and isinstance(messages_json.get("messages"), list):
            msg_count = len(messages_json["messages"])
            if msg_count:
                last = messages_json["messages"][-1]
                if isinstance(last, dict):
                    last_ts = last.get("created_at") or last.get("timestamp") or last.get("date")
        elif isinstance(messages_json, list):
            msg_count = len(messages_json)

        system = (
            "Eres un analista de RRHH. Extraes información SOLO de la conversación. "
            "NO inventes. Si no hay evidencia suficiente, usa UNKNOWN y needs_human_review=true. "
            "Devuelve SOLO JSON válido que cumpla el schema."
        )

        user = {
            "contact_key": job.get("contact_key"),
            "name": job.get("name"),
            "email": job.get("email"),
            "file_run_id": job.get("file_run_id"),
            "conversation_text": convo,
            "message_count_hint": msg_count,
            "last_message_ts_hint": last_ts,
            "allowed_primary_reason_codes": PRIMARY_REASON_ENUM,
            "allowed_outcome": OUTCOME_ENUM,
            "allowed_stages": STAGE_ENUM,
        }

        resp = self.client.models.generate_content(
            model=self.model,
            contents=[
                types.Content(role="user", parts=[types.Part.from_text(json.dumps(user, ensure_ascii=False))])
            ],
            config=types.GenerateContentConfig(
                system_instruction=system,
                response_mime_type="application/json",
                response_schema=self.schema,
                temperature=0.2,
            ),
        )

        # Devuelve texto JSON (en modo JSON)
        text = resp.text
        data = json.loads(text)

        # Completa meta si faltara (por seguridad)
        data.setdefault("meta", {})
        data["meta"].setdefault("model", self.model)
        data["meta"].setdefault("analysis_ts", utc_now_iso())

        return data
