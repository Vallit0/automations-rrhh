import json
import time
from drive_store import DriveStore
from maxhelper_client import MaxHelperClient, TokenBucket
from sheet_sink import SheetSink, APPLICANTS_COLUMNS
from utils import utc_now_iso, json_dumps, json_loads
from analyzer_gemini import GeminiAnalyzer


def load_config():
    with open("config.json","r",encoding="utf-8") as f:
        return json.load(f)

def get_contact_cache(ds: DriveStore, folder_id: str, contact_key: str):
    name = f"{contact_key}.json"
    f = ds.find_by_name(folder_id, name)
    if not f:
        return None
    data = ds.download_bytes(f["id"]).decode("utf-8")
    return json_loads(data)

def set_contact_cache(ds: DriveStore, folder_id: str, contact_key: str, obj):
    name = f"{contact_key}.json"
    existing = ds.find_by_name(folder_id, name)
    if existing:
        ds.update_file_json(existing["id"], json_dumps(obj))
    else:
        ds.upload_json(folder_id, name, json_dumps(obj))

def get_sheet_row_index(ds: DriveStore, folder_id: str, contact_key: str):
    name = f"{contact_key}.json"
    f = ds.find_by_name(folder_id, name)
    if not f:
        return None
    data = ds.download_bytes(f["id"]).decode("utf-8")
    return json_loads(data)

def set_sheet_row_index(ds: DriveStore, folder_id: str, contact_key: str, row: int):
    obj = {"contact_key": contact_key, "row": row, "updated_at": utc_now_iso()}
    name = f"{contact_key}.json"
    existing = ds.find_by_name(folder_id, name)
    if existing:
        ds.update_file_json(existing["id"], json_dumps(obj))
    else:
        ds.upload_json(folder_id, name, json_dumps(obj))

def flatten_analysis_to_row(analysis: dict) -> list:
    # mapea el JSON estándar a columnas de sheet
    contact = analysis.get("contact", {})
    funnel = analysis.get("funnel", {})
    reasoning = analysis.get("reasoning", {})
    profile = analysis.get("profile", {})
    conv = analysis.get("conversation", {})
    quality = analysis.get("quality", {})
    meta = analysis.get("meta", {})

    evidence = quality.get("evidence_quotes", []) or []
    ev1 = evidence[0] if len(evidence) > 0 else ""
    ev2 = evidence[1] if len(evidence) > 1 else ""

    row = {
        "applicant_id": analysis.get("applicant_id",""),
        "name": contact.get("name",""),
        "phone": contact.get("phone",""),
        "email": contact.get("email",""),
        "outcome": funnel.get("outcome","unknown"),
        "stage_reached": funnel.get("stage_reached","unknown"),
        "dropoff_stage": funnel.get("dropoff_stage",""),
        "primary_reason_code": reasoning.get("primary_reason_code","UNKNOWN"),
        "secondary_reason_codes": ",".join(reasoning.get("secondary_reason_codes",[]) or []),
        "reason_text": reasoning.get("reason_text",""),
        "skills_summary": profile.get("skills_summary",""),
        "skills": ",".join(profile.get("skills",[]) or []),
        "experience_level": profile.get("experience_level","unknown"),
        "role_interest": ",".join(profile.get("role_interest",[]) or []),
        "availability": profile.get("availability",""),
        "location": profile.get("location",""),
        "sentiment": conv.get("sentiment","unknown"),
        "message_count": conv.get("message_count",0),
        "last_message_ts": conv.get("last_message_ts",""),
        "confidence": quality.get("confidence",0.0),
        "needs_human_review": quality.get("needs_human_review",True),
        "evidence_quote_1": ev1,
        "evidence_quote_2": ev2,
        "analysis_ts": meta.get("analysis_ts", utc_now_iso())
    }
    return [row.get(col,"") for col in APPLICANTS_COLUMNS]

def make_analysis_mvp(job: dict, messages_raw: dict) -> dict:
    # MVP sin LLM: reglas simples para probar pipeline
    # Luego lo reemplazamos por el análisis IA real (JSON estricto).
    msgs = messages_raw if isinstance(messages_raw, list) else messages_raw.get("messages") or []
    message_count = len(msgs)
    last_ts = ""
    if message_count > 0:
        # si hay timestamps en el payload, ajustamos luego.
        last_ts = utc_now_iso()

    outcome = "unknown"
    primary = "UNKNOWN"
    reason_text = "Sin evidencia suficiente para clasificar automáticamente."

    if message_count == 0:
        outcome = "not_applied"
        primary = "NO_RESPONSE"
        reason_text = "No hay mensajes registrados para este contacto."

    return {
        "applicant_id": job["contact_key"],
        "contact": {
            "name": job.get("name"),
            "phone": job.get("contact_key"),
            "email": job.get("email")
        },
        "campaign": {"campaign_id": None, "source": "maxhelper", "channel": None},
        "funnel": {
            "outcome": outcome,
            "stage_reached": "unknown",
            "dropoff_stage": None
        },
        "reasoning": {
            "primary_reason_code": primary,
            "secondary_reason_codes": [],
            "reason_text": reason_text
        },
        "profile": {
            "skills_summary": "",
            "skills": [],
            "experience_level": "unknown",
            "role_interest": [],
            "availability": None,
            "location": None
        },
        "conversation": {
            "language": "unknown",
            "sentiment": "unknown",
            "last_message_ts": last_ts,
            "message_count": message_count
        },
        "quality": {
            "confidence": 0.2 if primary == "UNKNOWN" else 0.6,
            "evidence_quotes": [],
            "needs_human_review": True
        },
        "meta": {"model": "mvp-rules", "analysis_ts": utc_now_iso()}
    }

def claim_one_job(ds: DriveStore, pending_folder: str, processing_folder: str, limit: int):
    jobs = ds.list_files(pending_folder, limit=limit)
    for f in jobs:
        try:
            # move = claim lock
            ds.move_file(f["id"], processing_folder)
            return f  # claimed
        except Exception:
            continue
    return None

def main():
    cfg = load_config()
    F = cfg["drive"]["folders"]
    ds = DriveStore(cfg["service_account_json"])

    # Sheets client
    sink = SheetSink(cfg["service_account_json"])
    sink.ensure_header(cfg["sheets"]["spreadsheet_id"], cfg["sheets"]["sheet_applicants"])

    # MaxHelper client + rate limit
    bucket = TokenBucket(rate_per_sec=100/60, capacity=100)
    mh = MaxHelperClient(cfg["maxhelper"]["base_url"], cfg["maxhelper"]["api_key"], bucket)
    analyzer = GeminiAnalyzer(model=cfg.get("openai", {}).get("model", "gemini-1.5-flash"))
    # (si preferís, crea un bloque cfg["gemini"]["model"])


    max_attempts = cfg["runtime"]["max_attempts"]

    while True:
        claimed = claim_one_job(ds, F["queue_pending"], F["queue_processing"], cfg["runtime"]["worker_claim_limit"])
        if not claimed:
            time.sleep(2.0)
            continue

        job_file_id = claimed["id"]
        job_name = claimed["name"]

        # load job json
        job = json_loads(ds.download_bytes(job_file_id).decode("utf-8"))
        contact_key = job["contact_key"]

        try:
            # attempt
            job["attempt"] = int(job.get("attempt", 0)) + 1
            job["status"] = "processing"
            job["updated_at"] = utc_now_iso()
            ds.update_file_json(job_file_id, json_dumps(job))

            # 1) contact_id cache
            cache = get_contact_cache(ds, F["index_contacts"], contact_key)
            contact_id = cache.get("maxhelper_contact_id") if cache else None

            if not contact_id:
                c = mh.contact_by_number(contact_key)
                contact_id = str(c.get("id") or c.get("contact", {}).get("id") or "")
                if contact_id:
                    set_contact_cache(ds, F["index_contacts"], contact_key, {
                        "contact_key": contact_key,
                        "maxhelper_contact_id": contact_id,
                        "updated_at": utc_now_iso()
                    })

            # 2) messages
            messages_raw = []
            if contact_id:
                messages_raw = mh.messages(contact_id)

            # write Bronze
            bronze_name = f"{contact_key}__{job['file_run_id']}.json"
            ds.upload_json(F["bronze_messages_raw"], bronze_name, json_dumps({
                "contact_key": contact_key,
                "maxhelper_contact_id": contact_id,
                "fetched_at": utc_now_iso(),
                "messages_raw": messages_raw
            }))

            # 3) analysis (Gemini)
            analysis = analyzer.analyze(job, messages_raw)

            # write Silver
            silver_name = f"{contact_key}__{job['file_run_id']}.json"
            ds.upload_json(F["silver_analysis"], silver_name, json_dumps(analysis))

            # 4) upsert to Sheets (by row index cached in Drive)
            row_values = flatten_analysis_to_row(analysis)

            idx = get_sheet_row_index(ds, F["index_sheet_rows"], contact_key)
            if idx and idx.get("row"):
                sink.update_row(cfg["sheets"]["spreadsheet_id"], cfg["sheets"]["sheet_applicants"], int(idx["row"]), row_values)
            else:
                row_num = sink.append_row(cfg["sheets"]["spreadsheet_id"], cfg["sheets"]["sheet_applicants"], row_values)
                if row_num > 0:
                    set_sheet_row_index(ds, F["index_sheet_rows"], contact_key, row_num)

            # done
            job["status"] = "done"
            job["done_at"] = utc_now_iso()
            ds.update_file_json(job_file_id, json_dumps(job))
            ds.move_file(job_file_id, F["queue_done"])

        except Exception as e:
            job["status"] = "error"
            job["last_error"] = str(e)
            job["updated_at"] = utc_now_iso()
            try:
                ds.update_file_json(job_file_id, json_dumps(job))
            except Exception:
                pass

            if job.get("attempt", 1) >= max_attempts:
                ds.move_file(job_file_id, F["queue_error"])
            else:
                # requeue
                ds.move_file(job_file_id, F["queue_pending"])

if __name__ == "__main__":
    main()
