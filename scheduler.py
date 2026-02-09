# scheduler.py
import json
from io import BytesIO
import openpyxl

from drive_store import DriveStore
from utils import utc_now_iso, normalize_phone, json_dumps

def load_config():
    with open("config.json", "r", encoding="utf-8") as f:
        return json.load(f)

def read_xlsx_contacts(xlsx_bytes: bytes):
    """
    Reads the first sheet of an XLSX and returns normalized contacts:
    [{"name":..., "phone":digits, "email":...}, ...]
    """
    wb = openpyxl.load_workbook(BytesIO(xlsx_bytes), data_only=True)
    ws = wb.active
    rows = list(ws.iter_rows(values_only=True))
    if not rows:
        return []

    header = [str(h).strip() if h is not None else "" for h in rows[0]]
    idx = {name.lower(): i for i, name in enumerate(header)}

    def get(row, *keys):
        for key in keys:
            i = idx.get(key)
            if i is not None and i < len(row):
                return row[i]
        return None

    out = []
    for r in rows[1:]:
        name = get(r, "nombre", "name")
        phone = get(r, "número", "numero", "number", "teléfono", "telefono", "phone")
        email = get(r, "email", "correo", "mail")

        phone_norm = normalize_phone(phone)
        if not phone_norm:
            continue

        out.append(
            {
                "name": str(name).strip() if name else None,
                "phone": phone_norm,
                "email": str(email).strip() if email else None,
            }
        )

    # dedupe by phone
    seen = set()
    deduped = []
    for c in out:
        if c["phone"] in seen:
            continue
        seen.add(c["phone"])
        deduped.append(c)
    return deduped

def main():
    cfg = load_config()
    ds = DriveStore(cfg["service_account_json"])
    F = cfg["drive"]["folders"]

    batch_limit = int(cfg.get("runtime", {}).get("scheduler_batch_limit", 10))

    # List XLSX files in inbox folder
    inbox_files = ds.list_files(F["inbox_xlsx"], limit=batch_limit)

    for f in inbox_files:
        drive_file_id = f["id"]
        name = f["name"]

        # Idempotency: if index file exists, skip (already seen)
        index_name = f"{drive_file_id}.json"
        existing = ds.find_by_name(F["index_files"], index_name)
        if existing:
            continue

        # Create a run id for this file
        file_run_id = f"{drive_file_id}__{utc_now_iso().replace(':','-')}"
        idx_obj = {
            "drive_file_id": drive_file_id,
            "name": name,
            "status": "processing",
            "file_run_id": file_run_id,
            "created_at": utc_now_iso(),
        }

        # Create index (processing)
        idx_file_id = ds.upload_json(F["index_files"], index_name, json_dumps(idx_obj))

        try:
            # Download + parse XLSX
            xlsx_bytes = ds.download_bytes(drive_file_id)
            contacts = read_xlsx_contacts(xlsx_bytes)

            # Create a job per contact in queue/pending
            for c in contacts:
                job_name = f"{c['phone']}__{file_run_id}.json"

                # Optional: prevent duplicate job for same phone+run
                if ds.find_by_name(F["queue_pending"], job_name):
                    continue

                job_obj = {
                    "contact_key": c["phone"],
                    "name": c["name"],
                    "email": c["email"],
                    "file_run_id": file_run_id,
                    "attempt": 0,
                    "created_at": utc_now_iso(),
                    "status": "pending",
                }
                ds.upload_json(F["queue_pending"], job_name, json_dumps(job_obj))

            # Move XLSX to archive
            ds.move_file(drive_file_id, F["archive_xlsx"])

            # Mark index as done
            idx_obj["status"] = "done"
            idx_obj["processed_at"] = utc_now_iso()
            ds.update_file_json(idx_file_id, json_dumps(idx_obj))

        except Exception as e:
            # Mark index as error
            idx_obj["status"] = "error"
            idx_obj["error"] = str(e)
            idx_obj["processed_at"] = utc_now_iso()
            ds.update_file_json(idx_file_id, json_dumps(idx_obj))
            # Don't crash the whole scheduler; continue with next file
            continue

if __name__ == "__main__":
    main()
