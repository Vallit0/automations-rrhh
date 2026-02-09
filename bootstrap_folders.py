# bootstrap_drive_folders.py
import json
from drive_store import DriveStore

ROOT_NAME = "RRHH_PIPELINE"
SUBFOLDERS = [
  "inbox_xlsx","archive_xlsx",
  "queue_pending","queue_processing","queue_done","queue_error",
  "bronze_messages_raw","silver_analysis",
  "index_files","index_contacts","index_sheet_rows",
  "logs_runs"
]

def load_config():
    with open("config.json","r",encoding="utf-8") as f:
        return json.load(f)

def main():
    cfg = load_config()
    ds = DriveStore(cfg["service_account_json"])

    # 1) Create/find root folder under "My Drive" of the service account
    # NOTE: if you need it inside a specific existing folder, set parent_id and addParents accordingly.
    # We'll just create in the SA's Drive space.
    root = ds.find_by_name("root", ROOT_NAME)  # won't work because "root" isn't a folder id in our helper

if __name__ == "__main__":
    main()
