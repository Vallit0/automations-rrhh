# Proyecto RRHH Pipeline (Drive as DB)

Objetivo:
- Scheduler: Drive inbox_xlsx -> queue/pending jobs (JSON) -> archive_xlsx
- Worker: claim job moviéndolo a queue/processing -> MaxHelper -> guardar bronze/silver en Drive -> upsert Google Sheets -> mover a done/error
Restricciones:
- NO usar SQLite, NO usar n8n.
- Drive es "DB": estados por carpeta (pending/processing/done/error).
- Rate limit MaxHelper: <= 100 requests/min (usar token bucket).
- Salida IA: JSON estricto con campos outcome, primary_reason_code, reason_text, skills_summary, confidence, needs_human_review, evidence_quotes (máx 2).
Estilo de cambios:
- Código claro, con logging, retries con backoff, y idempotencia por drive_file_id.
- Proveer scripts ejecutables: scheduler.py y worker.py + requirements.txt
