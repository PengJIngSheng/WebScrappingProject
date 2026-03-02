import os
import time
import io
import logging
from datetime import datetime, timedelta
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload
from pyairtable import Api

logger = logging.getLogger(__name__)

SCOPES = [
    'https://www.googleapis.com/auth/documents',
    'https://www.googleapis.com/auth/drive'
]

# Default field mapping
DEFAULT_FIELD_MAPPING = {
    "{{Applicant Name}}": "Applicant Name",
    "{{IC Number}}": "IC Number",
    "{{Address Line 1}}": "Address Line 1",
    "{{Programme Name}}": "Programme Name",
    "{{student status}}": "student status"
}

CREDENTIALS_MAX_AGE_DAYS = 3


# ================= Credentials Expiry =================

def check_credentials_expiry(script_dir):
    """Delete credentials.json if older than CREDENTIALS_MAX_AGE_DAYS days. Returns True if valid."""
    creds_path = os.path.join(script_dir, 'credentials.json')
    if not os.path.exists(creds_path):
        return False

    mtime = datetime.fromtimestamp(os.path.getmtime(creds_path))
    age = datetime.now() - mtime
    if age > timedelta(days=CREDENTIALS_MAX_AGE_DAYS):
        os.remove(creds_path)
        token_path = os.path.join(script_dir, 'token.json')
        if os.path.exists(token_path):
            os.remove(token_path)
        logger.warning(f"credentials.json expired ({age.days} days old). Deleted. Please re-upload.")
        return False

    return True


def get_credentials_info(script_dir):
    """Return credentials status info."""
    creds_path = os.path.join(script_dir, 'credentials.json')
    if not os.path.exists(creds_path):
        return {"exists": False, "expires_in": None}

    mtime = datetime.fromtimestamp(os.path.getmtime(creds_path))
    age = datetime.now() - mtime
    remaining = timedelta(days=CREDENTIALS_MAX_AGE_DAYS) - age
    if remaining.total_seconds() <= 0:
        return {"exists": False, "expires_in": None}

    hours_left = remaining.total_seconds() / 3600
    return {
        "exists": True,
        "expires_in": f"{hours_left:.1f} hours",
        "uploaded_at": mtime.strftime('%Y-%m-%d %H:%M:%S')
    }


# ================= Google Auth =================

def authenticate_google(script_dir):
    """Handles Google OAuth2.0 authorization."""
    token_path = os.path.join(script_dir, 'token.json')
    creds_path = os.path.join(script_dir, 'credentials.json')

    if not os.path.exists(creds_path):
        raise FileNotFoundError("credentials.json not found. Please upload it first.")

    creds = None
    if os.path.exists(token_path):
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
            creds = flow.run_local_server(port=0)
        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    return creds


# ================= Retry Helper =================

def retry_with_backoff(func, max_retries=3, base_delay=2):
    """Execute a function with exponential backoff retry on HttpError 429/5xx."""
    for attempt in range(max_retries):
        try:
            return func()
        except HttpError as e:
            status = e.resp.status if e.resp else 0
            if status in (429, 500, 502, 503) and attempt < max_retries - 1:
                wait = base_delay * (2 ** attempt)
                logger.warning(f"  Rate limited (HTTP {status}). Retrying in {wait}s... ({attempt + 1}/{max_retries})")
                time.sleep(wait)
            else:
                raise
    return None


# ================= Core Processing =================

def process_records(config, script_dir):
    """
    Main processing function. Reads from Airtable, generates offer letters,
    and uploads PDFs back.

    Args:
        config: dict with keys: airtable_api_token, base_id, table_name, view_name,
                template_doc_id, target_folder_id, attachment_field_name, field_mapping
        script_dir: path to the script directory

    Returns:
        dict with keys: processed, skipped, errors, total
    """
    stats = {"processed": 0, "skipped": 0, "errors": 0, "total": 0}

    # --- Validate credentials ---
    if not check_credentials_expiry(script_dir):
        logger.error("credentials.json is missing or expired. Please upload a new one.")
        return stats

    # --- Google Auth ---
    logger.info("Verifying Google permissions...")
    try:
        creds = authenticate_google(script_dir)
        drive_service = build('drive', 'v3', credentials=creds)
        docs_service = build('docs', 'v1', credentials=creds)
        logger.info("Google verification successful!")
    except Exception as e:
        logger.error(f"Google auth failed: {e}")
        return stats

    # --- Airtable ---
    logger.info("Connecting to Airtable...")
    airtable_api = Api(config["airtable_api_token"])
    table = airtable_api.table(config["base_id"], config["table_name"])

    try:
        records = table.all(view=config.get("view_name"))
        stats["total"] = len(records)
        logger.info(f"Fetched {len(records)} records from Airtable (view: {config.get('view_name', 'default')})")
    except Exception as e:
        logger.error(f"Airtable connection failed: {e}")
        return stats

    today_str = datetime.today().strftime('%d %B %Y')
    attachment_field = config.get("attachment_field_name", "Offer")
    field_mapping = config.get("field_mapping", DEFAULT_FIELD_MAPPING)

    # --- Collect record IDs that need processing ---
    new_record_ids = []
    for record in records:
        fields = record.get('fields', {})
        applicant_name = fields.get('Applicant Name')
        if not applicant_name:
            continue
        existing_attachment = fields.get(attachment_field)
        if existing_attachment:
            stats["skipped"] += 1
        else:
            new_record_ids.append(record['id'])

    logger.info(f"Analysis: {len(new_record_ids)} new records to process, {stats['skipped']} already have attachments")

    if not new_record_ids:
        logger.info("No new records found. Nothing to process this run.")
        return stats

    logger.info("Starting Offer Letter Generation...")

    for i, record_id in enumerate(new_record_ids):
        # --- Re-fetch the record to get the LATEST data ---
        try:
            fresh_record = table.get(record_id)
        except Exception as e:
            logger.warning(f"[{i + 1}/{len(new_record_ids)}] Record {record_id} no longer exists, skipping: {e}")
            continue

        fields = fresh_record.get('fields', {})
        applicant_name = fields.get('Applicant Name', 'Unknown')

        # Double-check: skip if attachment was added since our initial scan
        if fields.get(attachment_field):
            logger.info(f"[{i + 1}/{len(new_record_ids)}] Skipped (processed by another run): {applicant_name}")
            stats["skipped"] += 1
            continue

        doc_title = f"Offer Letter - {applicant_name}"
        logger.info(f"[{i + 1}/{len(new_record_ids)}] Processing: {applicant_name} (ID: {record_id})")

        try:
            # --- A. Copy Template ---
            copy_metadata = {
                'name': doc_title,
                'parents': [config["target_folder_id"]]
            }
            copied_file = retry_with_backoff(
                lambda: drive_service.files().copy(
                    fileId=config["template_doc_id"],
                    body=copy_metadata
                ).execute()
            )
            new_doc_id = copied_file.get('id')

            # --- B. Replace Text ---
            doc_requests = []
            doc_requests.append({
                'replaceAllText': {
                    'containsText': {'text': '{{Date}}', 'matchCase': True},
                    'replaceText': today_str
                }
            })

            for placeholder, airtable_col in field_mapping.items():
                replace_val = str(fields.get(airtable_col, ""))
                doc_requests.append({
                    'replaceAllText': {
                        'containsText': {'text': placeholder, 'matchCase': True},
                        'replaceText': replace_val
                    }
                })

            retry_with_backoff(
                lambda: docs_service.documents().batchUpdate(
                    documentId=new_doc_id,
                    body={'requests': doc_requests}
                ).execute()
            )
            logger.info(f"  -> Doc created & text replaced.")

            # --- C. Export to PDF ---
            logger.info(f"  -> Converting to PDF...")
            pdf_content = retry_with_backoff(
                lambda: drive_service.files().export(
                    fileId=new_doc_id,
                    mimeType='application/pdf'
                ).execute()
            )

            pdf_metadata = {
                'name': f"{doc_title}.pdf",
                'parents': [config["target_folder_id"]]
            }
            media = MediaIoBaseUpload(io.BytesIO(pdf_content), mimetype='application/pdf', resumable=True)

            pdf_file = retry_with_backoff(
                lambda: drive_service.files().create(
                    body=pdf_metadata,
                    media_body=media,
                    fields='id'
                ).execute()
            )
            pdf_id = pdf_file.get('id')
            logger.info(f"  -> PDF uploaded to Drive.")

            # --- D. Clean up Google Docs copy ---
            try:
                retry_with_backoff(
                    lambda: drive_service.files().delete(fileId=new_doc_id).execute()
                )
                logger.info(f"  -> Google Docs copy cleaned up.")
            except Exception as e:
                logger.warning(f"  -> Could not delete Docs copy: {e}")

            # --- E. Attach to Airtable ---
            logger.info(f"  -> Attaching PDF to Airtable...")

            permission = retry_with_backoff(
                lambda: drive_service.permissions().create(
                    fileId=pdf_id,
                    body={'type': 'anyone', 'role': 'reader'}
                ).execute()
            )
            permission_id = permission.get('id')
            direct_download_url = f"https://drive.google.com/uc?export=download&id={pdf_id}"

            table.update(record_id, {
                attachment_field: [
                    {
                        "url": direct_download_url,
                        "filename": f"{doc_title}.pdf"
                    }
                ]
            })

            logger.info(f"  -> Waiting 10s for Airtable to download...")
            time.sleep(10)

            retry_with_backoff(
                lambda: drive_service.permissions().delete(
                    fileId=pdf_id,
                    permissionId=permission_id
                ).execute()
            )

            logger.info(f"  -> ✅ PDF attached & public link revoked.")
            stats["processed"] += 1

        except HttpError as error:
            logger.error(f"  -> Google API Error: {error}")
            stats["errors"] += 1
        except Exception as e:
            logger.error(f"  -> Unexpected Error: {e}")
            stats["errors"] += 1

        time.sleep(1)

    logger.info(f"Run complete! Processed: {stats['processed']}, Skipped: {stats['skipped']}, Errors: {stats['errors']}")
    return stats
