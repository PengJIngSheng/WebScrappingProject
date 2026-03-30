import os
import time
import io
import json
import logging
from datetime import datetime
from dotenv import load_dotenv
from pyairtable import Api
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

# ================= 1. Configuration =================

# Resolve paths relative to this script's directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Load environment variables from .env
load_dotenv(os.path.join(SCRIPT_DIR, '.env'))

# --- Airtable Config (from .env) ---
AIRTABLE_API_TOKEN = os.getenv("AIRTABLE_API_TOKEN")
BASE_ID = os.getenv("BASE_ID")
TABLE_NAME = os.getenv("TABLE_NAME")
VIEW_NAME = os.getenv("VIEW_NAME")

# --- Google Config (from .env) ---
TEMPLATE_DOC_ID = os.getenv("TEMPLATE_DOC_ID")
TARGET_FOLDER_ID = os.getenv("TARGET_FOLDER_ID")
# The exact name of your Attachment column in Airtable
ATTACHMENT_FIELD_NAME = "Offer"

# Google API Scopes (Read/Write for Drive and Docs)
SCOPES = [
    'https://www.googleapis.com/auth/documents',
    'https://www.googleapis.com/auth/drive'
]
OAUTH_LOCAL_SERVER_PORT = 8080

# ================= 2. Field Mapping =================
FIELD_MAPPING = {
    "{{Applicant Name}}": "Full Name (as per Identification Card)",
    "{{IC Number}}": "NRIC",
    "{{Address Line 1}}": "Address Line 1",
    "{{student status}}": "Student Status"
}

# ================= 3. Logging Setup =================

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler(os.path.join(SCRIPT_DIR, "offerletter.log"), encoding="utf-8")
    ]
)
logger = logging.getLogger(__name__)

# ================= 4. Google API Auth =================


def authenticate_google():
    """Handles Google OAuth2.0 authorization."""
    token_path = os.path.join(SCRIPT_DIR, 'token.json')
    creds_path = resolve_credentials_path(SCRIPT_DIR)

    creds = None
    if os.path.exists(token_path):
        try:
            creds = Credentials.from_authorized_user_file(token_path, SCOPES)
        except Exception as exc:
            logger.warning(f"Existing token.json is unreadable. Deleting it and re-authorizing. Error: {exc}")
            os.remove(token_path)

    if creds and creds.expired and creds.refresh_token:
        try:
            creds.refresh(Request())
        except RefreshError as exc:
            logger.warning(f"Existing token.json could not be refreshed. Re-authorizing with credentials.json. Error: {exc}")
            os.remove(token_path)
            creds = None

    if not creds or not creds.valid:
        flow = InstalledAppFlow.from_client_secrets_file(creds_path, SCOPES)
        creds = flow.run_local_server(port=OAUTH_LOCAL_SERVER_PORT)
        with open(token_path, 'w') as token:
            token.write(creds.to_json())

    return creds


def resolve_credentials_path(script_dir):
    """Return a usable credentials.json path and fail with actionable errors otherwise."""
    creds_path = os.path.join(script_dir, 'credentials.json')
    typo_path = os.path.join(script_dir, 'credentiails.json')

    if os.path.exists(creds_path):
        validate_credentials_file(creds_path)
        return creds_path

    if os.path.exists(typo_path):
        raise FileNotFoundError(
            "Found 'credentiails.json', but the bot expects 'credentials.json'. "
            "Rename the file or upload it again through the web app."
        )

    raise FileNotFoundError("credentials.json not found. Upload or create the correct Google OAuth client file first.")


def validate_credentials_file(creds_path):
    """Validate the Google OAuth client secrets file before using it."""
    try:
        with open(creds_path, 'r', encoding='utf-8') as f:
            payload = json.load(f)
    except json.JSONDecodeError as exc:
        raise ValueError(f"{os.path.basename(creds_path)} is not valid JSON: {exc}") from exc

    client_config = payload.get('installed') or payload.get('web')
    if not isinstance(client_config, dict):
        raise ValueError("credentials.json must contain a top-level 'installed' or 'web' object.")

    required_keys = ['client_id', 'client_secret', 'auth_uri', 'token_uri']
    missing_keys = [key for key in required_keys if not client_config.get(key)]
    if missing_keys:
        raise ValueError(f"credentials.json is missing required fields: {', '.join(missing_keys)}")

    if 'web' in payload:
        redirect_uris = client_config.get('redirect_uris') or []
        expected_redirect_uri = f"http://localhost:{OAUTH_LOCAL_SERVER_PORT}/"
        if expected_redirect_uri not in redirect_uris:
            raise ValueError(
                "This credentials.json is a Web OAuth client, but its redirect_uris do not include "
                f"'{expected_redirect_uri}'. Add it in Google Cloud Console or use a Desktop app OAuth client instead."
            )


# ================= 5. Retry Helper =================


def retry_with_backoff(func, max_retries=3, base_delay=2):
    """Execute a function with exponential backoff retry on HttpError 429/5xx."""
    for attempt in range(max_retries):
        try:
            return func()
        except HttpError as e:
            status = e.resp.status if e.resp else 0
            if status in (429, 500, 502, 503) and attempt < max_retries - 1:
                wait = base_delay * (2 ** attempt)
                logger.warning(f"  Rate limited / server error (HTTP {status}). "
                               f"Retrying in {wait}s... (attempt {attempt + 1}/{max_retries})")
                time.sleep(wait)
            else:
                raise
    return None  # Should not reach here


# ================= 6. Core Logic =================


def main():
    # --- Validate config ---
    required_vars = {
        "AIRTABLE_API_TOKEN": AIRTABLE_API_TOKEN,
        "BASE_ID": BASE_ID,
        "TABLE_NAME": TABLE_NAME,
        "VIEW_NAME": VIEW_NAME,
        "TEMPLATE_DOC_ID": TEMPLATE_DOC_ID,
        "TARGET_FOLDER_ID": TARGET_FOLDER_ID,
    }
    missing = [k for k, v in required_vars.items() if not v]
    if missing:
        logger.error(f"Missing environment variables: {', '.join(missing)}. Check your .env file.")
        return

    # --- Google Auth ---
    logger.info("1. Verifying Google permissions...")
    try:
        creds = authenticate_google()
        drive_service = build('drive', 'v3', credentials=creds)
        docs_service = build('docs', 'v1', credentials=creds)
        logger.info("   -> Google verification successful!")
    except Exception as e:
        logger.error(f"   -> Google auth failed. Check credentials.json. Error: {e}")
        return

    # --- Airtable ---
    logger.info("2. Connecting to Airtable...")
    api = Api(AIRTABLE_API_TOKEN)
    table = api.table(BASE_ID, TABLE_NAME)

    try:
        records = table.all(view=VIEW_NAME)
        logger.info(f"   -> Found {len(records)} records.")
    except Exception as e:
        logger.error(f"   -> Airtable connection failed: {e}")
        return

    today_str = datetime.today().strftime('%d %B %Y')
    applicant_name_field = FIELD_MAPPING.get("{{Applicant Name}}", "Applicant Name")

    logger.info("3. Starting Offer Letter Generation...")

    processed = 0
    skipped = 0

    for i, record in enumerate(records):
        fields = record.get('fields', {})

        # --- Skip already-processed records ---
        existing_attachment = fields.get(ATTACHMENT_FIELD_NAME)
        if existing_attachment:
            applicant_name = fields.get(applicant_name_field) or record.get('id', 'Unknown')
            logger.info(f"   [{i + 1}/{len(records)}] Skipped (already has attachment): {applicant_name}")
            skipped += 1
            continue

        applicant_name = fields.get(applicant_name_field)
        if applicant_name:
            applicant_name = str(applicant_name).strip()
        if not applicant_name:
            applicant_name = f"Record {record.get('id', i + 1)}"
            logger.warning(
                f"   [{i + 1}/{len(records)}] Applicant name field '{applicant_name_field}' is empty. "
                f"Using fallback title: {applicant_name}"
            )

        doc_title = f"Offer Letter - {applicant_name}"
        logger.info(f"   [{i + 1}/{len(records)}] Processing: {applicant_name} ...")

        try:
            # --- A. Copy Template (Generate Doc) ---
            copy_metadata = {
                'name': doc_title,
                'parents': [TARGET_FOLDER_ID]
            }
            copied_file = retry_with_backoff(
                lambda: drive_service.files().copy(
                    fileId=TEMPLATE_DOC_ID,
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

            for placeholder, airtable_col in FIELD_MAPPING.items():
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

            logger.info(f"      -> Doc created & text replaced.")

            # --- C. Export to PDF ---
            logger.info(f"      -> Converting to PDF...")

            pdf_content = retry_with_backoff(
                lambda: drive_service.files().export(
                    fileId=new_doc_id,
                    mimeType='application/pdf'
                ).execute()
            )

            pdf_metadata = {
                'name': f"{doc_title}.pdf",
                'parents': [TARGET_FOLDER_ID]
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
            logger.info(f"      -> PDF uploaded to Drive.")

            # --- D. Clean up Google Docs copy (keep only PDF) ---
            try:
                retry_with_backoff(
                    lambda: drive_service.files().delete(fileId=new_doc_id).execute()
                )
                logger.info(f"      -> Google Docs copy cleaned up.")
            except Exception as e:
                logger.warning(f"      -> Could not delete Docs copy: {e}")

            # --- E. Attach to Airtable (Securely) ---
            logger.info(f"      -> Attaching PDF to Airtable record...")

            # 1. Temporarily make PDF public so Airtable can download it
            permission = retry_with_backoff(
                lambda: drive_service.permissions().create(
                    fileId=pdf_id,
                    body={'type': 'anyone', 'role': 'reader'}
                ).execute()
            )

            permission_id = permission.get('id')
            direct_download_url = f"https://drive.google.com/uc?export=download&id={pdf_id}"

            # 2. Update Airtable Record
            table.update(record['id'], {
                ATTACHMENT_FIELD_NAME: [
                    {
                        "url": direct_download_url,
                        "filename": f"{doc_title}.pdf"
                    }
                ]
            })

            # 3. Wait for Airtable to download, then revoke public access
            logger.info(f"      -> Waiting 10s for Airtable to download...")
            time.sleep(10)

            retry_with_backoff(
                lambda: drive_service.permissions().delete(
                    fileId=pdf_id,
                    permissionId=permission_id
                ).execute()
            )

            logger.info(f"      -> ✅ PDF attached & public link revoked.")
            processed += 1

        except HttpError as error:
            logger.error(f"      -> Google API Error: {error}")
        except Exception as e:
            logger.error(f"      -> Unexpected Error: {e}")

        time.sleep(1)

    logger.info(f"\n🎉 All tasks completed! Processed: {processed}, Skipped: {skipped}")


if __name__ == '__main__':
    main()
