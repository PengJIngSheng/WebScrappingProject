import os
import time
import io
from datetime import datetime
from pyairtable import Api
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from googleapiclient.http import MediaIoBaseUpload

# ================= 1. 基础配置区域 =================

# --- Airtable 配置 ---
AIRTABLE_API_TOKEN = "pat27ri02lJGXMjU4.731acf6dc2fc706b5533e0228261c06c5e90a6b4b8d44489c77479fed0774571"
BASE_ID = "app3TUhKzusEAK583"
TABLE_NAME = "JAVA - Student"
VIEW_NAME = "SRecord1"

# --- Google 配置 ---
TEMPLATE_DOC_ID = "1WQy6JLz_0FujHvTFTu_8DhSuJ40XqpcwWnhSbXkPgLk"
TARGET_FOLDER_ID = "1Ch3ePaZx6hRqOe6JlS81BiiljqcUi7dr"

# Google API 权限范围 (读写 Drive 和 Docs)
SCOPES = [
    'https://www.googleapis.com/auth/documents',
    'https://www.googleapis.com/auth/drive'
]

# ================= 2. 字段映射配置 =================
FIELD_MAPPING = {
    "{{Applicant Name}}": "Applicant Name",
    "{{IC Number}}": "IC Number",
    "{{Address Line 1}}": "Address Line 1",
    "{{Programme Name}}": "Programme Name",
    "{{student status}}": "student status"
}


# ================= 3. Google API 授权 =================

def authenticate_google():
    """处理 Google OAuth2.0 授权，生成或加载 token.json"""
    creds = None
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        with open('token.json', 'w') as token:
            token.write(creds.to_json())

    return creds


# ================= 4. 核心逻辑 =================

def main():
    print("1. 正在验证 Google 权限...")
    try:
        creds = authenticate_google()
        drive_service = build('drive', 'v3', credentials=creds)
        docs_service = build('docs', 'v1', credentials=creds)
        print(" -> Google 验证成功！")
    except Exception as e:
        print(f" -> Google 验证失败，请检查 credentials.json 是否存在且正确。错误: {e}")
        return

    print(f"\n2. 正在连接 Airtable 获取数据...")
    api = Api(AIRTABLE_API_TOKEN)
    table = api.table(BASE_ID, TABLE_NAME)

    try:
        records = table.all(view=VIEW_NAME)
        print(f" -> 成功获取 {len(records)} 条记录。")
    except Exception as e:
        print(f" -> Airtable 连接失败: {e}")
        return

    today_str = datetime.today().strftime('%d %B %Y')

    print("\n3. 开始生成 Offer Letters (Doc & PDF)...")

    for i, record in enumerate(records):
        fields = record.get('fields', {})
        applicant_name = fields.get('Applicant Name')

        if not applicant_name:
            continue

        doc_title = f"Offer Letter - {applicant_name}"
        print(f"\n[{i + 1}/{len(records)}] 正在处理: {applicant_name} ...")

        try:
            # --- A. 复制模板文件 (生成 Doc) ---
            copy_metadata = {
                'name': doc_title,
                'parents': [TARGET_FOLDER_ID]
            }
            copied_file = drive_service.files().copy(
                fileId=TEMPLATE_DOC_ID,
                body=copy_metadata
            ).execute()

            new_doc_id = copied_file.get('id')

            # --- B. 准备替换文本的请求 ---
            requests = []
            requests.append({
                'replaceAllText': {
                    'containsText': {'text': '{{Date}}', 'matchCase': True},
                    'replaceText': today_str
                }
            })

            for placeholder, airtable_col in FIELD_MAPPING.items():
                replace_val = str(fields.get(airtable_col, ""))
                requests.append({
                    'replaceAllText': {
                        'containsText': {'text': placeholder, 'matchCase': True},
                        'replaceText': replace_val
                    }
                })

            # --- C. 执行替换 (完成 Doc 修改) ---
            docs_service.documents().batchUpdate(
                documentId=new_doc_id,
                body={'requests': requests}
            ).execute()

            print(f"    -> Doc 生成成功！链接: https://docs.google.com/document/d/{new_doc_id}/edit")

            # ================= 新增：生成并上传 PDF =================
            print(f"    -> 正在转换并上传 PDF 版本...")

            # 1. 将刚修改好的 Doc 导出为 PDF 字节流
            pdf_content = drive_service.files().export(
                fileId=new_doc_id,
                mimeType='application/pdf'
            ).execute()

            # 2. 准备上传 PDF 的元数据
            pdf_metadata = {
                'name': f"{doc_title}.pdf",  # 加上 .pdf 后缀
                'parents': [TARGET_FOLDER_ID]  # 传回同一个文件夹
            }

            # 3. 将内存中的 PDF 字节流转化为可上传的媒体对象
            media = MediaIoBaseUpload(io.BytesIO(pdf_content), mimetype='application/pdf', resumable=True)

            # 4. 执行上传
            pdf_file = drive_service.files().create(
                body=pdf_metadata,
                media_body=media,
                fields='id'
            ).execute()

            pdf_id = pdf_file.get('id')
            print(f"    -> PDF 生成成功！链接: https://drive.google.com/file/d/{pdf_id}/view")
            # ==========================================================

            # [进阶推荐] 将 Doc 和 PDF 的链接同时写回 Airtable (如果你在Airtable里建了这两列)
            # table.update(record['id'], {
            #     "Doc Link": f"https://docs.google.com/document/d/{new_doc_id}/edit",
            #     "PDF Link": f"https://drive.google.com/file/d/{pdf_id}/view"
            # })

        except HttpError as error:
            print(f"    -> 生成失败: {error}")

        time.sleep(1.5)

    print("\n🎉 所有任务处理完毕！")


if __name__ == '__main__':
    main()