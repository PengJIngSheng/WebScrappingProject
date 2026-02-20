import os
import time
from datetime import datetime
from pyairtable import Api
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# ================= 1. 基础配置区域 =================

# --- Airtable 配置 ---
AIRTABLE_API_TOKEN = "pat27ri02lJGXMjU4.731acf6dc2fc706b5533e0228261c06c5e90a6b4b8d44489c77479fed0774571"  # e.g., patGBs...
BASE_ID = "app3TUhKzusEAK583"  # e.g., appNlwtavM92s2F9I
TABLE_NAME = "JAVA - Student"  # e.g., Applicants - UTM (根据截图)
VIEW_NAME = "SRecord1"  # 你的视图名称

# --- Google 配置 ---
# 请在浏览器打开你的模板文档和目标文件夹，从网址中复制 ID
TEMPLATE_DOC_ID = "1WQy6JLz_0FujHvTFTu_8DhSuJ40XqpcwWnhSbXkPgLk"
TARGET_FOLDER_ID = "1Ch3ePaZx6hRqOe6JlS81BiiljqcUi7dr"

# Google API 权限范围 (读写 Drive 和 Docs)
SCOPES = [
    'https://www.googleapis.com/auth/documents',
    'https://www.googleapis.com/auth/drive'
]

# ================= 2. 字段映射配置 =================
# 左边是 Google Doc 里的 {{双引号变量}}，右边是 Airtable 里的列名
# 注意：Date 是程序自动生成的当前日期，不需要在 Airtable 里有
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
    # token.json 存储用户的访问令牌，首次运行后自动生成
    if os.path.exists('token.json'):
        creds = Credentials.from_authorized_user_file('token.json', SCOPES)

    # 如果没有有效凭证，让用户登录
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            flow = InstalledAppFlow.from_client_secrets_file('credentials.json', SCOPES)
            creds = flow.run_local_server(port=0)
        # 保存凭证以供下次使用
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

    # 获取今天的日期，格式如: 19th January 2025
    # (这里用简单点的 19 January 2025，避免后缀 st/nd/rd 的复杂处理)
    today_str = datetime.today().strftime('%d %B %Y')

    print("\n3. 开始生成 Offer Letters...")

    for i, record in enumerate(records):
        fields = record.get('fields', {})
        applicant_name = fields.get('Applicant Name')

        # 如果名字为空，跳过
        if not applicant_name:
            continue

        # [可选] 检查是否已经生成过，避免重复运行生成一堆重复文件
        # if fields.get('Document Generated'):
        #     continue

        doc_title = f"Offer Letter - {applicant_name}"
        print(f"[{i + 1}/{len(records)}] 正在生成: {doc_title} ...")

        try:
            # --- A. 复制模板文件到指定文件夹 ---
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

            # 1. 替换日期 (固定)
            requests.append({
                'replaceAllText': {
                    'containsText': {'text': '{{Date}}', 'matchCase': True},
                    'replaceText': today_str
                }
            })

            # 2. 遍历字典，替换 Airtable 里的字段
            for placeholder, airtable_col in FIELD_MAPPING.items():
                # 如果 Airtable 里某些字段没填，用空字符串代替，防止报错
                replace_val = str(fields.get(airtable_col, ""))

                requests.append({
                    'replaceAllText': {
                        'containsText': {'text': placeholder, 'matchCase': True},
                        'replaceText': replace_val
                    }
                })

            # --- C. 执行批量替换 ---
            docs_service.documents().batchUpdate(
                documentId=new_doc_id,
                body={'requests': requests}
            ).execute()

            print(f"    -> 成功！文档链接: https://docs.google.com/document/d/{new_doc_id}/edit")

            # [进阶推荐] 这里可以加一行代码，把生成的文档链接写回 Airtable
            # table.update(record['id'], {"Offer Link": f"https://docs.google.com/document/d/{new_doc_id}/edit"})

        except HttpError as error:
            print(f"    -> 生成失败: {error}")

        # 避免触发 Google API 频率限制
        time.sleep(1.5)

    print("\n🎉 所有任务处理完毕！")


if __name__ == '__main__':
    main()