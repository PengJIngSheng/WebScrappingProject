import random
import time
import re
import datetime
from datetime import datetime as dt
from pyairtable import Api
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup

# ================= 配置区域 =================

AIRTABLE_API_TOKEN = "patGBsrbdT5kJiPgS.9fd76f99b5e9016e9b6bf0b3e1b3e30912f878ac48b892515fb5657e6557cc7d"
BASE_ID = "appNlwtavM92s2F9I"
TABLE_NAME = "Applicants - MOT"
VIEW_NAME = "Raw Application"
TARGET_URL = "https://peneraju.org/candidate-verification"

STATUS_NORMALIZATION = {
    "NO APPLICATION FOUND": "No application found", # 比如可以改成 "candidates does not found"
    "CANDIDATE DOES NOT EXIST": "Candidates do not exist ", # 比如可以改成 "candidate not found"
    "REJECTED": "Rejected",
    "APPROVED": "Approved",
    "SUCCESS": "Approved",
    "SIGNED": "Signed",
    "ACCEPTED": "Accepted",
    "INCOMPLETE": "Incomplete",
    "REVIEW": "Review",
    "PENDING": "Pending",
    "DELETED": "Deleted"
}


# ================= 辅助函数 =================

def format_ic_number(raw_id):
    """格式化 IC 号码为 123456-12-1234"""
    raw_id = str(raw_id).strip()
    if "-" in raw_id and len(raw_id) > 12:
        return raw_id
    if len(raw_id) == 12:
        return f"{raw_id[:6]}-{raw_id[6:8]}-{raw_id[8:]}"
    return raw_id


def parse_date(date_str):
    """
    将网页上的日期字符串 (e.g., '12 January 2026') 转换为 datetime 对象以便比较
    如果解析失败，返回一个非常旧的日期作为保底
    """
    try:
        # 清理多余空格和前缀 "Application Date:"
        clean_str = date_str.replace("Application Date:", "").strip()
        # 格式匹配: 12 January 2026 (%d %B %Y)
        return dt.strptime(clean_str, "%d %B %Y")
    except Exception:
        return dt(1900, 1, 1)  # 如果无法解析，视为最旧


def extract_latest_status(html_source, ic_number):

    soup = BeautifulSoup(html_source, 'html.parser')

    # 1. 移除脚本，防止干扰
    for script in soup(["script", "style"]):
        script.decompose()

    full_text = soup.get_text(" ", strip=True).upper()  # 获取纯文本，用空格连接

    # 2. 快速检查无结果情况
    if "CANDIDATE DOES NOT EXIST" in full_text:
        return STATUS_NORMALIZATION.get("CANDIDATE DOES NOT EXIST", "Candidate does not exist")
    if "NO APPLICATION FOUND" in full_text:
        return STATUS_NORMALIZATION.get("NO APPLICATION FOUND", "No application found")

    found_records = []

    anchors = soup.find_all(string=re.compile("Application Date", re.IGNORECASE))

    for anchor in anchors:
        try:
            # 向上找，直到找到一个包含 "NRIC" 或 "NAME" 的容器 (这就是一张完整的卡片)
            card = anchor.parent
            for _ in range(6):  # 最多往上找6层
                if card and ("NRIC" in card.get_text().upper() or "NAME" in card.get_text().upper()):
                    break
                card = card.parent

            if not card:
                continue

            card_text = card.get_text(" ", strip=True)

            date_match = re.search(r"(\d{1,2})\s+([a-zA-Z]+)\s+(\d{4})", card_text)

            app_date = dt(1900, 1, 1)  # 默认值
            raw_date_str = "Unknown"

            if date_match:
                raw_date_str = date_match.group(0)  # 拿到的完整日期字符串
                try:
                    # 尝试解析日期
                    app_date = dt.strptime(raw_date_str, "%d %B %Y")
                except:
                    try:
                        # 备用尝试 (防止月份缩写，如 Jan)
                        app_date = dt.strptime(raw_date_str, "%d %b %Y")
                    except:
                        pass
            else:
                # 如果卡片里没日期，跳过
                continue

            # --- 提取状态 ---
            status_found = None
            card_text_upper = card_text.upper()

            for keyword, status_val in STATUS_NORMALIZATION.items():
                # 必须匹配全词，防止 "ACCEPTED" 匹配到 "NOT ACCEPTED" (虽然这个网站应该没这问题)
                if keyword.upper() in card_text_upper:
                    status_found = status_val
                    break

            if status_found:
                found_records.append({
                    "date": app_date,
                    "status": status_found,
                    "raw_date": raw_date_str
                })
                # print(f"    [调试] 抓取成功: {raw_date_str} -> {status_found}")

        except Exception as e:
            # print(f"    [调试] 卡片解析出错: {e}")
            pass

    # 4. 排序取最新
    if found_records:
        # 按日期降序
        found_records.sort(key=lambda x: x['date'], reverse=True)
        latest = found_records[0]

        if len(found_records) > 1:
            print(f"    -> 发现多条: 最新 {latest['raw_date']} ({latest['status']})")

        return latest['status']

    return None

# ================= 主程序 =================

def main():
    print(f"正在连接 Airtable 表格: {TABLE_NAME} ...")
    api = Api(AIRTABLE_API_TOKEN)
    table = api.table(BASE_ID, TABLE_NAME)

    try:
        records = table.all(view=VIEW_NAME)
        print(f"共找到 {len(records)} 条需处理记录。")
    except Exception as e:
        print(f"连接 Airtable 失败: {e}")
        return

    print("正在启动浏览器...")
    options = webdriver.ChromeOptions()
    # options.add_argument("--headless") # 想要后台静默运行可以取消注释
    driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()), options=options)

    # 用于批量更新的列表
    batch_updates = []

    try:
        driver.get(TARGET_URL)
        time.sleep(3)

        for i, record in enumerate(records):
            fields = record.get('fields', {})
            nric = fields.get('NRIC')
            record_id = record['id']

            if not nric:
                continue

            formatted_id = format_ic_number(nric)
            print(f"\n[{i + 1}/{len(records)}] 查询: {formatted_id} ...")

            try:
                wait = WebDriverWait(driver, 5)

                # --- 1. 输入框逻辑 (保持稳健) ---
                inputs = driver.find_elements(By.TAG_NAME, "input")
                target_input = None
                for inp in inputs:
                    if inp.is_displayed() and inp.get_attribute("type") == "text":
                        target_input = inp
                        break

                if not target_input:
                    try:
                        target_input = driver.find_element(By.XPATH,
                                                           "//button[contains(text(), 'Search')]/preceding::input[1]")
                    except:
                        pass

                if target_input:
                    driver.execute_script("arguments[0].value = '';", target_input)
                    target_input.send_keys(formatted_id)

                    # 点击搜索
                    search_btn = wait.until(
                        EC.element_to_be_clickable((By.XPATH, "//button[contains(text(), 'Search')]")))
                    search_btn.click()

                    # 等待 AJAX 加载
                    # 技巧：等待 body 里的文字发生变化，或者简单等待
                    time.sleep(1.5)

                    # --- 2. 获取 HTML 并交给 BS4 处理 ---
                    page_source = driver.page_source

                    # 调用我们写的“最新状态提取”函数
                    final_status = extract_latest_status(page_source, formatted_id)

                    if final_status:
                        print(f"    -> 最终判定结果: {final_status}")

                        # 检查是否需要更新
                        current_status = fields.get('Registration Status')
                        if current_status != final_status:
                            # 添加到待更新列表
                            batch_updates.append({"id": record_id, "fields": {"Registration Status": final_status}})
                            print("    -> [加入更新队列]")
                        else:
                            print("    -> [状态一致，无需更新]")
                    else:
                        print("    -> 未能提取到有效状态")

                else:
                    print("    -> 错误: 找不到输入框")

            except Exception as e:
                print(f"    -> 查询出错: {e}")

            # --- 3. 批量写入逻辑 (优化点) ---
            # 每积累 10 条，或者到了最后一条，就提交一次 Airtable
            if len(batch_updates) >= 10 or (i == len(records) - 1 and batch_updates):
                print(f"正在批量写入 {len(batch_updates)} 条数据到 Airtable ...")
                try:
                    table.batch_update(batch_updates)
                    print("写入成功！")
                    batch_updates = []  # 清空队列
                except Exception as e:
                    print(f"批量写入失败: {e}")

            # 稍微停顿
            sleep_time = random.uniform(1.5, 3.5)
            print(f"    [防封] 休息 {sleep_time:.2f} 秒...")
            time.sleep(sleep_time)

    finally:
        driver.quit()
        print("\n程序结束。")


if __name__ == "__main__":
    main()