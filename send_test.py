# send_test.py
import os, requests

token = os.environ["TELEGRAM_TOKEN"]
chat_id = os.environ["CHAT_ID"]
message = "✅ 텔레그램 연결 테스트 성공!"

r = requests.post(
    f"https://api.telegram.org/bot{token}/sendMessage",
    data={"chat_id": chat_id, "text": message}
)

print(r.status_code, r.text)
