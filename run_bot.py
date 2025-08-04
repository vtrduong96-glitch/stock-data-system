import json
import gspread
from datetime import datetime
import paho.mqtt.client as mqtt
from dotenv import load_dotenv
from requests import post, get
from random import randint
import os, ssl
import time

# --- PHẦN KẾT NỐI GOOGLE SHEETS ---
SERVICE_ACCOUNT_FILE = 'credentials.json' 
SHEET_NAME = "Stock data realtime"

gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
sh = gc.open(SHEET_NAME)
worksheet_data = sh.worksheet("Data")
worksheet_realtime = sh.worksheet("Realtime")
print(f"Đã kết nối thành công tới Google Sheet: '{SHEET_NAME}'")

# --- CACHING DATA ---
def build_symbol_map(worksheet):
    print(f"Đang đọc và tạo bản đồ cho sheet: {worksheet.title}...")
    try:
        all_symbols = worksheet.col_values(1)[1:] 
        symbol_map = {symbol: i + 2 for i, symbol in enumerate(all_symbols)}
        print("Tạo bản đồ thành công!")
        return symbol_map
    except Exception as e:
        print(f"Lỗi khi tạo bản đồ: {e}")
        return {}
data_map = build_symbol_map(worksheet_data)
realtime_map = build_symbol_map(worksheet_realtime)


# --- PHẦN XÁC THỰC DNSE (Giữ nguyên) ---
load_dotenv()
username = "anhphecan303@gmail.com"
password = "Anhyeuem1!"
investor_id = None
token = None
def authenticate(username, password):
    try:
        url = "https://api.dnse.com.vn/user-service/api/auth"
        _json = {"username": username, "password": password}
        response = post(url, json=_json)
        response.raise_for_status()
        print("Xác thực DNSE thành công!")
        return response.json().get("token")
    except Exception as e:
        print(f"Xác thực DNSE thất bại: {e}")
        return None
try:
    token = authenticate(username, password)
    if token:
        url = f"https://api.dnse.com.vn/user-service/api/me"
        headers = {"authorization": f"Bearer {token}"}
        response = get(url, headers=headers)
        response.raise_for_status()
        investor_id = str(response.json()["investorId"])
    else:
        raise Exception("Không lấy được token.")
except Exception as e:
    print(f"Lỗi trong quá trình xác thực DNSE: {e}")
    exit()


# --- CẤU HÌNH MQTT (Giữ nguyên) ---
BROKER_HOST = "datafeed-lts-krx.dnse.com.vn"
BROKER_PORT = 443
client_id = f"dnse-price-json-mqtt-ws-sub-{randint(1000, 2000)}"
client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id, protocol=mqtt.MQTTv5, transport="websockets")
client.username_pw_set(investor_id, token)
client.tls_set(cert_reqs=ssl.CERT_NONE)
client.tls_insecure_set(True)
client.ws_set_options(path="/wss")

def on_connect(client, userdata, flags, rc, properties):
    if rc == 0 and client.is_connected():
        print("Đã kết nối tới MQTT Broker! Bắt đầu lắng nghe dữ liệu...")
        client.subscribe("plaintext/quotes/krx/mdds/v2/ohlc/stock/1D/+", qos=1)
        client.subscribe("plaintext/quotes/krx/mdds/tick/v1/roundlot/symbol/+", qos=1)
    else:
        print(f"Kết nối MQTT thất bại, code: {rc}\n")

def on_message(client, userdata, msg):
    try:
        topic = msg.topic
        payload = json.loads(msg.payload.decode())
        symbol = payload.get('symbol')
        if not symbol: return

        if "ohlc" in topic:
            row_data = [payload.get('tradingDate'), payload.get('open'), payload.get('high'), payload.get('low'), payload.get('close'), payload.get('volume'), datetime.now().strftime("%Y-%m-%d %H:%M:%S")]
            
            if symbol in data_map:
                row_number = data_map[symbol]
                # SỬA LẠI THAM SỐ THEO ĐÚNG CHUẨN MỚI
                worksheet_data.update(range_name=f'B{row_number}', values=[row_data])
            else:
                worksheet_data.append_row([symbol] + row_data)
                data_map[symbol] = len(data_map) + 2

        elif "tick" in topic:
            last_price = payload.get('matchPrice')
            if symbol in realtime_map:
                row_number = realtime_map[symbol]
                worksheet_realtime.update_cell(row_number, 2, last_price)
            else:
                worksheet_realtime.append_row([symbol, last_price])
                realtime_map[symbol] = len(realtime_map) + 2
        
        time.sleep(1.1)

    except gspread.exceptions.APIError as e:
        if e.response.status_code == 429:
            print("Gặp lỗi Quota. Tạm dừng 60 giây...")
            time.sleep(60)
        else:
            print(f"Gặp lỗi API Google: {e}")
    except Exception as e:
        print(f"Lỗi khi xử lý tin nhắn: {e}")

# --- Gán callback và chạy (Giữ nguyên) ---
client.on_connect = on_connect
client.on_message = on_message
client.connect(BROKER_HOST, BROKER_PORT, keepalive=1200)
client.loop_forever()