import gspread
from vnstock import Listing, Trading 
from datetime import datetime, timedelta
import time

# --- CẤU HÌNH ---
SERVICE_ACCOUNT_FILE = 'credentials.json' 
SHEET_NAME = "Stock data realtime"
WORKSHEET_NAME = "Data"

# --- HÀM CHÍNH ĐỂ NẠP DỮ LIỆU ---
if __name__ == "__main__":
    # Kết nối Google Sheet
    gc = gspread.service_account(filename=SERVICE_ACCOUNT_FILE)
    sh = gc.open(SHEET_NAME)
    worksheet = sh.worksheet(WORKSHEET_NAME)
    print("Đã kết nối tới Google Sheet.")
    
    listing_tool = Listing()
    trading_tool = Trading()
    
    print("Đang lấy danh sách mã chính thức...")
    try:
        symbols_df = listing_tool.all_symbols() 
        
        symbols_to_fetch = symbols_df['symbol'].tolist()
        print(f"Lấy thành công danh sách {len(symbols_to_fetch)} mã cổ phiếu.")
    except Exception as e:
        print(f"Lỗi khi lấy danh sách mã: {e}")
        symbols_to_fetch = []

    if symbols_to_fetch:
        # Xóa dữ liệu cũ trước khi nạp
        print("Đang xóa dữ liệu cũ trong Sheet 'Data'...")
        worksheet.clear()
        headers = ["Symbol", "TradingDate", "Open", "High", "Low", "Close", "Volume", "LastUpdated"]
        worksheet.append_row(headers)
        print("Đã xóa xong.")

        all_data_to_append = []
        total_symbols = len(symbols_to_fetch)

        # Xác định khoảng thời gian 1 năm
        end_date = datetime.now()
        start_date = end_date - timedelta(days=365)
        start_date_str = start_date.strftime('%Y-%m-%d')
        end_date_str = end_date.strftime('%Y-%m-%d')
        
        # Vòng lặp qua từng mã để lấy dữ liệu lịch sử
        for index, symbol in enumerate(symbols_to_fetch):
            print(f"Đang tải mã {symbol} ({index + 1}/{total_symbols})...")
            try:
                # SỬA LỖI: Dùng tên hàm đúng là price_history()
                df = trading_tool.price_history(symbol=symbol, start_date=start_date_str, end_date=end_date_str, resolution='1D')
                
                if not df.empty:
                    df['symbol'] = symbol
                    df['last_updated'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                    df = df.rename(columns={'time': 'trading_date', 'open': 'open', 'high': 'high', 'low': 'low', 'close': 'close', 'volume': 'volume'})
                    df = df[['symbol', 'trading_date', 'open', 'high', 'low', 'close', 'volume', 'last_updated']]
                    all_data_to_append.extend(df.values.tolist())
                
                if len(all_data_to_append) >= 5000:
                    print(f">>> Đang ghi {len(all_data_to_append)} hàng vào Google Sheet...")
                    worksheet.append_rows(all_data_to_append, value_input_option='USER_ENTERED')
                    all_data_to_append = []
                    print(">>> Tạm nghỉ 60 giây để tránh lỗi Quota...")
                    time.sleep(60)
                
            except Exception as e:
                print(f"  -> Lỗi khi tải mã {symbol}: {e}. Bỏ qua...")
                time.sleep(1)
                continue

        if all_data_to_append:
            print(f">>> Đang ghi {len(all_data_to_append)} hàng cuối cùng vào Google Sheet...")
            worksheet.append_rows(all_data_to_append, value_input_option='USER_ENTERED')

        print("\n✅ HOÀN THÀNH! Đã nạp xong toàn bộ dữ liệu lịch sử bằng vnstock.")