import firebase_admin
from firebase_admin import credentials, firestore
from datetime import datetime, timezone, timedelta
import os
import asyncio
from fastapi import FastAPI
from fastapi_mail import ConnectionConfig, FastMail, MessageSchema
from dotenv import load_dotenv
from pydantic import BaseModel
import re
import pytz

#Test

# โหลดค่าจากไฟล์ .env
load_dotenv()

monitor_task = None  # ตัวแปรเก็บ task

# ตั้งค่าอีเมลเซิร์ฟเวอร์
conf = ConnectionConfig(
    MAIL_USERNAME=os.getenv("MAIL_USERNAME"),
    MAIL_PASSWORD=os.getenv("MAIL_PASSWORD"),
    MAIL_FROM=os.getenv("MAIL_FROM"),
    MAIL_FROM_NAME=os.getenv("MAIL_FROM_NAME", "No-Reply"),
    MAIL_PORT=int(os.getenv("MAIL_PORT", 587)),
    MAIL_SERVER=os.getenv("MAIL_SERVER"),
    MAIL_STARTTLS=os.getenv("MAIL_STARTTLS") == "True",
    MAIL_SSL_TLS=os.getenv("MAIL_SSL_TLS") == "True"
)

# ใช้ Service Account Key เพื่อเชื่อมต่อ Firebase
cred = credentials.Certificate('noti-kifarm-firebase-adminsdk-fbsvc-7b86fd53bc.json')
firebase_admin.initialize_app(cred)

# เชื่อมต่อกับ Firestore
db = firestore.client()

# FastAPI app initialization
app = FastAPI()

# Pydantic model for request data
class StatusRequest(BaseModel):
    timestamp: str

# ฟังก์ชันส่งอีเมล
async def send_alert_email(data: tuple, recipient: str):
    result = data[0]
    timenow = data[1]
    timedevice = data[2]
    status = data[3]

    if not status:
        message = MessageSchema(
            subject="🔥[K-iFarm] Status IoT device Alert!",
            recipients=[recipient],
            body=f"""
            Warning! The detected status {result}<br>
            TimeNow : {timenow}<br>
            TimeDevice : {timedevice}
            """,
            subtype="html"
        )
        fm = FastMail(conf)
        await fm.send_message(message)
        print("Email sent successfully!")
    else:
        print("No alert needed. Device status is fine.")


def read_latest_doc_from_all_collections():
    collections = ['kifarmTelemetryDataGH', 'kifarmTelemetryDataFZ']  # รายการของ collection ที่ต้องการดึงข้อมูล
    latest_docs = {}  # เก็บข้อมูลล่าสุดของแต่ละ collection

    for collection_name in collections:
        print(f"Checking collection: {collection_name}")
        collection_ref = db.collection(collection_name)
        docs = collection_ref.stream()

        doc_timestamps = []  # เก็บ doc_id, timestamp, และข้อมูลที่จำเป็นสำหรับแต่ละ collection

        for doc in docs:
            doc_id = doc.id
            #print(f"Found doc_id: {doc_id}")  # พิมพ์ doc_id เพื่อดูว่าเอกสารใน collection มีอะไรบ้าง
            try:
                # แยก device name และ timestamp จาก doc_id
                device_name, timestamp_str = doc_id.split('_')
                doc_timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%S.%f")
                doc_timestamp = doc_timestamp.replace(tzinfo=timezone.utc)
                doc_timestamps.append((doc, doc_timestamp, device_name, collection_name))
            except Exception as e:
                print(f"Error parsing timestamp from docId {doc_id}: {e}")

        if doc_timestamps:
            # หาข้อมูลล่าสุดของแต่ละ collection
            latest_doc, _, device_name, collection_name = max(doc_timestamps, key=lambda x: x[1])
            latest_docs[collection_name] = (latest_doc, device_name)

    # คืนค่าข้อมูลล่าสุดจากทั้งสอง collection
    return latest_docs

# กำหนด timezone ของประเทศไทย
THAILAND_TZ = timezone(timedelta(hours=7))

async def monitor_data():
    while True:
        print("\nChecking the latest documents...")
        latest_docs = read_latest_doc_from_all_collections()  # ดึงข้อมูลล่าสุดจากแต่ละ collection
         # กำหนดโซนเวลาเป็นประเทศไทย
        thailand_tz = pytz.timezone('Asia/Bangkok')
        # เวลาปัจจุบันในประเทศไทย
        now = datetime.now(thailand_tz)

        if not latest_docs:
            print("ไม่พบเอกสารในทั้งสอง collection")
        else:
            for collection_name, (latest_doc, device_name) in latest_docs.items():
                doc_id = latest_doc.id
                match = re.search(r"_(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d+)", doc_id)

                if match:
                    time_str = match.group(1)
                    telemetry_time_Thai = datetime.fromisoformat(time_str)

                    status_result = check_status(telemetry_time_Thai)

                    print(f"\nCollection: {collection_name}")
                    print(f"Device Name: {device_name}")
                    print(f"Latest Document ID: {doc_id}")
                    #print(f"Device Time (Thailand Local Time): {telemetry_time_Thai}")
                    #print(f"Current Time (Thailand Local Time): {now}")
                    print(f"Status: {status_result}")

                    if status_result[0] == "Device is Offline":
                        await send_alert_email(status_result, "iotfarm.krda@gmail.com")
                        print("Email sent successfully!")
                    else:
                        print(status_result[0])
                        print("ไม่มีการส่งอีเมล")

                else:
                    print(f"ไม่พบ DateTime ในชื่อเอกสาร: {doc_id}")

        await asyncio.sleep(3600)  # รอ 10 วินาทีก่อนตรวจสอบใหม่


from datetime import datetime, timedelta
import pytz

def check_status(timestamp: datetime):
    try:
        if not timestamp:
            raise ValueError("Timestamp is required")

        # กำหนดโซนเวลาเป็นประเทศไทย
        thailand_tz = pytz.timezone('Asia/Bangkok')

        # ถ้า timestamp ไม่มีข้อมูลโซนเวลา, สมมติว่าเป็นเวลาไทยแล้วแปลงเป็น timezone
        if timestamp.tzinfo is None:
            dt_input = thailand_tz.localize(timestamp)
        else:
            # ถ้ามีข้อมูลโซนเวลาอยู่แล้ว ให้แปลงเป็นเวลาในประเทศไทย
            dt_input = timestamp.astimezone(thailand_tz)

        # เวลาปัจจุบันในประเทศไทย
        now = datetime.now(thailand_tz)

        # แสดงผลเวลาในประเทศไทย
        timenow = now.strftime('%Y-%m-%d %H:%M:%S')
        timedevice = dt_input.strftime('%Y-%m-%d %H:%M:%S')
        #print("Current Time (Thailand Local Time):", timenow)
        #print("Device Time (Thailand Local Time):", timedevice)

        if now - dt_input <= timedelta(minutes=20):      #สามารถ setting parameter   (seconds,minutes)
            #print("Status: Online")
            return ('Device is Online', timenow, timedevice, True)
        elif dt_input < now:
            #print("Status: Offline")
            return ('Device is Offline', timenow, timedevice, False)
        else:
            #print("Status: Online")
            return ('Device is Online', timenow, timedevice, True)

    except Exception as e:
        print(f"Error: {e}")
        return ('Invalid timestamp format', None, None, False)


# API endpoint to check device status
@app.get("/status")
def status_server():
    return {"status": "online"}

@app.on_event("startup")
async def startup_event():
    global monitor_task
    monitor_task = asyncio.create_task(monitor_data())  # สร้าง task แบบ global

@app.on_event("shutdown")
async def shutdown_event_handler():
    global monitor_task
    if monitor_task:
        print("Shutting down gracefully...")
        monitor_task.cancel()  # ยกเลิก task
        try:
            await monitor_task  # รอให้ task หยุดทำงาน
        except asyncio.CancelledError:
            print("monitor_data() has been cancelled.")

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
