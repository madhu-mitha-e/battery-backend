from fastapi import FastAPI, HTTPException
from fastapi.responses import StreamingResponse
from sqlalchemy import create_engine, Column, Integer, String, Float, text
from sqlalchemy.orm import declarative_base, sessionmaker
from pydantic import BaseModel
from typing import List, Optional
from datetime import datetime
import pandas as pd
import io
import os

app = FastAPI(title="Battery Study Backend", version="1.0.0")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DB_PATH = os.path.join(BASE_DIR, "battery_study_server.db")
engine = create_engine(
    f"sqlite:///{DB_PATH}",
    connect_args={"check_same_thread": False}
)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

class BatteryLogDB(Base):
    __tablename__ = "battery_logs"
    id = Column(Integer, primary_key=True, index=True, autoincrement=True)
    userId = Column(String, nullable=False)
    deviceBrand = Column(String, default="")
    deviceModel = Column(String, default="")
    osVersion = Column(String, default="")
    batterySoc = Column(Integer, default=-1)
    batteryTemperatureC = Column(Float, default=-1)
    batteryVoltageMv = Column(Integer, default=-1)
    chargingStatus = Column(String, default="UNKNOWN")
    chargingSource = Column(String, default="UNKNOWN")
    isCharging = Column(Integer, default=-1)
    screenOn = Column(Integer, default=-1)
    chargingCurrentMa = Column(Float, default=-1)
    remainingCapacityMah = Column(Float, default=-1)
    batteryHealthPercent = Column(Float, default=-1)
    batteryHealthState = Column(String, default="UNKNOWN")
    timestamp = Column(String, nullable=False)
    ambientTemperatureC = Column(Float, default=-1)
    humidity = Column(Float, default=-1)
    cityName = Column(String, default="")
    logSource = Column(String, default="UNKNOWN")
    receivedAt = Column(String, default="")

Base.metadata.create_all(bind=engine)

class BatteryLogRequest(BaseModel):
    userId: str
    deviceBrand: Optional[str] = ""
    deviceModel: Optional[str] = ""
    osVersion: Optional[str] = ""
    batterySoc: Optional[int] = -1
    batteryTemperatureC: Optional[float] = -1
    batteryVoltageMv: Optional[int] = -1
    chargingStatus: Optional[str] = "UNKNOWN"
    chargingSource: Optional[str] = "UNKNOWN"
    isCharging: Optional[int] = -1
    screenOn: Optional[int] = -1
    chargingCurrentMa: Optional[float] = -1
    remainingCapacityMah: Optional[float] = -1
    batteryHealthPercent: Optional[float] = -1
    batteryHealthState: Optional[str] = "UNKNOWN"
    timestamp: Optional[str] = ""
    ambientTemperatureC: Optional[float] = -1
    humidity: Optional[float] = -1
    cityName: Optional[str] = ""
    logSource: Optional[str] = "UNKNOWN"

class BulkLogsRequest(BaseModel):
    logs: List[BatteryLogRequest]

@app.get("/health")
def health_check():
    db = SessionLocal()
    total = db.query(BatteryLogDB).count()
    db.close()
    return {
        "status": "running",
        "total_logs": total,
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    }

@app.post("/logs")
def receive_logs(request: BulkLogsRequest):
    db = SessionLocal()
    try:
        inserted = 0
        received_at = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        for log in request.logs:
            db_log = BatteryLogDB(
                userId=log.userId,
                deviceBrand=log.deviceBrand,
                deviceModel=log.deviceModel,
                osVersion=log.osVersion,
                batterySoc=log.batterySoc,
                batteryTemperatureC=log.batteryTemperatureC,
                batteryVoltageMv=log.batteryVoltageMv,
                chargingStatus=log.chargingStatus,
                chargingSource=log.chargingSource,
                isCharging=log.isCharging,
                screenOn=log.screenOn,
                chargingCurrentMa=log.chargingCurrentMa,
                remainingCapacityMah=log.remainingCapacityMah,
                batteryHealthPercent=log.batteryHealthPercent,
                batteryHealthState=log.batteryHealthState,
                timestamp=log.timestamp,
                ambientTemperatureC=log.ambientTemperatureC,
                humidity=log.humidity,
                cityName=log.cityName,
                logSource=log.logSource,
                receivedAt=received_at
            )
            db.add(db_log)
            inserted += 1
        db.commit()
        return {
            "status": "success",
            "inserted": inserted,
            "message": f"{inserted} logs received"
        }
    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=str(e))
    finally:
        db.close()

@app.get("/logs")
def get_logs(limit: int = 100, user_id: Optional[str] = None):
    db = SessionLocal()
    try:
        query = db.query(BatteryLogDB)
        if user_id:
            query = query.filter(BatteryLogDB.userId == user_id)
        logs = query.order_by(BatteryLogDB.id.desc()).limit(limit).all()
        return {
            "total": len(logs),
            "logs": [
                {
                    "id": log.id,
                    "userId": log.userId,
                    "deviceBrand": log.deviceBrand,
                    "deviceModel": log.deviceModel,
                    "osVersion": log.osVersion,
                    "batterySoc": log.batterySoc,
                    "batteryTemperatureC": log.batteryTemperatureC,
                    "batteryVoltageMv": log.batteryVoltageMv,
                    "chargingStatus": log.chargingStatus,
                    "chargingSource": log.chargingSource,
                    "isCharging": log.isCharging,
                    "screenOn": log.screenOn,
                    "chargingCurrentMa": log.chargingCurrentMa,
                    "remainingCapacityMah": log.remainingCapacityMah,
                    "batteryHealthPercent": log.batteryHealthPercent,
                    "batteryHealthState": log.batteryHealthState,
                    "timestamp": log.timestamp,
                    "ambientTemperatureC": log.ambientTemperatureC,
                    "humidity": log.humidity,
                    "cityName": log.cityName,
                    "logSource": log.logSource,
                    "receivedAt": log.receivedAt
                }
                for log in logs
            ]
        }
    finally:
        db.close()

@app.get("/stats")
def get_stats():
    db = SessionLocal()
    try:
        total_logs = db.query(BatteryLogDB).count()
        users = db.execute(
            text("SELECT COUNT(DISTINCT userId) FROM battery_logs")
        ).scalar()
        latest = db.query(BatteryLogDB).order_by(
            BatteryLogDB.id.desc()).first()
        return {
            "total_logs": total_logs,
            "total_users": users,
            "latest_log": latest.timestamp if latest else None,
            "latest_user": latest.userId if latest else None
        }
    finally:
        db.close()

@app.get("/export")
def export_csv(user_id: Optional[str] = None):
    db = SessionLocal()
    try:
        query = db.query(BatteryLogDB)
        if user_id:
            query = query.filter(BatteryLogDB.userId == user_id)
        logs = query.order_by(BatteryLogDB.id.asc()).all()
        if not logs:
            raise HTTPException(status_code=404, detail="No logs found")
        data = []
        for log in logs:
            data.append({
                "id": log.id,
                "userId": log.userId,
                "deviceBrand": log.deviceBrand,
                "deviceModel": log.deviceModel,
                "osVersion": log.osVersion,
                "batterySoc": log.batterySoc,
                "batteryTemperatureC": log.batteryTemperatureC,
                "batteryVoltageMv": log.batteryVoltageMv,
                "chargingStatus": log.chargingStatus,
                "chargingSource": log.chargingSource,
                "isCharging": log.isCharging,
                "screenOn": log.screenOn,
                "chargingCurrentMa": log.chargingCurrentMa,
                "remainingCapacityMah": log.remainingCapacityMah,
                "batteryHealthPercent": log.batteryHealthPercent,
                "batteryHealthState": log.batteryHealthState,
                "timestamp": log.timestamp,
                "ambientTemperatureC": log.ambientTemperatureC,
                "humidity": log.humidity,
                "cityName": log.cityName,
                "logSource": log.logSource,
                "receivedAt": log.receivedAt
            })
        df = pd.DataFrame(data)
        output = io.StringIO()
        df.to_csv(output, index=False)
        output.seek(0)
        if user_id:
            filename = f"battery_study_{user_id}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        else:
            filename = f"battery_study_all_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"
        return StreamingResponse(
            io.BytesIO(output.getvalue().encode()),
            media_type="text/csv",
            headers={
                "Content-Disposition": f"attachment; filename={filename}"
            }
        )
    finally:
        db.close()

@app.get("/users")
def get_users():
    db = SessionLocal()
    try:
        result = db.execute(text("""
            SELECT
                userId,
                COUNT(*) as total_logs,
                MIN(timestamp) as first_log,
                MAX(timestamp) as last_log,
                deviceBrand,
                deviceModel,
                cityName
            FROM battery_logs
            GROUP BY userId
            ORDER BY total_logs DESC
        """)).fetchall()
        return {
            "total_users": len(result),
            "users": [
                {
                    "userId": row[0],
                    "total_logs": row[1],
                    "first_log": row[2],
                    "last_log": row[3],
                    "deviceBrand": row[4],
                    "deviceModel": row[5],
                    "cityName": row[6]
                }
                for row in result
            ]
        }
    finally:
        db.close()

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)