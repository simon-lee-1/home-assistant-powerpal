#!/usr/bin/env python3
"""
Powerpal BLE Gateway — replaces the Powerpal phone app.

Connects to the Powerpal device via BLE using bleak, subscribes to measurement
notifications, stores readings in SQLite, publishes to MQTT with HA
auto-discovery, and serves an HTTP API.
"""

import asyncio
import json
import logging
import os
import signal
import sqlite3
import struct
import sys
import threading
import time
from datetime import datetime, timezone, timedelta
from http.server import HTTPServer, BaseHTTPRequestHandler
from pathlib import Path

from bleak import BleakClient, BleakScanner
import sdnotify
_sd = sdnotify.SystemdNotifier()

# ── Configuration ──────────────────────────────────────────────────

DEVICE_ADDR = os.environ.get("POWERPAL_BLE_ADDR", "")
DEVICE_NAME = os.environ.get("POWERPAL_BLE_NAME", "")
PAIRING_CODE = int(os.environ.get("POWERPAL_PAIRING_CODE", "0"))
DEVICE_ID = os.environ.get("POWERPAL_DEVICE_ID", "")
PULSES_PER_KWH = float(os.environ.get("POWERPAL_PULSES_PER_KWH", "3200"))
COST_PER_KWH = float(os.environ.get("POWERPAL_COST_PER_KWH", "0.30"))
API_PORT = int(os.environ.get("API_PORT", "8080"))
API_KEYS = [k.strip() for k in os.environ.get("API_KEYS", "").split(",") if k.strip()]
DB_PATH = os.environ.get("POWERPAL_DB_PATH", str(Path(__file__).parent / "powerpal.db"))
READING_BATCH_SIZE = int(os.environ.get("POWERPAL_BATCH_MINUTES", "1"))
RECONNECT_MIN = int(os.environ.get("POWERPAL_RECONNECT_MIN", "10"))
RECONNECT_MAX = int(os.environ.get("POWERPAL_RECONNECT_MAX", "300"))

# MQTT
MQTT_HOST = os.environ.get("MQTT_HOST", "")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ.get("MQTT_PASS", "")
MQTT_TOPIC_PREFIX = os.environ.get("MQTT_TOPIC_PREFIX", "powerpal")

# BLE UUIDs
CHAR_MEASUREMENT = "59da0001-12f4-25a6-7d4f-55961dce4205"
CHAR_PAIRING = "59da0011-12f4-25a6-7d4f-55961dce4205"
CHAR_BATCH_SIZE = "59da0013-12f4-25a6-7d4f-55961dce4205"

WH_PER_PULSE = 1000.0 / PULSES_PER_KWH

log = logging.getLogger("powerpal")


# ── SQLite Storage ─────────────────────────────────────────────────


class ReadingStore:
    """Thread-safe SQLite store for meter readings."""

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.lock = threading.Lock()
        self._init_db()

    def _init_db(self):
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.execute("""
                CREATE TABLE IF NOT EXISTS readings (
                    timestamp INTEGER PRIMARY KEY,
                    pulses INTEGER NOT NULL,
                    watt_hours REAL NOT NULL
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_readings_ts ON readings (timestamp)")
            conn.commit()
            conn.close()

    def insert(self, timestamp: int, pulses: int, watt_hours: float):
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            conn.execute(
                "INSERT OR REPLACE INTO readings (timestamp, pulses, watt_hours) VALUES (?, ?, ?)",
                (timestamp, pulses, watt_hours),
            )
            conn.commit()
            conn.close()

    def latest_reading(self):
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            row = conn.execute(
                "SELECT timestamp, pulses, watt_hours FROM readings ORDER BY timestamp DESC LIMIT 1"
            ).fetchone()
            conn.close()
            return row

    def readings_between(self, start_ts: int, end_ts: int):
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            rows = conn.execute(
                "SELECT timestamp, pulses, watt_hours FROM readings WHERE timestamp >= ? AND timestamp < ? ORDER BY timestamp",
                (start_ts, end_ts),
            ).fetchall()
            conn.close()
            return rows

    def daily_total(self, day_start_ts: int, day_end_ts: int):
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            row = conn.execute(
                "SELECT COALESCE(SUM(pulses), 0), COALESCE(SUM(watt_hours), 0) FROM readings WHERE timestamp >= ? AND timestamp < ?",
                (day_start_ts, day_end_ts),
            ).fetchone()
            conn.close()
            return row

    def total_count(self):
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            row = conn.execute("SELECT COUNT(*) FROM readings").fetchone()
            conn.close()
            return row[0]

    def total_wh(self):
        with self.lock:
            conn = sqlite3.connect(self.db_path)
            row = conn.execute("SELECT COALESCE(SUM(watt_hours), 0) FROM readings").fetchone()
            conn.close()
            return row[0]


# ── MQTT / Home Assistant ──────────────────────────────────────────


class MQTTPublisher:
    """Publishes readings to MQTT with HA auto-discovery."""

    def __init__(self):
        self.client = None
        self.available = False
        self._total_kwh = 0.0

        if not MQTT_HOST:
            log.info("MQTT disabled (no MQTT_HOST)")
            return

        import paho.mqtt.client as mqtt
        self.client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="powerpal-ble-gateway")
        if MQTT_USER:
            self.client.username_pw_set(MQTT_USER, MQTT_PASS)
        self.client.will_set(f"{MQTT_TOPIC_PREFIX}/status", "offline", retain=True)
        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.reconnect_delay_set(min_delay=5, max_delay=120)
        try:
            self.client.connect(MQTT_HOST, MQTT_PORT, keepalive=60)
        except Exception as e:
            log.warning("MQTT initial connect failed: %s (will retry)", e)
        self.client.loop_start()
        log.info("MQTT connecting to %s:%d", MQTT_HOST, MQTT_PORT)

    def _on_connect(self, client, userdata, flags, rc, properties=None):
        log.info("MQTT connected")
        self.available = True
        self._publish_discovery()
        self.client.publish(f"{MQTT_TOPIC_PREFIX}/status", "online", retain=True)

    def _on_disconnect(self, client, userdata, flags, rc, properties=None):
        log.warning("MQTT disconnected")
        self.available = False

    def _publish_discovery(self):
        device_info = {
            "identifiers": [f"powerpal_{DEVICE_ID}"],
            "name": "Powerpal",
            "manufacturer": "Powerpal Pty Ltd",
            "model": "Powerpal",
            "serial_number": DEVICE_ID,
        }

        sensors = [
            ("power", "Power", "power", "measurement", "W", "{{ value_json.power }}", "mdi:flash"),
            ("energy", "Energy", "energy", "total_increasing", "kWh", "{{ value_json.energy }}", "mdi:lightning-bolt"),
            ("daily_energy", "Daily Energy", "energy", "total_increasing", "kWh", "{{ value_json.daily_kwh }}", "mdi:calendar-today"),
            ("daily_cost", "Daily Cost", "monetary", "total_increasing", "$", "{{ value_json.daily_cost }}", "mdi:currency-usd"),
        ]

        for suffix, name, dev_class, state_class, unit, tpl, icon in sensors:
            uid = f"powerpal_{DEVICE_ID}_{suffix}"
            config = {
                "unique_id": uid,
                "object_id": uid,
                "name": name,
                "device_class": dev_class,
                "state_class": state_class,
                "unit_of_measurement": unit,
                "value_template": tpl,
                "state_topic": f"{MQTT_TOPIC_PREFIX}/state",
                "availability_topic": f"{MQTT_TOPIC_PREFIX}/status",
                "device": device_info,
                "icon": icon,
            }
            self.client.publish(
                f"homeassistant/sensor/powerpal_{DEVICE_ID}/{suffix}/config",
                json.dumps(config), retain=True,
            )

        # BLE connectivity binary sensor
        self.client.publish(
            f"homeassistant/binary_sensor/powerpal_{DEVICE_ID}/ble_connected/config",
            json.dumps({
                "unique_id": f"powerpal_{DEVICE_ID}_ble_connected",
                "object_id": f"powerpal_{DEVICE_ID}_ble_connected",
                "name": "BLE Connected",
                "device_class": "connectivity",
                "state_topic": f"{MQTT_TOPIC_PREFIX}/status",
                "payload_on": "online",
                "payload_off": "offline",
                "device": device_info,
            }),
            retain=True,
        )
        log.info("MQTT HA discovery published")

    def publish_reading(self, timestamp: int, pulses: int, watt_hours: float,
                        daily_kwh: float, daily_cost: float):
        if not self.client or not self.available:
            return
        interval = READING_BATCH_SIZE * 60
        power_watts = round(watt_hours * 3600 / interval, 1) if interval else 0
        self._total_kwh += watt_hours / 1000
        self.client.publish(f"{MQTT_TOPIC_PREFIX}/state", json.dumps({
            "power": power_watts,
            "energy": round(self._total_kwh, 6),
            "daily_kwh": round(daily_kwh, 4),
            "daily_cost": round(daily_cost, 4),
            "pulses": pulses,
            "timestamp": timestamp,
        }), retain=True)

    def stop(self):
        if self.client:
            self.client.publish(f"{MQTT_TOPIC_PREFIX}/status", "offline", retain=True)
            self.client.loop_stop()
            self.client.disconnect()


# ── BLE Gateway (bleak) ───────────────────────────────────────────


class PowerpalBLE:
    """BLE connection manager using bleak (async)."""

    def __init__(self, store: ReadingStore, mqtt: MQTTPublisher):
        self.store = store
        self.mqtt = mqtt
        self.connected = False
        self.notification_count = 0
        self.last_notification_time = 0.0
        self._client: BleakClient | None = None

    def _on_notification(self, sender, raw: bytearray):
        """Handle measurement notification from Powerpal."""
        if len(raw) < 6:
            return

        timestamp = struct.unpack_from("<I", raw, 0)[0]
        pulses = struct.unpack_from("<H", raw, 4)[0]
        watt_hours = pulses * WH_PER_PULSE

        self.store.insert(timestamp, pulses, watt_hours)
        self.last_notification_time = time.time()
        self.notification_count += 1

        dt = datetime.fromtimestamp(timestamp, tz=timezone.utc)
        log.info("Reading: %s  pulses=%d  wh=%.2f  (#%d)",
                 dt.strftime("%Y-%m-%d %H:%M:%S"), pulses, watt_hours, self.notification_count)

        # Publish to MQTT
        now = datetime.now(timezone.utc)
        day_start = int(now.replace(hour=0, minute=0, second=0, microsecond=0).timestamp())
        day_end = int((now.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)).timestamp())
        _, day_wh = self.store.daily_total(day_start, day_end)
        self.mqtt.publish_reading(timestamp, pulses, watt_hours, day_wh / 1000, day_wh / 1000 * COST_PER_KWH)

    def _on_disconnect(self, client: BleakClient):
        """Handle BLE disconnection."""
        if self.connected:
            log.warning("Device disconnected")
            self.connected = False

    async def connect_and_subscribe(self) -> bool:
        """Connect to Powerpal and subscribe to measurement notifications.

        No BLE-level bonding — the Powerpal authenticates via its custom
        pairing characteristic, not standard BLE bonding.
        """
        # Scan for device
        log.info("Scanning for %s...", DEVICE_ADDR)
        device = await BleakScanner.find_device_by_address(DEVICE_ADDR, timeout=20.0)
        if not device:
            log.warning("Device not found")
            return False
        log.info("Found: %s (%s)", device.name or "unknown", device.address)

        # Connect
        self._client = BleakClient(device, disconnected_callback=self._on_disconnect)
        try:
            await self._client.connect(timeout=15.0)
        except Exception as e:
            log.error("Connection failed: %s: %s", type(e).__name__, e)
            self._client = None
            return False

        if not self._client.is_connected:
            log.error("Not connected after connect()")
            self._client = None
            return False

        log.info("Connected")
        self.connected = True

        # Write pairing code
        try:
            await self._client.write_gatt_char(CHAR_PAIRING, struct.pack("<I", PAIRING_CODE))
            log.info("Authenticated")
        except Exception as e:
            log.error("Pairing write failed: %s", e)
            await self._disconnect()
            return False

        await asyncio.sleep(1)

        # Set batch size
        try:
            await self._client.write_gatt_char(CHAR_BATCH_SIZE, struct.pack("<I", READING_BATCH_SIZE))
            log.info("Batch: %d min", READING_BATCH_SIZE)
        except Exception as e:
            log.warning("Batch size write failed (non-fatal): %s", e)

        await asyncio.sleep(1)

        # Subscribe to measurement notifications
        try:
            await self._client.start_notify(CHAR_MEASUREMENT, self._on_notification)
            log.info("Subscribed — waiting for readings")
        except Exception as e:
            log.error("Subscribe failed: %s", e)
            await self._disconnect()
            return False

        # Verify still connected after setup
        await asyncio.sleep(3)
        if not self._client.is_connected:
            log.warning("Disconnected right after subscribe")
            self.connected = False
            return False

        return True

    async def _disconnect(self):
        """Gracefully disconnect."""
        self.connected = False
        if self._client:
            try:
                await self._client.disconnect()
            except Exception:
                pass
            self._client = None

    async def wait_until_disconnected(self):
        """Block until the device disconnects."""
        while self.connected and self._client and self._client.is_connected:
            await asyncio.sleep(2)

    async def disconnect(self):
        await self._disconnect()


# ── HTTP API ───────────────────────────────────────────────────────


def make_handler(store: ReadingStore, ble: PowerpalBLE):

    class Handler(BaseHTTPRequestHandler):
        def log_message(self, fmt, *args): pass

        def _auth(self):
            if not API_KEYS:
                return True
            key = self.headers.get("Authorization", "").replace("Bearer ", "").strip()
            if key in API_KEYS:
                return True
            self._json(401, {"error": "unauthorized"})
            return False

        def _json(self, status, data):
            body = json.dumps(data).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def _today(self):
            now = datetime.now(timezone.utc)
            s = now.replace(hour=0, minute=0, second=0, microsecond=0)
            return int(s.timestamp()), int((s + timedelta(days=1)).timestamp())

        def do_GET(self):
            p = self.path.split("?")[0]
            if p == "/health":
                latest = store.latest_reading()
                last_ts = latest[0] if latest else None
                self._json(200, {
                    "status": "ok", "ble_connected": ble.connected,
                    "notifications": ble.notification_count,
                    "last_reading_ts": last_ts,
                    "last_reading_age_s": (int(time.time()) - last_ts) if last_ts else None,
                    "readings_stored": store.total_count(),
                })
            elif p == "/energy/current" and self._auth():
                latest = store.latest_reading()
                ds, de = self._today()
                _, dwh = store.daily_total(ds, de)
                w, live, at = 0, False, datetime.now(timezone.utc).isoformat()
                if latest:
                    ts, pl, wh = latest
                    if time.time() - ts < 300:
                        w = round(wh * 3600 / (READING_BATCH_SIZE * 60), 1)
                        live = True
                    at = datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
                self._json(200, {
                    "currentWatts": w, "dailyKwh": round(dwh/1000, 4),
                    "costToday": round(dwh/1000*COST_PER_KWH, 4),
                    "isLive": live, "scrapedAt": at,
                })
            elif p == "/energy/daily" and self._auth():
                ds, de = self._today()
                rows = store.readings_between(ds, de)
                hourly = {}
                for ts, pl, wh in rows:
                    h = datetime.fromtimestamp(ts, tz=timezone.utc).hour
                    hourly.setdefault(h, {"kwh": 0.0, "cost": 0.0})
                    hourly[h]["kwh"] += wh/1000
                    hourly[h]["cost"] += wh/1000*COST_PER_KWH
                self._json(200, {
                    "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
                    "totalKwh": round(sum(v["kwh"] for v in hourly.values()), 4),
                    "totalCost": round(sum(v["cost"] for v in hourly.values()), 4),
                    "hourlyReadings": [{"hour": h, "kwh": round(hourly[h]["kwh"], 4), "cost": round(hourly[h]["cost"], 4)} for h in range(24) if h in hourly],
                })
            elif p == "/energy/hourly" and self._auth():
                q = self.path.split("?", 1)[1] if "?" in self.path else ""
                params = dict(x.split("=", 1) for x in q.split("&") if "=" in x)
                d = params.get("date", datetime.now(timezone.utc).strftime("%Y-%m-%d"))
                try:
                    day = datetime.strptime(d, "%Y-%m-%d").replace(tzinfo=timezone.utc)
                except ValueError:
                    self._json(400, {"error": "bad date"}); return
                ds, de = int(day.timestamp()), int((day+timedelta(days=1)).timestamp())
                rows = store.readings_between(ds, de)
                hourly = {}
                for ts, pl, wh in rows:
                    h = datetime.fromtimestamp(ts, tz=timezone.utc).hour
                    hourly.setdefault(h, {"kwh": 0.0, "pulses": 0, "n": 0})
                    hourly[h]["kwh"] += wh/1000; hourly[h]["pulses"] += pl; hourly[h]["n"] += 1
                self._json(200, {"date": d, "hourlyReadings": [
                    {"hour": h, "kwh": round(hourly[h]["kwh"], 4), "cost": round(hourly[h]["kwh"]*COST_PER_KWH, 4), "pulses": hourly[h]["pulses"], "readings": hourly[h]["n"]}
                    for h in range(24) if h in hourly
                ]})
            else:
                self._json(404, {"error": "not found"})

    return Handler


# ── Main ───────────────────────────────────────────────────────────


async def run_gateway():
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(message)s", datefmt="%Y-%m-%d %H:%M:%S")

    log.info("Powerpal BLE Gateway starting")
    log.info("  Device: %s (%s)", DEVICE_NAME, DEVICE_ADDR)
    log.info("  Pulses/kWh: %.0f (%.4f Wh/pulse)", PULSES_PER_KWH, WH_PER_PULSE)
    log.info("  Batch: %d min | DB: %s | Port: %d", READING_BATCH_SIZE, DB_PATH, API_PORT)

    store = ReadingStore(DB_PATH)
    mqtt_pub = MQTTPublisher()
    ble = PowerpalBLE(store, mqtt_pub)

    # HTTP server in background thread
    http = HTTPServer(("0.0.0.0", API_PORT), make_handler(store, ble))
    threading.Thread(target=http.serve_forever, daemon=True).start()
    log.info("HTTP API on port %d", API_PORT)
    _sd.notify("READY=1")

    # Shutdown handling
    shutting_down = asyncio.Event()
    loop = asyncio.get_running_loop()

    def handle_signal():
        log.info("Shutting down...")
        shutting_down.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, handle_signal)

    # Connection loop with exponential backoff
    delay = RECONNECT_MIN
    while not shutting_down.is_set():
        prev_count = ble.notification_count
        try:
            if await ble.connect_and_subscribe():
                delay = RECONNECT_MIN
                # Wait until disconnected or shutdown
                while ble.connected and not shutting_down.is_set():
                    await asyncio.sleep(2)
                    _sd.notify("WATCHDOG=1")
                    # Reset backoff when we receive readings
                    if ble.notification_count > prev_count:
                        delay = RECONNECT_MIN
                        prev_count = ble.notification_count

                if shutting_down.is_set():
                    break

                # If no readings received this session, increase backoff
                if ble.notification_count == prev_count:
                    delay = min(delay * 2, RECONNECT_MAX)
            else:
                delay = min(delay * 2, RECONNECT_MAX)
        except Exception as e:
            log.error("Error: %s", e)
            delay = min(delay * 2, RECONNECT_MAX)

        await ble.disconnect()

        if not shutting_down.is_set():
            log.info("Reconnecting in %ds...", delay)
            _sd.notify("WATCHDOG=1")
            try:
                await asyncio.wait_for(shutting_down.wait(), timeout=delay)
            except asyncio.TimeoutError:
                pass

    # Cleanup
    await ble.disconnect()
    mqtt_pub.stop()
    http.shutdown()
    log.info("Stopped")


def main():
    asyncio.run(run_gateway())


if __name__ == "__main__":
    main()
