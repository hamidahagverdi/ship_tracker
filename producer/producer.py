import asyncio
import json
import os

import websockets
from kafka import KafkaProducer

API_KEY = os.environ["AISSTREAM_API_KEY"]
TOPIC = "ship_positions"

# Xəzər dənizi: [[cənub-qərb lat, lon], [şimal-şərq lat, lon]]
# AREA_WEST = [[32.0, -125.0], [49.0, -117.0]]
# AREA_EAST = [[25.0, -81.0], [45.0, -66.0]]
AREA_GULF = [[24.0, -98.0], [31.0, -81.0]]
AREA_CASPIAN = [[36.5, 46.5], [47.2, 55.2]]

producer = KafkaProducer(
    bootstrap_servers="localhost:29092",
    key_serializer=lambda k: str(k).encode("utf-8"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)


async def stream():
    async with websockets.connect("wss://stream.aisstream.io/v0/stream") as ws:
        subscription = {
            "APIKey": API_KEY,
            "BoundingBoxes": [AREA_GULF, AREA_CASPIAN],
            "FilterMessageTypes": ["PositionReport"],
        }
        await ws.send(json.dumps(subscription))
        print("AISStream-ə qoşuldu, gəmilər gözlənilir...")

        count = 0
        async for raw in ws:
            msg = json.loads(raw)
            if msg.get("MessageType") != "PositionReport":
                continue

            meta = msg["MetaData"]
            pos = msg["Message"]["PositionReport"]

            record = {
                "mmsi": meta["MMSI"],
                "ship_name": meta.get("ShipName", "").strip(),
                "lat": pos["Latitude"],
                "lon": pos["Longitude"],
                "speed_knots": pos["Sog"],
                "course": pos["Cog"],
                "heading": pos["TrueHeading"],
                "nav_status": pos["NavigationalStatus"],
                "time_utc": meta["time_utc"],
            }

            producer.send(TOPIC, key=record["mmsi"], value=record)
            count += 1
            print(f"[{count}] {record['ship_name'] or record['mmsi']}: "
                  f"{record['lat']:.4f}, {record['lon']:.4f} | {record['speed_knots']} knot")


async def main():
    while True:  # bağlantı kəsilsə, avtomatik yenidən qoşul
        try:
            await stream()
        except Exception as e:
            print(f"Bağlantı xətası: {e}. 5 saniyəyə yenidən qoşulur...")
            await asyncio.sleep(5)


if __name__ == "__main__":
    asyncio.run(main())