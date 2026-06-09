import asyncio
import json
import logging
import time
from datetime import datetime

from google.cloud import pubsub_v1

from ..config import Settings
from ..core.state import AppState

logger = logging.getLogger(__name__)

REQUIRED_FIELDS = {
    "VehicleNumber", "Latitude", "Longitude", "Speed", "Heading",
    "RouteNumber", "DoorStatus", "OccupancyLevel", "Acceleration",
    "ScheduleOffset", "Timestamp",
}


def _vehicle_ts(payload: dict) -> float:
    try:
        return datetime.fromisoformat(payload["Timestamp"].replace("Z", "+00:00")).timestamp()
    except (KeyError, ValueError, AttributeError):
        return time.time()


async def run_pubsub_consumer(state: AppState, settings: Settings):
    loop = asyncio.get_running_loop()
    logger.info("Starting Pub/Sub consumer on %s", settings.subscription_path)

    distribution_queue: asyncio.Queue = asyncio.Queue(maxsize=settings.pubsub_queue_maxsize)

    async def worker(worker_id: int):
        processed = 0
        last_report = time.time()
        while True:
            uid, payload = await distribution_queue.get()
            try:
                current = state.vehicle_state.get(uid)
                stale = False
                if current:
                    try:
                        stale = payload["Timestamp"] <= current["Timestamp"]
                    except (KeyError, TypeError):
                        pass
                if not stale:
                    state.vehicle_state[uid] = payload
                    state.last_seen[uid] = _vehicle_ts(payload)
                    state.message_timestamps.append(time.time())
                    state.pending_updates[uid] = payload
                    processed += 1
                    now = time.time()
                    if now - last_report >= 5.0:
                        logger.info(
                            "Worker %d: %.1f msg/s",
                            worker_id, processed / (now - last_report),
                        )
                        processed = 0
                        last_report = now
            except Exception:
                logger.exception("Error processing queued message")
            finally:
                distribution_queue.task_done()

    _worker_tasks: set = set()
    for i in range(settings.pubsub_worker_count):
        t = asyncio.create_task(worker(i))
        _worker_tasks.add(t)
        t.add_done_callback(_worker_tasks.discard)

    async def callback_wrapper(message: pubsub_v1.subscriber.message.Message):
        try:
            data = json.loads(message.data.decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError) as exc:
            logger.warning("Failed to parse Pub/Sub message: %s", exc)
            message.ack()
            return

        missing_or_null = [f for f in REQUIRED_FIELDS if f not in data or data.get(f) is None]
        vehicle_id = None
        if missing_or_null:
            if "VehicleNumber" in data and data.get("VehicleNumber") is not None:
                try:
                    vehicle_id = int(data["VehicleNumber"])
                except (ValueError, TypeError):
                    vehicle_id = None
            if not vehicle_id or any(f not in {"Latitude", "Longitude"} for f in missing_or_null):
                if vehicle_id and vehicle_id in state.vehicle_state:
                    state.last_seen[vehicle_id] = time.time()
                message.ack()
                return

        try:
            if vehicle_id is None:
                vehicle_id = int(data["VehicleNumber"])
            _op = data.get("OperatorID") or 0
            prev_vehicle = state.vehicle_state.get(f"{_op}_{vehicle_id}")

            if data.get("Latitude") is None:
                if prev_vehicle and prev_vehicle.get("Latitude") is not None:
                    lat = float(prev_vehicle["Latitude"])
                else:
                    raise ValueError("Latitude missing and no prior GPS state")
            else:
                lat = float(data["Latitude"])

            if data.get("Longitude") is None:
                if prev_vehicle and prev_vehicle.get("Longitude") is not None:
                    lng = float(prev_vehicle["Longitude"])
                else:
                    raise ValueError("Longitude missing and no prior GPS state")
            else:
                lng = float(data["Longitude"])

            speed = float(data["Speed"])
            heading = float(data["Heading"])
            route = str(data.get("RouteNumber", ""))
            door = int(data["DoorStatus"])
            occupancy = int(data["OccupancyLevel"])
            accel = float(data["Acceleration"])
            schedule = int(data["ScheduleOffset"])
            timestamp = str(data["Timestamp"])
        except (ValueError, TypeError, KeyError) as exc:
            logger.warning("Invalid field in message: %s; raw=%s", exc, str(data)[:200])
            message.ack()
            return

        operator_id = data.get("OperatorID", prev_vehicle.get("OperatorID") if prev_vehicle else 0) or 0
        uid = f"{operator_id}_{vehicle_id}"

        transport_mode = str(data.get("TransportMode", prev_vehicle.get("TransportMode") if prev_vehicle else "") or "").lower()
        vehicle_type = str(data.get("VehicleType", prev_vehicle.get("VehicleType") if prev_vehicle else "") or "").lower()

        vehicle_payload = {
            "_uid": uid,
            "VehicleNumber": int(vehicle_id),
            "Latitude": lat,
            "Longitude": lng,
            "Speed": speed,
            "Heading": heading,
            "RouteNumber": route,
            "DoorStatus": door,
            "OccupancyLevel": occupancy,
            "Acceleration": accel,
            "ScheduleOffset": schedule,
            "Timestamp": timestamp,
            "LocationSource": data.get("LocationSource", prev_vehicle.get("LocationSource") if prev_vehicle else "GPS"),
            "LineID": data.get("LineID", prev_vehicle.get("LineID") if prev_vehicle else None),
            "OperatorID": operator_id,
            "TransportMode": transport_mode,
            "VehicleType": vehicle_type,
        }

        try:
            message.ack()
        except Exception as exc:
            logger.warning("Failed to ack message: %s", exc)

        try:
            loop.call_soon_threadsafe(distribution_queue.put_nowait, (uid, vehicle_payload))
        except asyncio.QueueFull:
            logger.warning("Distribution queue full, dropping message for vehicle %s", vehicle_id)
        except Exception:
            logger.exception("Failed to enqueue message")

    while True:
        subscriber = pubsub_v1.SubscriberClient()
        try:
            flow = pubsub_v1.types.FlowControl(
                max_messages=settings.pubsub_flow_max_messages,
                max_bytes=settings.pubsub_flow_max_bytes,
            )
            streaming_pull = subscriber.subscribe(
                settings.subscription_path,
                callback=lambda msg: asyncio.run_coroutine_threadsafe(
                    callback_wrapper(msg), loop
                ).result(),
                flow_control=flow,
            )
            logger.info("Pub/Sub streaming pull active")
            await loop.run_in_executor(None, streaming_pull.result)
        except Exception as exc:
            logger.exception("Pub/Sub consumer error, reconnecting in 5s: %s", exc)
            try:
                streaming_pull.cancel()
            except Exception:
                pass
            await asyncio.sleep(5)
        finally:
            subscriber.close()
