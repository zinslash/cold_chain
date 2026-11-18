
import json
import logging
import math
import os
import threading
import time
import uuid

from flask import Flask, jsonify, request
import paho.mqtt.client as mqtt

BROKER = os.getenv("COLDCHAIN_MQTT_HOST", "127.0.0.1")
MQTT_PORT = int(os.getenv("COLDCHAIN_MQTT_PORT", "1883"))
TOPIC_ROOT = os.getenv(
    "COLDCHAIN_MQTT_PREFIX",
    "coldchain/lab-7b39f2/zoneA/S1"
).strip("/")

TOPIC_COMMAND = f"{TOPIC_ROOT}/control/command"
TOPIC_STATE = f"{TOPIC_ROOT}/control/state"
TOPIC_TELEMETRY = f"{TOPIC_ROOT}/temperature"
TOPIC_STATUS = f"{TOPIC_ROOT}/status"

SAFE_MIN = -25.0
SAFE_MAX = -15.0
DEFAULT_TARGET = -20.0
ACK_TIMEOUT = 15.0
FRESH_SECONDS = 12.0
VALID_MODES = {"auto", "heat", "cool", "off"}
VALID_ACTUATORS = {"heat", "cool", "off"}

app = Flask(__name__)
state_lock = threading.Lock()

state = {
    "broker_connected": False,
    "esp32_status": "unknown",
    "temp": None,
    "hum": None,
    "ambient": None,
    "actuator": "unknown",
    "device_mode": None,
    "device_target": None,
    "requested_mode": "auto",
    "requested_target": DEFAULT_TARGET,
    "pending_request_id": None,
    "pending_mode": None,
    "pending_target": None,
    "pending_since": None,
    "last_ack_id": None,
    "last_ack_ok": None,
    "last_ack_message": None,
    "last_ack_time": None,
    "last_command_observed": None,
    "last_command_id": None,
    "last_seen": None,
    "last_telemetry_monotonic": None,
    "last_state_monotonic": None,
}


def as_number(value):
    if isinstance(value, bool) or not isinstance(
        value, (int, float)
    ):
        return None

    number = float(value)

    if not math.isfinite(number):
        return None

    return number


def expire_pending_locked():
    started = state["pending_since"]

    if (
        started is None
        or time.monotonic() - started < ACK_TIMEOUT
    ):
        return

    state["last_ack_id"] = state["pending_request_id"]
    state["last_ack_ok"] = False
    state["last_ack_message"] = (
        "No matching ESP32 acknowledgement within 15 seconds"
    )
    state["last_ack_time"] = time.time()

    state["pending_request_id"] = None
    state["pending_mode"] = None
    state["pending_target"] = None
    state["pending_since"] = None


def on_connect(
    client,
    userdata,
    flags,
    reason_code,
    properties=None
):
    connected = reason_code == 0

    with state_lock:
        state["broker_connected"] = connected

    if not connected:
        logging.error(
            "MQTT connection rejected: %s",
            reason_code
        )
        return

    logging.info(
        "Connected to local Mosquitto %s:%s",
        BROKER,
        MQTT_PORT
    )

    topics = (
        TOPIC_TELEMETRY,
        TOPIC_STATE,
        TOPIC_STATUS,
        TOPIC_COMMAND
    )

    for topic in topics:
        result, mid = client.subscribe(
            topic,
            qos=1
        )

        logging.info(
            "Subscribed %s: result=%s mid=%s",
            topic,
            result,
            mid
        )


def on_disconnect(client, userdata, *args):
    if len(args) >= 3:
        reason_code = args[1]
    elif args:
        reason_code = args[0]
    else:
        reason_code = "unknown"

    with state_lock:
        state["broker_connected"] = False

    logging.warning(
        "MQTT disconnected: %s",
        reason_code
    )


def on_message(client, userdata, message):
    topic = message.topic

    raw = message.payload.decode(
        "utf-8",
        errors="replace"
    )

    if topic == TOPIC_STATUS:
        with state_lock:
            state["esp32_status"] = raw.strip().lower()

        logging.info(
            "S1 status: %s",
            raw.strip()
        )
        return

    try:
        payload = json.loads(raw)

    except (ValueError, TypeError):
        logging.warning(
            "Invalid JSON on topic %s",
            topic
        )
        return

    if not isinstance(payload, dict):
        return

    if payload.get("node") != "S1":
        return

    if topic == TOPIC_COMMAND:
        with state_lock:
            state["last_command_observed"] = time.time()
            state["last_command_id"] = payload.get(
                "request_id"
            )

        logging.info(
            "S1 command seen: %s",
            raw
        )
        return

    if topic not in (TOPIC_TELEMETRY, TOPIC_STATE):
        return

    now_wall = time.time()
    now_mono = time.monotonic()

    with state_lock:

        if topic == TOPIC_TELEMETRY:
            temp = as_number(
                payload.get("temp")
            )

            if temp is None:
                logging.warning(
                    "S1 telemetry has invalid temperature: %s",
                    raw
                )
                return

            state["temp"] = temp
            state["last_seen"] = now_wall
            state["last_telemetry_monotonic"] = now_mono

        elif topic == TOPIC_STATE:
            state["last_state_monotonic"] = now_mono

        for key in ("ambient", "hum"):
            value = as_number(
                payload.get(key)
            )

            if value is not None:
                state[key] = value

        mode = payload.get("mode")

        if (
            isinstance(mode, str)
            and mode in VALID_MODES
        ):
            state["device_mode"] = mode

        actuator = payload.get("actuator")

        if (
            isinstance(actuator, str)
            and actuator in VALID_ACTUATORS
        ):
            state["actuator"] = actuator

        target = as_number(
            payload.get("target")
        )

        if (
            target is not None
            and SAFE_MIN <= target <= SAFE_MAX
        ):
            state["device_target"] = target

        expire_pending_locked()

        pending_id = state["pending_request_id"]

        if (
            topic == TOPIC_STATE
            and pending_id is not None
            and payload.get("request_id") == pending_id
            and isinstance(payload.get("ok"), bool)
        ):
            expected_mode = state["pending_mode"]
            expected_target = state["pending_target"]

            if payload["ok"] is False:
                acknowledged = False
                ack_message = (
                    "ESP32 rejected the command"
                )

            elif (
                mode != expected_mode
                or target is None
                or abs(target - expected_target) > 0.06
            ):
                acknowledged = False
                ack_message = (
                    "ESP32 returned a different mode or target"
                )

            else:
                acknowledged = True
                ack_message = (
                    f"ESP32 confirmed {mode.upper()} "
                    f"at {target:.1f} C"
                )

            state["last_ack_id"] = pending_id
            state["last_ack_ok"] = acknowledged
            state["last_ack_message"] = ack_message
            state["last_ack_time"] = now_wall

            state["pending_request_id"] = None
            state["pending_mode"] = None
            state["pending_target"] = None
            state["pending_since"] = None

            logging.info(
                "S1 acknowledgement: %s",
                ack_message
            )


def make_client():
    client_id = (
        "coldchain-s1-api-"
        + uuid.uuid4().hex[:12]
    )

    if hasattr(mqtt, "CallbackAPIVersion"):
        client = mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id
        )
    else:
        client = mqtt.Client(
            client_id=client_id
        )

    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.on_message = on_message

    client.reconnect_delay_set(
        min_delay=1,
        max_delay=20
    )

    return client


mqtt_client = make_client()


@app.get("/health")
def health():
    with state_lock:
        connected = state["broker_connected"]

    return jsonify(
        server="running",
        broker_connected=connected,
        broker=BROKER,
        port=MQTT_PORT,
        topic_root=TOPIC_ROOT
    )


@app.get("/api/s1")
def get_sensor_state():
    with state_lock:
        expire_pending_locked()
        result = dict(state)

    telemetry_at = result.pop(
        "last_telemetry_monotonic"
    )

    controller_at = result.pop(
        "last_state_monotonic"
    )

    result.pop("pending_since")

    now = time.monotonic()

    result["online"] = bool(
        result["broker_connected"]
        and telemetry_at is not None
        and now - telemetry_at < FRESH_SECONDS
        and result["esp32_status"] != "offline"
    )

    result["controller_online"] = bool(
        result["broker_connected"]
        and controller_at is not None
        and now - controller_at < FRESH_SECONDS
    )

    result["safe_min"] = SAFE_MIN
    result["safe_max"] = SAFE_MAX

    result["breach"] = (
        result["online"]
        and result["temp"] is not None
        and not SAFE_MIN <= result["temp"] <= SAFE_MAX
    )

    result["topics"] = {
        "command": TOPIC_COMMAND,
        "state": TOPIC_STATE,
        "temperature": TOPIC_TELEMETRY,
        "status": TOPIC_STATUS
    }

    return jsonify(result)


@app.post("/api/s1")
def set_sensor_control():
    body = request.get_json(
        silent=True
    )

    if not isinstance(body, dict) or not body:
        return jsonify(
            error="Send JSON with mode and/or target"
        ), 400

    allowed = {
        "mode",
        "target",
        "setpoint"
    }

    if any(key not in allowed for key in body):
        return jsonify(
            error="Only mode, target, setpoint are supported"
        ), 400

    if "target" in body and "setpoint" in body:
        first = as_number(
            body["target"]
        )

        second = as_number(
            body["setpoint"]
        )

        if (
            first is None
            or second is None
            or first != second
        ):
            return jsonify(
                error="target and setpoint must agree"
            ), 400

    with state_lock:
        expire_pending_locked()

        mode = state["requested_mode"]
        target = state["requested_target"]
        connected = state["broker_connected"]

        already_pending = (
            state["pending_request_id"] is not None
        )

    if "mode" in body:
        mode = body["mode"]

        if (
            not isinstance(mode, str)
            or mode not in VALID_MODES
        ):
            return jsonify(
                error="mode must be auto, heat, cool or off"
            ), 400

    if "target" in body or "setpoint" in body:
        target = as_number(
            body.get(
                "target",
                body.get("setpoint")
            )
        )

        if (
            target is None
            or not SAFE_MIN <= target <= SAFE_MAX
        ):
            return jsonify(
                error="Target must be between -25 and -15 C"
            ), 400

    if not connected or not mqtt_client.is_connected():
        return jsonify(
            error="Local MQTT broker disconnected"
        ), 503

    if already_pending:
        return jsonify(
            error="Wait for the previous command acknowledgement"
        ), 409

    request_id = (
        "api-"
        + uuid.uuid4().hex[:16]
    )

    command = {
        "node": "S1",
        "mode": mode,
        "target": round(target, 2),
        "request_id": request_id,
        "ts": int(time.time() * 1000)
    }

    with state_lock:
        expire_pending_locked()

        if state["pending_request_id"] is not None:
            return jsonify(
                error="Another command is pending"
            ), 409

        state["pending_request_id"] = request_id
        state["pending_mode"] = mode
        state["pending_target"] = target
        state["pending_since"] = time.monotonic()

        state["last_ack_id"] = None
        state["last_ack_ok"] = None
        state["last_ack_message"] = None

    info = mqtt_client.publish(
        TOPIC_COMMAND,
        json.dumps(command),
        qos=1,
        retain=False
    )

    if info.rc != mqtt.MQTT_ERR_SUCCESS:
        with state_lock:
            if (
                state["pending_request_id"]
                == request_id
            ):
                state["pending_request_id"] = None
                state["pending_mode"] = None
                state["pending_target"] = None
                state["pending_since"] = None

        return jsonify(
            error=f"MQTT publish failed, code {info.rc}"
        ), 503

    with state_lock:
        state["requested_mode"] = mode
        state["requested_target"] = target

    logging.info(
        "S1 command sent: %s",
        command
    )

    return jsonify(
        queued=True,
        confirmed=False,
        request_id=request_id,
        command=command,
        message=(
            "Command queued; check GET /api/s1 "
            "for acknowledgement"
        )
    ), 202


def main():
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(message)s"
    )

    logging.info(
        "Local broker: %s:%s",
        BROKER,
        MQTT_PORT
    )

    logging.info(
        "S1 command topic: %s",
        TOPIC_COMMAND
    )

    logging.info(
        "S1 state topic: %s",
        TOPIC_STATE
    )

    logging.info(
        "S1 telemetry topic: %s",
        TOPIC_TELEMETRY
    )

    mqtt_client.connect_async(
        BROKER,
        MQTT_PORT,
        keepalive=30
    )

    mqtt_client.loop_start()

    try:
        app.run(
            host="127.0.0.1",
            port=8765,
            debug=False,
            use_reloader=False,
            threaded=True
        )

    finally:
        mqtt_client.disconnect()
        mqtt_client.loop_stop()


if __name__ == "__main__":
    main()
