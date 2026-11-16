
import json
import logging
import math
import os
import threading
import time
import uuid

from flask import Flask, jsonify, request
import paho.mqtt.client as mqtt

BROKER = os.getenv(
    "COLDCHAIN_MQTT_HOST",
    "broker.hivemq.com"
)

MQTT_PORT = int(
    os.getenv("COLDCHAIN_MQTT_PORT", "1883")
)

TOPIC_ROOT = os.getenv(
    "COLDCHAIN_MQTT_PREFIX",
    "coldchain/zoneA/S1"
).strip("/")

TOPIC_COMMAND = f"{TOPIC_ROOT}/control/command"
TOPIC_STATE = f"{TOPIC_ROOT}/control/state"
TOPIC_TELEMETRY = f"{TOPIC_ROOT}/temperature"
TOPIC_STATUS = f"{TOPIC_ROOT}/status"

SAFE_MIN = -25.0
SAFE_MAX = -15.0
DEFAULT_TARGET = -20.0

VALID_MODES = {
    "auto",
    "heat",
    "cool",
    "off"
}

VALID_ACTUATORS = {
    "heat",
    "cool",
    "off"
}

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
    "last_ack_id": None,
    "last_ack_ok": None,
    "last_ack_time": None,
    "last_command_observed": None,
    "last_command_id": None,
    "last_seen": None,
    "last_telemetry_monotonic": None,
    "last_state_monotonic": None
}


def as_number(value):
    if isinstance(value, bool):
        return None

    if not isinstance(value, (int, float)):
        return None

    number = float(value)

    if not math.isfinite(number):
        return None

    return number


def on_connect(
    client,
    userdata,
    flags,
    reason_code,
    properties
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
        "Connected to MQTT broker %s:%s",
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
            "Subscribe %s: rc=%s mid=%s",
            topic,
            result,
            mid
        )


def on_disconnect(
    client,
    userdata,
    disconnect_flags,
    reason_code,
    properties
):
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
            state["esp32_status"] = raw.strip()

        logging.info(
            "S1 status: %s",
            raw.strip()
        )
        return

    try:
        payload = json.loads(raw)

    except (ValueError, TypeError) as error:
        logging.warning(
            "Invalid JSON on %s: %s",
            topic,
            error
        )
        return

    if not isinstance(payload, dict):
        return

    if payload.get("node") != "S1":
        return

    if topic == TOPIC_COMMAND:
        request_id = payload.get("request_id")

        with state_lock:
            state["last_command_observed"] = time.time()
            state["last_command_id"] = request_id

        logging.info(
            "Command observed on broker: %s",
            raw
        )
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
                    "Invalid sensor temperature: %s",
                    raw
                )
                return

            state["temp"] = temp
            state["last_seen"] = now_wall
            state["last_telemetry_monotonic"] = now_mono

        elif topic == TOPIC_STATE:
            state["last_state_monotonic"] = now_mono

        else:
            return

        for key in ("ambient", "hum"):
            if key in payload:
                number = as_number(payload[key])

                if number is not None:
                    state[key] = number

        mode = payload.get("mode")

        if mode in VALID_MODES:
            state["device_mode"] = mode

        actuator = payload.get("actuator")

        if actuator in VALID_ACTUATORS:
            state["actuator"] = actuator

        target = as_number(
            payload.get("target")
        )

        if (
            target is not None
            and SAFE_MIN <= target <= SAFE_MAX
        ):
            state["device_target"] = target

        request_id = payload.get("request_id")

        if (
            request_id
            and request_id == state["pending_request_id"]
        ):
            if payload.get("ok") is True:
                state["pending_request_id"] = None
                state["last_ack_id"] = request_id
                state["last_ack_ok"] = True
                state["last_ack_time"] = now_wall

                logging.info(
                    "ESP32 ACK confirmed: %s",
                    request_id
                )

            elif payload.get("ok") is False:
                state["pending_request_id"] = None
                state["last_ack_id"] = request_id
                state["last_ack_ok"] = False
                state["last_ack_time"] = now_wall

                logging.warning(
                    "ESP32 rejected command: %s",
                    request_id
                )


def make_client():
    client = mqtt.Client(
        mqtt.CallbackAPIVersion.VERSION2,
        client_id=(
            "coldchain-s1-api-"
            + uuid.uuid4().hex[:12]
        )
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


@app.get("/api/s1")
def get_sensor_state():
    with state_lock:
        result = dict(state)

    telemetry_at = result.pop(
        "last_telemetry_monotonic"
    )

    controller_at = result.pop(
        "last_state_monotonic"
    )

    now = time.monotonic()

    result["online"] = bool(
        result["broker_connected"]
        and telemetry_at is not None
        and now - telemetry_at < 12
        and result["esp32_status"] != "offline"
    )

    result["controller_online"] = bool(
        result["broker_connected"]
        and controller_at is not None
        and now - controller_at < 12
    )

    result["safe_min"] = SAFE_MIN
    result["safe_max"] = SAFE_MAX

    result["breach"] = (
        result["temp"] is not None
        and not (
            SAFE_MIN
            <= result["temp"]
            <= SAFE_MAX
        )
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
    body = request.get_json(silent=True)

    if not isinstance(body, dict):
        return jsonify(
            error="Send JSON containing mode and/or target"
        ), 400

    allowed = {
        "mode",
        "target",
        "setpoint"
    }

    if not body or any(k not in allowed for k in body):
        return jsonify(
            error="Only mode, target and setpoint are supported"
        ), 400

    if (
        "target" in body
        and "setpoint" in body
        and body["target"] != body["setpoint"]
    ):
        return jsonify(
            error="target and setpoint must agree"
        ), 400

    with state_lock:
        mode = state["requested_mode"]
        target = state["requested_target"]
        connected = state["broker_connected"]

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
            error="MQTT broker disconnected"
        ), 503

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
        state["pending_request_id"] = request_id

    result = mqtt_client.publish(
        TOPIC_COMMAND,
        json.dumps(command),
        qos=1,
        retain=False
    )

    if result.rc != mqtt.MQTT_ERR_SUCCESS:
        with state_lock:
            if state["pending_request_id"] == request_id:
                state["pending_request_id"] = None

        return jsonify(
            error=f"MQTT publish failed: {result.rc}"
        ), 503

    with state_lock:
        state["requested_mode"] = mode
        state["requested_target"] = target

    logging.info(
        "Published command id=%s mode=%s target=%s",
        request_id,
        mode,
        target
    )

    return jsonify(
        queued=True,
        confirmed=False,
        request_id=request_id,
        command=command,
        message=(
            "Command queued. Check GET /api/s1 "
            "for ESP32 acknowledgement."
        )
    ), 202


@app.get("/health")
def health():
    with state_lock:
        connected = state["broker_connected"]

    return jsonify(
        server="running",
        broker_connected=connected,
        broker=BROKER,
        port=MQTT_PORT
    )


def main():
    logging.basicConfig(
        level=logging.INFO,
        format=(
            "%(asctime)s %(levelname)s %(message)s"
        )
    )

    logging.info(
        "Broker: %s:%s",
        BROKER,
        MQTT_PORT
    )

    logging.info(
        "Command topic: %s",
        TOPIC_COMMAND
    )

    logging.info(
        "Telemetry topic: %s",
        TOPIC_TELEMETRY
    )

    logging.info(
        "State topic: %s",
        TOPIC_STATE
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
