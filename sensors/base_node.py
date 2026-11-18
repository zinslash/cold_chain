
import json
import random
import threading
import time
import uuid

import paho.mqtt.client as mqtt


class SensorNode(threading.Thread):

    def __init__(
        self,
        node_id,
        zone,
        base_temp,
        temp_range,
        optimum,
        broker="127.0.0.1"
    ):
        super().__init__(daemon=True)

        self.node_id = node_id
        self.zone = zone
        self.base_temp = float(base_temp)
        self.temp_range = temp_range
        self.optimum = optimum
        self.broker = broker

        self.cooling = False
        self.heating = False
        self.alive = True
        self.drift_dir = 1

        self.stop_event = threading.Event()

        self.temperature_topic = (
            f"coldchain/{zone}/{node_id}/temperature"
        )

        self.status_topic = (
            f"coldchain/{zone}/{node_id}/status"
        )

        self.control_topic = (
            f"coldchain/control/+/{zone}"
        )

        client_id = (
            f"coldchain_{node_id}_"
            f"{uuid.uuid4().hex[:8]}"
        )

        if hasattr(mqtt, "CallbackAPIVersion"):
            self.client = mqtt.Client(
                mqtt.CallbackAPIVersion.VERSION2,
                client_id=client_id
            )
        else:
            self.client = mqtt.Client(
                client_id=client_id
            )

        self.client.will_set(
            self.status_topic,
            payload="offline",
            qos=1,
            retain=True
        )

        self.client.on_connect = self._on_connect
        self.client.on_disconnect = self._on_disconnect
        self.client.on_message = self._on_control

        self.client.reconnect_delay_set(
            min_delay=1,
            max_delay=10
        )

        print(
            f"[{self.node_id}] Connecting to "
            f"{self.broker}:1883",
            flush=True
        )

        try:
            self.client.connect(
                self.broker,
                1883,
                keepalive=60
            )

            self.client.loop_start()

        except OSError as error:
            raise ConnectionError(
                f"[{self.node_id}] Cannot connect to "
                f"local Mosquitto: {error}"
            ) from error

    def _on_connect(
        self,
        client,
        userdata,
        flags,
        reason_code,
        properties=None
    ):
        if reason_code != 0:
            print(
                f"[{self.node_id}] MQTT connection "
                f"failed: {reason_code}",
                flush=True
            )
            return

        if not self.alive:
            return

        print(
            f"[{self.node_id}] Connected to "
            "local Mosquitto",
            flush=True
        )

        client.subscribe(
            self.control_topic,
            qos=1
        )

        client.publish(
            self.status_topic,
            payload="online",
            qos=1,
            retain=True
        )

        print(
            f"[{self.node_id}] Subscribed to "
            f"{self.control_topic}",
            flush=True
        )

    def _on_disconnect(
        self,
        client,
        userdata,
        *args
    ):
        if self.alive:
            print(
                f"[{self.node_id}] MQTT disconnected. "
                "Waiting for reconnection...",
                flush=True
            )

    def _on_control(
        self,
        client,
        userdata,
        msg
    ):
        command = msg.payload.decode(
            "utf-8",
            errors="replace"
        ).strip()

        if command == "cooling:ON":
            self.cooling = True
            self.heating = False

        elif command == "heating:ON":
            self.heating = True
            self.cooling = False

        elif command == "cooling:OFF":
            self.cooling = False

        elif command == "heating:OFF":
            self.heating = False

        else:
            return

        print(
            f"[{self.node_id}] Received control: "
            f"{command}",
            flush=True
        )

    def run(self):

        try:
            while (
                self.alive
                and not self.stop_event.is_set()
            ):

                if not self.client.is_connected():
                    self.stop_event.wait(1)
                    continue

                if self.cooling:

                    self.base_temp -= (
                        0.3
                        + random.uniform(0, 0.05)
                    )

                    if self.base_temp <= self.optimum:
                        self.cooling = False

                        self.client.publish(
                            f"coldchain/control/cooling/{self.zone}",
                            "cooling:OFF",
                            qos=1
                        )

                elif self.heating:

                    self.base_temp += (
                        0.3
                        + random.uniform(0, 0.05)
                    )

                    if self.base_temp >= self.optimum:
                        self.heating = False

                        self.client.publish(
                            f"coldchain/control/heating/{self.zone}",
                            "heating:OFF",
                            qos=1
                        )

                else:

                    self.base_temp += (
                        0.12 * self.drift_dir
                        + random.uniform(-0.05, 0.08)
                    )

                temperature = round(
                    self.base_temp
                    + random.uniform(-0.3, 0.3),
                    2
                )

                humidity = round(
                    60
                    + random.uniform(-3, 3),
                    2
                )

                payload = {
                    "node": self.node_id,
                    "zone": self.zone,
                    "temp": temperature,
                    "hum": humidity,
                    "ts": int(time.time())
                }

                message = json.dumps(payload)

                result = self.client.publish(
                    self.temperature_topic,
                    message,
                    qos=0,
                    retain=False
                )

                if result.rc == mqtt.MQTT_ERR_SUCCESS:

                    print(
                        f"[{self.node_id}] "
                        f"{temperature:.2f} C | "
                        f"Humidity {humidity:.1f}% | "
                        "MQTT SENT",
                        flush=True
                    )

                else:

                    print(
                        f"[{self.node_id}] "
                        f"MQTT publish failed: "
                        f"{result.rc}",
                        flush=True
                    )

                self.stop_event.wait(5)

        finally:

            if self.client.is_connected():

                result = self.client.publish(
                    self.status_topic,
                    payload="offline",
                    qos=1,
                    retain=True
                )

                try:
                    result.wait_for_publish(timeout=1)
                except (RuntimeError, ValueError):
                    pass

            self.client.disconnect()
            self.client.loop_stop()

            print(
                f"[{self.node_id}] Stopped",
                flush=True
            )

    def kill(self):
        self.alive = False
        self.stop_event.set()
