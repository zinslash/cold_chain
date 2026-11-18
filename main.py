
import csv
import json
import math
import random
import socket
import threading
import time
import uuid
from pathlib import Path

try:
    import paho.mqtt.client as mqtt
except ImportError:
    raise SystemExit("Install MQTT first: python -m pip install paho-mqtt")

BROKER = "127.0.0.1"
PORT = 1883
WOKWI_S1_ROOT = "coldchain/lab-7b39f2/zoneA/S1"
WOKWI_S1_TEMP_TOPIC = f"{WOKWI_S1_ROOT}/temperature"
SIMULATED_TEMP_FILTER = "coldchain/+/+/temperature"
LOG_FILE = Path(__file__).resolve().parent / "logs" / "readings.csv"

THRESHOLDS = {
    "zoneA": {"lo": -25, "hi": -15, "opt": -18},
    "zoneB": {"lo": 0, "hi": 5, "opt": 2},
    "zoneC": {"lo": 2, "hi": 8, "opt": 5},
}

NODE_ZONES = {
    "S1": "zoneA", "S2": "zoneA", "S3": "zoneB",
    "S4": "zoneB", "S5": "zoneC", "S6": "zoneC",
}

BASE_TEMPS = {"S2": -20, "S3": 2, "S4": 4, "S5": 6, "S6": 6}

SENSOR_CONFIGS = [
    ("S2", "zoneA", -20, (-25, -15), -18),
    ("S3", "zoneB", 2, (0, 5), 2),
    ("S4", "zoneB", 4, (0, 5), 2),
    ("S5", "zoneC", 6, (2, 8), 5),
    ("S6", "zoneC", 6, (2, 8), 5),
]


def make_mqtt_client(label):
    client_id = f"{label}_{uuid.uuid4().hex[:10]}"

    if hasattr(mqtt, "CallbackAPIVersion"):
        return mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id
        )

    return mqtt.Client(client_id=client_id)


def connect_client(client):
    client.reconnect_delay_set(min_delay=1, max_delay=10)

    try:
        client.connect(BROKER, PORT, keepalive=60)
    except OSError as exc:
        raise RuntimeError(
            f"Cannot connect to local Mosquitto at {BROKER}:{PORT}. "
            "Start Mosquitto before running main.py."
        ) from exc

    client.loop_start()


def subscribe_sensor_topics(client):
    client.subscribe(SIMULATED_TEMP_FILTER, qos=0)
    client.subscribe(WOKWI_S1_TEMP_TOPIC, qos=0)


def is_sensor_telemetry(topic, payload):
    if not isinstance(payload, dict):
        return False

    node = payload.get("node")
    zone = payload.get("zone")
    temp = payload.get("temp")

    if node not in NODE_ZONES or zone != NODE_ZONES[node]:
        return False

    if isinstance(temp, bool) or not isinstance(temp, (int, float)):
        return False

    if not math.isfinite(temp):
        return False

    if node == "S1":
        return (
            topic == WOKWI_S1_TEMP_TOPIC
            and payload.get("controller") is True
        )

    return topic == f"coldchain/{zone}/{node}/temperature"


class SensorNode(threading.Thread):
    def __init__(
        self,
        node_id,
        zone,
        base_temp,
        temp_range,
        optimum
    ):
        super().__init__(
            daemon=True,
            name=f"Sensor-{node_id}"
        )

        self.node_id = node_id
        self.zone = zone
        self.base_temp = float(base_temp)
        self.temp_range = temp_range
        self.optimum = optimum
        self.cooling = False
        self.heating = False
        self.stop_event = threading.Event()

        self.status_topic = (
            f"coldchain/{zone}/{node_id}/status"
        )

        self.temperature_topic = (
            f"coldchain/{zone}/{node_id}/temperature"
        )

        self.client = make_mqtt_client(
            f"sensor_{node_id}"
        )

        self.client.will_set(
            self.status_topic,
            "offline",
            qos=1,
            retain=True
        )

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_control

        connect_client(self.client)

    def on_connect(
        self,
        client,
        userdata,
        flags,
        reason_code,
        properties=None
    ):
        if reason_code != 0:
            print(
                f"[{self.node_id}] "
                f"MQTT connection refused: {reason_code}"
            )
            return

        client.subscribe(
            f"coldchain/control/+/{self.zone}",
            qos=1
        )

        client.publish(
            self.status_topic,
            "online",
            qos=1,
            retain=True
        )

        print(
            f"[{self.node_id}] "
            "Connected to local Mosquitto"
        )

    def on_control(self, client, userdata, message):
        command = message.payload.decode(
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

    def run(self):
        try:
            while not self.stop_event.is_set():
                if not self.client.is_connected():
                    self.stop_event.wait(0.5)
                    continue

                if self.cooling:
                    self.base_temp -= (
                        0.3 + random.uniform(0, 0.05)
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
                        0.3 + random.uniform(0, 0.05)
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
                        0.12 + random.uniform(-0.05, 0.08)
                    )

                temp = round(
                    self.base_temp + random.uniform(-0.3, 0.3),
                    2
                )

                humidity = round(
                    60 + random.uniform(-3, 3),
                    2
                )

                payload = json.dumps({
                    "node": self.node_id,
                    "zone": self.zone,
                    "temp": temp,
                    "hum": humidity,
                    "ts": int(time.time()),
                })

                info = self.client.publish(
                    self.temperature_topic,
                    payload,
                    qos=0
                )

                if info.rc == mqtt.MQTT_ERR_SUCCESS:
                    print(
                        f"[{self.node_id}] "
                        f"{temp:.2f} C | "
                        f"Humidity {humidity:.1f}%"
                    )
                else:
                    print(
                        f"[{self.node_id}] "
                        f"Publish failed, MQTT code {info.rc}"
                    )

                self.stop_event.wait(5)

        finally:
            if self.client.is_connected():
                result = self.client.publish(
                    self.status_topic,
                    "offline",
                    qos=1,
                    retain=True
                )

                try:
                    result.wait_for_publish(timeout=1)
                except (RuntimeError, ValueError):
                    pass

            self.client.disconnect()
            self.client.loop_stop()

    def kill(self):
        self.stop_event.set()


class DODAGNode:
    def __init__(self, node_id, rank, parent=None):
        self.node_id = node_id
        self.rank = rank
        self.parent = parent
        self.children = []
        self.alive = True


class DODAG:
    def __init__(self):
        specs = {
            "root": (1, None),
            "routerA": (2, "root"),
            "routerB": (2, "root"),
            "routerC": (2, "root"),
            "S1": (3, "routerA"),
            "S2": (3, "routerA"),
            "S3": (3, "routerB"),
            "S4": (3, "routerB"),
            "S5": (3, "routerC"),
            "S6": (3, "routerC"),
        }

        self.nodes = {
            node_id: DODAGNode(node_id, rank, parent)
            for node_id, (rank, parent) in specs.items()
        }

        for node_id, node in self.nodes.items():
            if node.parent:
                self.nodes[node.parent].children.append(node_id)

        print(
            "[DODAG] S1 is external Wokwi; "
            "S2-S6 are Python sensors"
        )

    def kill_node(self, node_id):
        if node_id in self.nodes:
            self.nodes[node_id].alive = False
            print(f"[DODAG] {node_id} marked offline")

    def revive_node(self, node_id):
        if node_id in self.nodes:
            self.nodes[node_id].alive = True
            print(f"[DODAG] {node_id} rejoined")

    def print_tree(self):
        def show(node_id, depth=0):
            node = self.nodes[node_id]

            status = (
                "online" if node.alive else "offline"
            )

            print(
                " " * depth +
                f"{node_id} | rank={node.rank} | {status}"
            )

            for child in node.children:
                show(child, depth + 4)

        show("root")


class DODAGVisualizer:
    POS = {
        "root": (0.50, 0.95),
        "routerA": (0.17, 0.65),
        "routerB": (0.50, 0.65),
        "routerC": (0.83, 0.65),
        "S1": (0.08, 0.30),
        "S2": (0.26, 0.30),
        "S3": (0.42, 0.30),
        "S4": (0.58, 0.30),
        "S5": (0.74, 0.30),
        "S6": (0.92, 0.30),
    }

    def __init__(self, dodag):
        self.dodag = dodag

    def animate(self):
        try:
            import networkx as nx
            import matplotlib.pyplot as plt
            from matplotlib.animation import FuncAnimation
        except ImportError:
            print(
                "Install visualization: "
                "python -m pip install networkx matplotlib"
            )
            return

        fig, ax = plt.subplots(figsize=(10, 6))

        def draw(frame):
            ax.clear()
            graph = nx.DiGraph()

            for node_id, node in self.dodag.nodes.items():
                graph.add_node(node_id)

                if node.parent:
                    graph.add_edge(node_id, node.parent)

            colors = [
                "#f09595"
                if not self.dodag.nodes[node_id].alive
                else "#888780"
                if self.dodag.nodes[node_id].rank == 1
                else "#afa9ec"
                if self.dodag.nodes[node_id].rank == 2
                else "#9fe1cb"
                for node_id in graph.nodes
            ]

            nx.draw(
                graph,
                self.POS,
                ax=ax,
                with_labels=True,
                node_color=colors,
                node_size=1800,
                arrows=True,
                arrowsize=18
            )

            ax.set_title(
                "Cold Chain DODAG - S1 from Wokwi"
            )

        animation = FuncAnimation(
            fig,
            draw,
            interval=3000,
            cache_frame_data=False
        )

        draw(0)
        plt.show()

        return animation


class AlertEngine:
    def __init__(self):
        self.active = {}

        self.client = make_mqtt_client(
            "alert_engine"
        )

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        connect_client(self.client)

        print(
            "[AlertEngine] Monitoring Wokwi S1 "
            "and Python S2-S6"
        )

    def on_connect(
        self,
        client,
        userdata,
        flags,
        reason_code,
        properties=None
    ):
        if reason_code == 0:
            subscribe_sensor_topics(client)
        else:
            print(
                "[AlertEngine] MQTT connection refused: "
                f"{reason_code}"
            )

    def on_message(self, client, userdata, message):
        try:
            payload = json.loads(message.payload)
        except (ValueError, TypeError):
            return

        if not is_sensor_telemetry(
            message.topic,
            payload
        ):
            return

        zone = payload["zone"]
        node = payload["node"]
        temp = payload["temp"]

        thresholds = THRESHOLDS[zone]
        previous = self.active.get(node)

        if temp > thresholds["hi"]:
            kind = "cooling"

        elif temp < thresholds["lo"]:
            kind = "heating"

        else:
            kind = None

        if kind is None:
            if previous:
                duration = round(
                    time.time() - previous["start"],
                    1
                )

                self.active.pop(node, None)

                self.client.publish(
                    "coldchain/alerts/recover",
                    json.dumps({
                        "node": node,
                        "zone": zone,
                        "temp": temp,
                        "duration_s": duration,
                    }),
                    qos=1
                )

                if node != "S1":
                    self.client.publish(
                        f"coldchain/control/{previous['kind']}/{zone}",
                        f"{previous['kind']}:OFF",
                        qos=1
                    )

                print(
                    f"[RECOVER] {node} ({zone}) safe again"
                )

            return

        if previous and previous["kind"] == kind:
            return

        if (
            previous
            and previous["kind"] != kind
            and node != "S1"
        ):
            self.client.publish(
                f"coldchain/control/{previous['kind']}/{zone}",
                f"{previous['kind']}:OFF",
                qos=1
            )

        self.active[node] = {
            "kind": kind,
            "start": time.time()
        }

        self.client.publish(
            "coldchain/alerts/breach",
            json.dumps({
                "node": node,
                "zone": zone,
                "temp": temp,
                "type": kind.upper(),
                "range": [
                    thresholds["lo"],
                    thresholds["hi"]
                ],
                "optimum": thresholds["opt"],
                "ts": int(time.time()),
            }),
            qos=1
        )

        if node != "S1":
            self.client.publish(
                f"coldchain/control/{kind}/{zone}",
                f"{kind}:ON",
                qos=1
            )

        print(
            f"[ALERT] {node} {zone}: "
            f"{temp} C ({kind})"
        )

    def stop(self):
        self.client.disconnect()
        self.client.loop_stop()


class DataLogger:
    def __init__(self):
        self.log_file = LOG_FILE

        self.log_file.parent.mkdir(
            parents=True,
            exist_ok=True
        )

        if not self.log_file.exists():
            with self.log_file.open(
                "w",
                newline="",
                encoding="utf-8"
            ) as file:
                csv.writer(file).writerow([
                    "timestamp",
                    "node",
                    "zone",
                    "temp",
                    "hum",
                    "event"
                ])

        self.client = make_mqtt_client(
            "logger"
        )

        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message

        connect_client(self.client)

        print(
            f"[DataLogger] Writing readings to "
            f"{self.log_file}"
        )

    def on_connect(
        self,
        client,
        userdata,
        flags,
        reason_code,
        properties=None
    ):
        if reason_code == 0:
            subscribe_sensor_topics(client)

            client.subscribe(
                "coldchain/alerts/#",
                qos=0
            )
        else:
            print(
                "[DataLogger] MQTT connection refused: "
                f"{reason_code}"
            )

    def on_message(self, client, userdata, message):
        try:
            payload = json.loads(message.payload)
        except (ValueError, TypeError):
            return

        if not isinstance(payload, dict):
            return

        is_reading = message.topic.endswith(
            "/temperature"
        )

        if is_reading and not is_sensor_telemetry(
            message.topic,
            payload
        ):
            return

        if (
            not is_reading
            and not message.topic.startswith(
                "coldchain/alerts/"
            )
        ):
            return

        event = (
            "breach"
            if "/breach" in message.topic
            else "recover"
            if "/recover" in message.topic
            else "reading"
        )

        with self.log_file.open(
            "a",
            newline="",
            encoding="utf-8"
        ) as file:
            csv.writer(file).writerow([
                time.strftime("%Y-%m-%d %H:%M:%S"),
                payload.get("node", ""),
                payload.get("zone", ""),
                payload.get("temp", ""),
                payload.get("hum", ""),
                event
            ])

    def stop(self):
        self.client.disconnect()
        self.client.loop_stop()


def main():
    print("=" * 56)
    print("COLD CHAIN - LOCAL MQTT TEMPERATURE REGULATOR")
    print(f"Mosquitto broker: {BROKER}:{PORT}")
    print("Dashboard WebSocket: ws://127.0.0.1:9001/mqtt")
    print(f"Wokwi S1 telemetry: {WOKWI_S1_TEMP_TOPIC}")
    print("Python publishes only S2-S6; S1 is controlled by Wokwi")
    print("=" * 56)

    try:
        with socket.create_connection(
            (BROKER, PORT),
            timeout=3
        ):
            pass

    except OSError:
        raise SystemExit(
            f"Mosquitto is not reachable on {BROKER}:{PORT}. "
            "Start your local broker first."
        )

    dodag = DODAG()
    dodag.print_tree()

    nodes = {}
    alert_engine = None
    logger = None

    try:
        alert_engine = AlertEngine()
        logger = DataLogger()

        for config in SENSOR_CONFIGS:
            node = SensorNode(*config)

            nodes[node.node_id] = node
            node.start()

        print(
            f"[main] Started {len(nodes)} "
            "Python sensors (S2-S6)"
        )

        print(
            "Commands: breach | kill S5 | "
            "revive S5 | tree | plot | quit"
        )

        while True:
            try:
                parts = input("> ").strip().split()
            except EOFError:
                break

            if not parts:
                continue

            command = parts[0].lower()

            if command == "breach":
                if "S6" in nodes:
                    nodes["S6"].base_temp = 14

                    print(
                        "S6 set to 14 C "
                        "to trigger a breach"
                    )
                else:
                    print(
                        "S6 is offline; revive S6 first"
                    )

            elif command in ("kill", "revive"):
                if len(parts) != 2:
                    print(f"Usage: {command} S2")
                    continue

                node_id = parts[1].upper()

                if node_id == "S1":
                    print(
                        "S1 runs in Wokwi; "
                        "manage it there"
                    )
                    continue

                if node_id not in BASE_TEMPS:
                    print(
                        "Unknown sensor; use S2-S6"
                    )
                    continue

                if (
                    command == "kill"
                    and node_id in nodes
                ):
                    old = nodes.pop(node_id)
                    old.kill()
                    old.join(timeout=3)

                    dodag.kill_node(node_id)

                elif (
                    command == "revive"
                    and node_id not in nodes
                ):
                    zone = NODE_ZONES[node_id]
                    cfg = THRESHOLDS[zone]

                    new_node = SensorNode(
                        node_id,
                        zone,
                        BASE_TEMPS[node_id],
                        (cfg["lo"], cfg["hi"]),
                        cfg["opt"]
                    )

                    nodes[node_id] = new_node
                    new_node.start()

                    dodag.revive_node(node_id)

            elif command == "tree":
                dodag.print_tree()

            elif command == "plot":
                DODAGVisualizer(dodag).animate()

            elif command == "quit":
                break

            else:
                print(
                    "Commands: breach | kill S5 | "
                    "revive S5 | tree | plot | quit"
                )

    except KeyboardInterrupt:
        pass

    finally:
        print("Stopping Python sensors...")

        for node in nodes.values():
            node.kill()

        for node in nodes.values():
            node.join(timeout=3)

        if alert_engine is not None:
            alert_engine.stop()

        if logger is not None:
            logger.stop()

        print(
            "Stopped. Wokwi S1 runs independently."
        )


if __name__ == "__main__":
    main()
