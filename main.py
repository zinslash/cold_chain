
import csv
import json
import math
import os
import random
import threading
import time
import uuid

try:
    import paho.mqtt.client as mqtt
except ImportError:
    raise SystemExit("Install MQTT: python -m pip install paho-mqtt")


BROKER = os.getenv(
    "COLDCHAIN_MQTT_HOST",
    "broker.hivemq.com"
)

PORT = int(
    os.getenv("COLDCHAIN_MQTT_PORT", "1883")
)

WOKWI_S1_ROOT = os.getenv(
    "COLDCHAIN_WOKWI_S1_ROOT",
    "coldchain/zoneA/S1"
).strip("/")

WOKWI_S1_TEMP_TOPIC = (
    f"{WOKWI_S1_ROOT}/temperature"
)

WOKWI_S1_STATUS_TOPIC = (
    f"{WOKWI_S1_ROOT}/status"
)

SIMULATED_TEMP_FILTER = (
    "coldchain/+/+/temperature"
)

THRESHOLDS = {
    "zoneA": {
        "lo": -25,
        "hi": -15,
        "opt": -18
    },
    "zoneB": {
        "lo": 0,
        "hi": 5,
        "opt": 2
    },
    "zoneC": {
        "lo": 2,
        "hi": 8,
        "opt": 5
    }
}

NODE_ZONES = {
    "S1": "zoneA",
    "S2": "zoneA",
    "S3": "zoneB",
    "S4": "zoneB",
    "S5": "zoneC",
    "S6": "zoneC"
}

BASE_TEMPS = {
    "S2": -20,
    "S3": 2,
    "S4": 4,
    "S5": 6,
    "S6": 6
}


def make_mqtt_client(label):

    client_id = (
        f"{label}_{uuid.uuid4().hex[:10]}"
    )

    if hasattr(mqtt, "CallbackAPIVersion"):
        return mqtt.Client(
            mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id
        )

    return mqtt.Client(
        client_id=client_id
    )


def is_sensor_telemetry(topic, data):

    if not isinstance(data, dict):
        return False

    nid = data.get("node")
    zone = data.get("zone")
    temp = data.get("temp")

    if (
        nid not in NODE_ZONES
        or zone != NODE_ZONES[nid]
    ):
        return False

    if (
        isinstance(temp, bool)
        or not isinstance(temp, (int, float))
        or not math.isfinite(temp)
    ):
        return False

    if nid == "S1":
        return (
            topic == WOKWI_S1_TEMP_TOPIC
            and data.get("controller") is True
        )

    return (
        topic ==
        f"coldchain/{zone}/{nid}/temperature"
    )


def subscribe_sensor_topics(client):

    client.subscribe(
        SIMULATED_TEMP_FILTER,
        qos=0
    )

    if WOKWI_S1_TEMP_TOPIC != (
        "coldchain/zoneA/S1/temperature"
    ):
        client.subscribe(
            WOKWI_S1_TEMP_TOPIC,
            qos=0
        )


class SensorNode(threading.Thread):

    def __init__(
        self,
        node_id,
        zone,
        base_temp,
        temp_range,
        optimum
    ):

        super().__init__(daemon=True)

        self.node_id = node_id
        self.zone = zone
        self.base_temp = float(base_temp)
        self.temp_range = temp_range
        self.optimum = optimum

        self.cooling = False
        self.heating = False
        self.running = True

        self.client = make_mqtt_client(
            f"sensor_{node_id}"
        )

        self.status_topic = (
            f"coldchain/{zone}/{node_id}/status"
        )

        self.client.will_set(
            self.status_topic,
            "offline",
            qos=1,
            retain=True
        )

        self.client.on_message = self.on_control

        self.client.connect(
            BROKER,
            PORT,
            keepalive=60
        )

        self.client.subscribe(
            f"coldchain/control/+/{zone}",
            qos=1
        )

        self.client.loop_start()

    def on_control(
        self,
        client,
        userdata,
        message
    ):

        command = message.payload.decode(
            "utf-8",
            errors="replace"
        )

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

        self.client.publish(
            self.status_topic,
            "online",
            qos=1,
            retain=True
        )

        try:

            while self.running:

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
                        0.12 +
                        random.uniform(-0.05, 0.08)
                    )

                temp = round(
                    self.base_temp +
                    random.uniform(-0.3, 0.3),
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
                    "ts": int(time.time())
                })

                self.client.publish(
                    f"coldchain/{self.zone}/{self.node_id}/temperature",
                    payload,
                    qos=0
                )

                print(
                    f"[{self.node_id}] "
                    f"{temp:.2f} C | "
                    f"Humidity {humidity:.1f}%"
                )

                for _ in range(50):

                    if not self.running:
                        break

                    time.sleep(0.1)

        finally:

            self.client.publish(
                self.status_topic,
                "offline",
                qos=1,
                retain=True
            )

            self.client.disconnect()
            self.client.loop_stop()

    def kill(self):
        self.running = False


class DODAGNode:

    def __init__(
        self,
        node_id,
        rank,
        parent=None
    ):

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
            "S6": (3, "routerC")
        }

        self.nodes = {
            nid: DODAGNode(nid, rank, parent)
            for nid, (rank, parent) in specs.items()
        }

        for nid, node in self.nodes.items():

            if node.parent:

                self.nodes[
                    node.parent
                ].children.append(nid)

        print(
            "[DODAG] Topology created; "
            "S1 is the external Wokwi node"
        )

    def kill_node(self, nid):

        if nid in self.nodes:

            self.nodes[nid].alive = False

            print(
                f"[DODAG] {nid} marked offline"
            )

    def revive_node(self, nid):

        if nid in self.nodes:

            self.nodes[nid].alive = True

            print(
                f"[DODAG] {nid} rejoined"
            )

    def print_tree(self):

        def show(nid, depth=0):

            node = self.nodes[nid]

            status = (
                "online"
                if node.alive
                else "offline"
            )

            print(
                " " * depth +
                f"{nid} | rank={node.rank} | {status}"
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
        "S6": (0.92, 0.30)
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
                "python -m pip install "
                "networkx matplotlib"
            )
            return

        fig, ax = plt.subplots(
            figsize=(10, 6)
        )

        def draw(frame):

            ax.clear()

            graph = nx.DiGraph()

            for nid, node in self.dodag.nodes.items():

                graph.add_node(nid)

                if node.parent:
                    graph.add_edge(
                        nid,
                        node.parent
                    )

            colors = [
                "#f09595"
                if not self.dodag.nodes[n].alive
                else "#888780"
                if self.dodag.nodes[n].rank == 1
                else "#afa9ec"
                if self.dodag.nodes[n].rank == 2
                else "#9fe1cb"
                for n in graph.nodes
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
                "Cold Chain DODAG — S1 from Wokwi"
            )

        animation_handle = FuncAnimation(
            fig,
            draw,
            interval=3000,
            cache_frame_data=False
        )

        draw(0)
        plt.show()

        return animation_handle


class AlertEngine:

    def __init__(self):

        self.active = {}

        self.client = make_mqtt_client(
            "alert_engine"
        )

        self.client.on_message = self.on_message

        self.client.connect(
            BROKER,
            PORT,
            keepalive=60
        )

        subscribe_sensor_topics(
            self.client
        )

        print(
            "[AlertEngine] Monitoring "
            "S1 (Wokwi) and S2-S6 (Python)"
        )

    def on_message(
        self,
        client,
        userdata,
        msg
    ):

        try:
            data = json.loads(msg.payload)

        except (ValueError, TypeError):
            return

        if not is_sensor_telemetry(
            msg.topic,
            data
        ):
            return

        zone = data["zone"]
        nid = data["node"]
        temp = data["temp"]

        cfg = THRESHOLDS[zone]

        previous = self.active.get(nid)

        if temp > cfg["hi"]:
            kind = "cooling"

        elif temp < cfg["lo"]:
            kind = "heating"

        else:
            kind = None

        if kind is None:

            if previous:

                duration = round(
                    time.time() - previous["start"],
                    1
                )

                self.active.pop(nid, None)

                self.client.publish(
                    "coldchain/alerts/recover",
                    json.dumps({
                        "node": nid,
                        "zone": zone,
                        "temp": temp,
                        "duration_s": duration
                    }),
                    qos=1
                )

                if nid != "S1":

                    self.client.publish(
                        f"coldchain/control/{previous['kind']}/{zone}",
                        f"{previous['kind']}:OFF",
                        qos=1
                    )

                print(
                    f"[RECOVER] {nid} "
                    f"({zone}) safe again"
                )

            return

        if (
            previous
            and previous["kind"] == kind
        ):
            return

        if (
            previous
            and previous["kind"] != kind
            and nid != "S1"
        ):

            self.client.publish(
                f"coldchain/control/{previous['kind']}/{zone}",
                f"{previous['kind']}:OFF",
                qos=1
            )

        self.active[nid] = {
            "kind": kind,
            "start": time.time()
        }

        self.client.publish(
            "coldchain/alerts/breach",
            json.dumps({
                "node": nid,
                "zone": zone,
                "temp": temp,
                "type": kind.upper(),
                "range": [
                    cfg["lo"],
                    cfg["hi"]
                ],
                "optimum": cfg["opt"],
                "ts": int(time.time())
            }),
            qos=1
        )

        if nid != "S1":

            self.client.publish(
                f"coldchain/control/{kind}/{zone}",
                f"{kind}:ON",
                qos=1
            )

        print(
            f"[ALERT] {nid} {zone}: "
            f"{temp} C ({kind})"
        )

    def start(self):
        self.client.loop_forever()


class DataLogger:

    def __init__(self):

        os.makedirs(
            "logs",
            exist_ok=True
        )

        self.log_file = "logs/readings.csv"

        if not os.path.exists(self.log_file):

            with open(
                self.log_file,
                "w",
                newline="",
                encoding="utf-8"
            ) as f:

                csv.writer(f).writerow([
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

        self.client.on_message = self.on_message

        self.client.connect(
            BROKER,
            PORT,
            keepalive=60
        )

        subscribe_sensor_topics(
            self.client
        )

        self.client.subscribe(
            "coldchain/alerts/#",
            qos=0
        )

        print(
            "[DataLogger] Logging "
            "S1-S6 data to logs/readings.csv"
        )

    def on_message(
        self,
        client,
        userdata,
        msg
    ):

        try:
            data = json.loads(msg.payload)

        except (ValueError, TypeError):
            return

        if not isinstance(data, dict):
            return

        is_reading = msg.topic.endswith(
            "/temperature"
        )

        if is_reading and not is_sensor_telemetry(
            msg.topic,
            data
        ):
            return

        if (
            not is_reading
            and not msg.topic.startswith(
                "coldchain/alerts/"
            )
        ):
            return

        event = (
            "breach"
            if "/breach" in msg.topic
            else "recover"
            if "/recover" in msg.topic
            else "reading"
        )

        with open(
            self.log_file,
            "a",
            newline="",
            encoding="utf-8"
        ) as f:

            csv.writer(f).writerow([
                time.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                data.get("node", ""),
                data.get("zone", ""),
                data.get("temp", ""),
                data.get("hum", ""),
                event
            ])

    def start(self):
        self.client.loop_forever()


def main():

    print("=" * 56)
    print("COLD CHAIN TEMPERATURE REGULATOR")

    print(
        f"MQTT broker: {BROKER}:{PORT}"
    )

    print(
        f"Wokwi S1 topic: "
        f"{WOKWI_S1_TEMP_TOPIC}"
    )

    print(
        "S1 comes from Wokwi; "
        "Python simulates S2-S6"
    )

    print("=" * 56)

    dodag = DODAG()
    dodag.print_tree()

    configs = [
        ("S2", "zoneA", -20, (-25, -15), -18),
        ("S3", "zoneB", 2, (0, 5), 2),
        ("S4", "zoneB", 4, (0, 5), 2),
        ("S5", "zoneC", 6, (2, 8), 5),
        ("S6", "zoneC", 6, (2, 8), 5)
    ]

    nodes = {}

    for args in configs:

        node = SensorNode(*args)

        nodes[node.node_id] = node

        node.start()

    print(
        f"[main] Started "
        f"{len(nodes)} Python sensor nodes"
    )

    alert_engine = AlertEngine()
    logger = DataLogger()

    threading.Thread(
        target=alert_engine.start,
        daemon=True
    ).start()

    threading.Thread(
        target=logger.start,
        daemon=True
    ).start()

    print(
        "Commands: breach | kill S5 | "
        "revive S5 | tree | plot | quit"
    )

    try:

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
                        "S6 moved to 14 C "
                        "to trigger a breach"
                    )

            elif command in ("kill", "revive"):

                if len(parts) != 2:

                    print(
                        f"Usage: {command} S2"
                    )
                    continue

                nid = parts[1].upper()

                if nid == "S1":

                    print(
                        "S1 is controlled by Wokwi; "
                        "manage it in Wokwi"
                    )
                    continue

                if nid not in BASE_TEMPS:

                    print(
                        "Unknown sensor; use S2-S6"
                    )
                    continue

                if command == "kill":

                    if nid in nodes:

                        nodes.pop(nid).kill()
                        dodag.kill_node(nid)

                elif nid not in nodes:

                    zone = NODE_ZONES[nid]
                    cfg = THRESHOLDS[zone]

                    new_node = SensorNode(
                        nid,
                        zone,
                        BASE_TEMPS[nid],
                        (
                            cfg["lo"],
                            cfg["hi"]
                        ),
                        cfg["opt"]
                    )

                    nodes[nid] = new_node

                    new_node.start()

                    dodag.revive_node(nid)

            elif command == "tree":

                dodag.print_tree()

            elif command == "plot":

                DODAGVisualizer(
                    dodag
                ).animate()

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

        print(
            "Stopping Python sensor nodes..."
        )

        for node in nodes.values():
            node.kill()

        for node in nodes.values():
            node.join(timeout=2)

        print(
            "Stopped. Wokwi S1 runs independently."
        )


if __name__ == "__main__":
    main()
