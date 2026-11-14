# ================================================================
# COLD CHAIN TEMPERATURE REGULATOR
# Single-file version — no imports needed
# Run: python main.py
# ================================================================

import threading
import time
import json
import random
import os
import csv

try:
    import paho.mqtt.client as mqtt
except ImportError:
    print("ERROR: paho-mqtt not installed.")
    print("Run this in your terminal: pip install paho-mqtt")
    exit(1)

try:
    import networkx as nx
    import matplotlib
    matplotlib.use('TkAgg')
    import matplotlib.pyplot as plt
    import matplotlib.animation as animation
    HAS_PLOT = True
except ImportError:
    HAS_PLOT = False
    print("[WARNING] matplotlib/networkx not installed — DODAG graph disabled")
    print("Run: pip install networkx matplotlib")

# ================================================================
# CONFIGURATION
# ================================================================
BROKER = "broker.hivemq.com"
PORT   = 1883

THRESHOLDS = {
    "zoneA": {"lo": -25, "hi": -15, "opt": -18},
    "zoneB": {"lo":   0, "hi":   5, "opt":  2},
    "zoneC": {"lo":   2, "hi":   8, "opt":  5},
}

# ================================================================
# SENSOR NODE
# ================================================================
class SensorNode(threading.Thread):
    def __init__(self, node_id, zone, base_temp, temp_range, optimum):
        super().__init__(daemon=True)
        self.node_id    = node_id
        self.zone       = zone
        self.base_temp  = base_temp
        self.temp_range = temp_range
        self.optimum    = optimum
        self.cooling    = False
        self.heating    = False
        self.alive      = True
        self.drift_dir  = 1
        self._setup_mqtt()

    def _setup_mqtt(self):
        self.client = mqtt.Client(client_id=f"sensor_{self.node_id}_{random.randint(1000,9999)}")
        self.client.will_set(
            f"coldchain/{self.zone}/{self.node_id}/status",
            payload="offline", qos=1, retain=True
        )
        self.client.on_message = self._on_control
        try:
            self.client.connect(BROKER, PORT, keepalive=60)
            self.client.subscribe(f"coldchain/control/+/{self.zone}", qos=2)
            self.client.loop_start()
        except Exception as e:
            print(f"[{self.node_id}] MQTT connect error: {e}")

    def _on_control(self, client, userdata, msg):
        cmd = msg.payload.decode()
        if   "cooling:ON"  in cmd: self.cooling = True;  self.heating = False
        elif "heating:ON"  in cmd: self.heating = True;  self.cooling = False
        elif "cooling:OFF" in cmd: self.cooling = False
        elif "heating:OFF" in cmd: self.heating = False

    def run(self):
        try:
            self.client.publish(
                f"coldchain/{self.zone}/{self.node_id}/status",
                "online", retain=True
            )
        except Exception:
            pass

        while self.alive:
            try:
                if self.cooling:
                    self.base_temp -= 0.3 + random.uniform(0, 0.05)
                    if self.base_temp <= self.optimum:
                        self.cooling = False
                        self.client.publish(
                            f"coldchain/control/cooling/{self.zone}",
                            "cooling:OFF", qos=2
                        )
                elif self.heating:
                    self.base_temp += 0.3 + random.uniform(0, 0.05)
                    if self.base_temp >= self.optimum:
                        self.heating = False
                        self.client.publish(
                            f"coldchain/control/heating/{self.zone}",
                            "heating:OFF", qos=2
                        )
                else:
                    self.base_temp += 0.12 * self.drift_dir + random.uniform(-0.05, 0.08)

                temp    = round(self.base_temp + random.uniform(-0.3, 0.3), 2)
                hum     = round(60 + random.uniform(-3, 3), 2)
                payload = json.dumps({
                    "node": self.node_id,
                    "zone": self.zone,
                    "temp": temp,
                    "hum":  hum,
                    "ts":   int(time.time())
                })
                self.client.publish(
                    f"coldchain/{self.zone}/{self.node_id}/temperature",
                    payload, qos=0
                )
                print(f"  [{self.node_id}] {temp:>7.2f}°C  {hum:.1f}% RH")
            except Exception as e:
                print(f"[{self.node_id}] error: {e}")
            time.sleep(5)

    def kill(self):
        self.alive = False
        try:
            self.client.publish(
                f"coldchain/{self.zone}/{self.node_id}/status",
                "offline", retain=True
            )
        except Exception:
            pass

# ================================================================
# DODAG
# ================================================================
class DODAGNode:
    def __init__(self, node_id, rank, parent=None):
        self.node_id  = node_id
        self.rank     = rank
        self.parent   = parent
        self.children = []
        self.alive    = True

class DODAG:
    def __init__(self):
        self.nodes = {
            "root":    DODAGNode("root",    rank=1),
            "routerA": DODAGNode("routerA", rank=2, parent="root"),
            "routerB": DODAGNode("routerB", rank=2, parent="root"),
            "routerC": DODAGNode("routerC", rank=2, parent="root"),
            "S1": DODAGNode("S1", rank=3, parent="routerA"),
            "S2": DODAGNode("S2", rank=3, parent="routerA"),
            "S3": DODAGNode("S3", rank=3, parent="routerB"),
            "S4": DODAGNode("S4", rank=3, parent="routerB"),
            "S5": DODAGNode("S5", rank=3, parent="routerC"),
            "S6": DODAGNode("S6", rank=3, parent="routerC"),
        }
        for nid, node in self.nodes.items():
            if node.parent and node.parent in self.nodes:
                self.nodes[node.parent].children.append(nid)
        print("[DODAG] DIO broadcast — DODAG tree formed")
        print("[DODAG] All nodes sent DAO — routes registered\n")

    def kill_node(self, node_id):
        if node_id in self.nodes:
            self.nodes[node_id].alive = False
            parent_id = self.nodes[node_id].parent
            for nid, node in self.nodes.items():
                if node.parent == node_id and node.alive:
                    node.parent = parent_id
                    print(f"[DODAG] {nid} re-parented to {parent_id}")
            print(f"[DODAG] {node_id} removed — tree healed")

    def revive_node(self, node_id):
        if node_id in self.nodes:
            self.nodes[node_id].alive = True
            print(f"[DODAG] {node_id} sent DIS — rejoined DODAG")

    def print_tree(self):
        print("\n── DODAG tree ──")
        def _show(nid, indent=0):
            n = self.nodes[nid]
            st = "alive" if n.alive else "DEAD"
            print(" " * indent + f"[{nid}] rank={n.rank} ({st})")
            for child in n.children:
                if child in self.nodes:
                    _show(child, indent + 4)
        _show("root")
        print()

# ================================================================
# DODAG VISUALIZER
# ================================================================
class DODAGVisualizer:
    def __init__(self, dodag):
        self.dodag = dodag

    def _build(self):
        G = nx.DiGraph()
        for nid, node in self.dodag.nodes.items():
            G.add_node(nid, alive=node.alive, rank=node.rank)
            if node.parent:
                G.add_edge(nid, node.parent)
        return G

    def _colors(self, G):
        colors = []
        for nid in G.nodes():
            node = self.dodag.nodes[nid]
            if not node.alive:          colors.append("#F09595")
            elif node.rank == 1:        colors.append("#888780")
            elif node.rank == 2:        colors.append("#AFA9EC")
            else:                       colors.append("#9FE1CB")
        return colors

    POS = {
        "root":    (0.5,  0.95),
        "routerA": (0.17, 0.65),
        "routerB": (0.5,  0.65),
        "routerC": (0.83, 0.65),
        "S1": (0.08, 0.3), "S2": (0.26, 0.3),
        "S3": (0.42, 0.3), "S4": (0.58, 0.3),
        "S5": (0.74, 0.3), "S6": (0.92, 0.3),
    }

    def animate(self, interval_ms=3000):
        fig, ax = plt.subplots(figsize=(10, 6))
        fig.suptitle("Cold Chain — DODAG Live Topology", fontsize=13)
        def update(frame):
            ax.clear()
            G = self._build()
            nx.draw(G, self.POS, ax=ax,
                    with_labels=True,
                    node_color=self._colors(G),
                    node_size=1800, font_size=9,
                    arrows=True, arrowsize=18,
                    edge_color="#D3D1C7")
        ani = animation.FuncAnimation(fig, update, interval=interval_ms)
        plt.tight_layout()
        plt.show()

# ================================================================
# ALERT ENGINE
# ================================================================
class AlertEngine:
    def __init__(self):
        self.active = {}
        self.client = mqtt.Client(client_id=f"alert_engine_{random.randint(1000,9999)}")
        self.client.on_message = self._on_message
        self.client.connect(BROKER, PORT, keepalive=60)
        self.client.subscribe("coldchain/+/+/temperature", qos=0)
        print("[AlertEngine] Started — watching all zones")

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except Exception:
            return
        zone = data.get("zone")
        temp = data.get("temp")
        node = data.get("node")
        if zone not in THRESHOLDS:
            return
        cfg = THRESHOLDS[zone]
        lo, hi = cfg["lo"], cfg["hi"]
        was_breach = zone in self.active

        if temp > hi:
            self._trigger("cooling", zone, node, temp, cfg)
        elif temp < lo:
            self._trigger("heating", zone, node, temp, cfg)
        elif was_breach:
            self._resolve(zone, temp)

    def _trigger(self, kind, zone, node, temp, cfg):
        if zone not in self.active:
            alert = json.dumps({
                "node": node, "zone": zone,
                "temp": temp, "type": kind.upper(),
                "range": [cfg["lo"], cfg["hi"]],
                "optimum": cfg["opt"],
                "ts": int(time.time())
            })
            self.client.publish("coldchain/alerts/breach", alert, qos=1)
            self.client.publish(
                f"coldchain/control/{kind}/{zone}",
                f"{kind}:ON", qos=2
            )
            self.active[zone] = {"type": kind, "start": time.time()}
            print(f"\n  [ALERT] {zone} {kind.upper()} BREACH — {node}: {temp}°C"
                  f" (safe: {cfg['lo']} to {cfg['hi']}°C)\n")

    def _resolve(self, zone, temp):
        action = self.active.pop(zone, {})
        kind   = action.get("type", "cooling")
        dur    = round(time.time() - action.get("start", time.time()), 1)
        self.client.publish(
            f"coldchain/control/{kind}/{zone}",
            f"{kind}:OFF", qos=2
        )
        print(f"\n  [RECOVER] {zone} back in range after {dur}s — {kind} OFF\n")

    def start(self):
        self.client.loop_forever()

# ================================================================
# DATA LOGGER
# ================================================================
class DataLogger:
    def __init__(self):
        os.makedirs("logs", exist_ok=True)
        self.log_file = "logs/readings.csv"
        if not os.path.exists(self.log_file):
            with open(self.log_file, "w", newline="") as f:
                csv.writer(f).writerow(
                    ["timestamp","node","zone","temp","hum","event"]
                )
        self.client = mqtt.Client(client_id=f"logger_{random.randint(1000,9999)}")
        self.client.on_message = self._on_msg
        self.client.connect(BROKER, PORT, keepalive=60)
        self.client.subscribe("coldchain/+/+/temperature", qos=0)
        self.client.subscribe("coldchain/alerts/#",        qos=0)
        print("[DataLogger] Started — logging to logs/readings.csv")

    def _on_msg(self, client, userdata, msg):
        try:
            data  = json.loads(msg.payload)
        except Exception:
            return
        event = "breach"  if "breach"  in msg.topic else \
                "recover" if "recover" in msg.topic else "reading"
        row = [
            time.strftime("%Y-%m-%d %H:%M:%S"),
            data.get("node",""),
            data.get("zone",""),
            data.get("temp",""),
            data.get("hum",""),
            event
        ]
        with open(self.log_file, "a", newline="") as f:
            csv.writer(f).writerow(row)

    def start(self):
        self.client.loop_forever()

# ================================================================
# MAIN
# ================================================================
if __name__ == "__main__":
    print("=" * 50)
    print("  COLD CHAIN TEMPERATURE REGULATOR")
    print(f"  Broker: {BROKER}:{PORT}")
    print("=" * 50)
    print()

    # Build DODAG
    dodag = DODAG()
    dodag.print_tree()

    # Start sensor nodes
    node_configs = [
        ("S1", "zoneA", -18, (-25,-15), -18),
        ("S2", "zoneA", -20, (-25,-15), -18),
        ("S3", "zoneB",   2, (  0,  5),   2),
        ("S4", "zoneB",   4, (  0,  5),   2),
        ("S5", "zoneC",   6, (  2,  8),   5),
        ("S6", "zoneC",   6, (  2,  8),   5),
    ]
    nodes_dict = {}
    for cfg in node_configs:
        n = SensorNode(*cfg)
        nodes_dict[n.node_id] = n
        n.start()
    print(f"[main] 6 sensor nodes started\n")

    # Start application layer
    alert  = AlertEngine()
    logger = DataLogger()
    threading.Thread(target=alert.start,  daemon=True).start()
    threading.Thread(target=logger.start, daemon=True).start()

    # Start DODAG visualizer
    if HAS_PLOT:
        viz = DODAGVisualizer(dodag)
        threading.Thread(target=viz.animate, daemon=True).start()

    # Command loop
    print()
    print("Commands: breach | kill S5 | revive S5 | tree | quit")
    print()

    BASE_TEMPS = {"S1":-18,"S2":-20,"S3":2,"S4":4,"S5":6,"S6":6}

    while True:
        try:
            cmd = input("> ").strip().lower()
        except (EOFError, KeyboardInterrupt):
            break

        if cmd == "breach":
            nodes_dict["S6"].base_temp = 14
            print("  S6 set to +14°C — breach fires in 5s")

        elif cmd.startswith("kill "):
            nid = cmd.split()[1].upper()
            if nid in nodes_dict:
                nodes_dict[nid].kill()
                dodag.kill_node(nid)
            else:
                print(f"  Node {nid} not found")

        elif cmd.startswith("revive "):
            nid = cmd.split()[1].upper()
            if nid in nodes_dict:
                nodes_dict[nid].alive     = True
                nodes_dict[nid].base_temp = BASE_TEMPS.get(nid, 6)
                new_node = SensorNode(
                    nid,
                    nodes_dict[nid].zone,
                    BASE_TEMPS.get(nid, 6),
                    nodes_dict[nid].temp_range,
                    nodes_dict[nid].optimum
                )
                nodes_dict[nid] = new_node
                new_node.start()
                dodag.revive_node(nid)
            else:
                print(f"  Node {nid} not found")

        elif cmd == "tree":
            dodag.print_tree()

        elif cmd == "quit":
            print("Shutting down.")
            break