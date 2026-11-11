import paho.mqtt.client as mqtt
import json, time

THRESHOLDS = {
    "zoneA": {"lo": -25, "hi": -15, "opt": -18},
    "zoneB": {"lo":   0, "hi":   5, "opt":   2},
    "zoneC": {"lo":   2, "hi":   8, "opt":   5},
}

class AlertEngine:
    def __init__(self, broker="localhost"):
        self.active = {}   # zones currently in breach
        self.client = mqtt.Client(client_id="alert_engine")
        self.client.on_message = self._on_message
        self.client.connect(broker, 1883)
        self.client.subscribe("coldchain/+/+/temperature", qos=0)
        print("[AlertEngine] Subscribed to all temperature topics")

    def _on_message(self, client, userdata, msg):
        try:
            data = json.loads(msg.payload)
        except Exception:
            return

        zone  = data.get("zone")
        temp  = data.get("temp")
        node  = data.get("node")
        if zone not in THRESHOLDS:
            return

        cfg = THRESHOLDS[zone]
        lo, hi = cfg["lo"], cfg["hi"]
        was_breach = zone in self.active

        if temp > hi:                        # TOO HOT
            self._trigger("cooling", zone, node, temp, cfg)
        elif temp < lo:                      # TOO COLD
            self._trigger("heating", zone, node, temp, cfg)
        elif was_breach:                     # RECOVERED
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
            print(f"[ALERT] {zone} {kind.upper()} breach — {node}: {temp}°C")

    def _resolve(self, zone, temp):
        action = self.active.pop(zone, {})
        kind   = action.get("type","cooling")
        dur    = round(time.time() - action.get("start", time.time()), 1)
        self.client.publish(
            f"coldchain/control/{kind}/{zone}",
            f"{kind}:OFF", qos=2
        )
        recover = json.dumps({"zone": zone, "temp": temp,
                              "duration_s": dur, "ts": int(time.time())})
        self.client.publish("coldchain/alerts/recover", recover, qos=1)
        print(f"[RECOVER] {zone} back in range after {dur}s — {kind} OFF")

    def start(self):
        self.client.loop_forever()