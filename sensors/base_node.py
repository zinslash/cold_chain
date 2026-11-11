import paho.mqtt.client as mqtt
import threading, time, json, random

class SensorNode(threading.Thread):
    def __init__(self, node_id, zone, base_temp,
                 temp_range, optimum, broker="localhost"):
        super().__init__(daemon=True)
        self.node_id   = node_id
        self.zone      = zone
        self.base_temp = base_temp
        self.temp_range = temp_range   # (lo, hi)
        self.optimum   = optimum
        self.cooling   = False
        self.heating   = False
        self.alive     = True
        self.drift_dir = 1

        self.client = mqtt.Client(client_id=node_id)
        self.client.will_set(
            f"coldchain/{zone}/{node_id}/status",
            payload="offline", qos=1, retain=True
        )
        self.client.on_message = self._on_control
        self.client.connect(broker, 1883)
        self.client.subscribe(
            f"coldchain/control/+/{zone}", qos=2
        )
        self.client.loop_start()

    def _on_control(self, client, userdata, msg):
        cmd = msg.payload.decode()
        if   "cooling:ON"  in cmd: self.cooling = True;  self.heating = False
        elif "heating:ON"  in cmd: self.heating = True;  self.cooling = False
        elif "cooling:OFF" in cmd: self.cooling = False
        elif "heating:OFF" in cmd: self.heating = False

    def run(self):
        self.client.publish(
            f"coldchain/{self.zone}/{self.node_id}/status",
            "online", retain=True
        )
        while self.alive:
            # drift base temp
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
                self.base_temp += (
                    0.12 * self.drift_dir
                    + random.uniform(-0.05, 0.08)
                )

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
            time.sleep(5)

    def kill(self):
        self.alive = False