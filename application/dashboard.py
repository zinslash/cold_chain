import paho.mqtt.client as mqtt
import json, os, time

class Dashboard:
    def __init__(self, broker="localhost"):
        self.readings = {}
        self.alerts   = []
        self.client   = mqtt.Client(client_id="dashboard")
        self.client.on_message = self._on_msg
        self.client.connect(broker, 1883)
        self.client.subscribe("coldchain/+/+/temperature", qos=0)
        self.client.subscribe("coldchain/alerts/#",        qos=0)
        self.client.subscribe("coldchain/+/+/status",      qos=0)

    def _on_msg(self, client, userdata, msg):
        topic = msg.topic
        try:
            data = json.loads(msg.payload)
        except Exception:
            data = msg.payload.decode()

        if "/temperature" in topic:
            node = data.get("node","?")
            self.readings[node] = data

        elif "/alerts/breach" in topic:
            self.alerts.append(
                f"BREACH  {data.get('zone')} {data.get('node')} "
                f"{data.get('temp')}°C [{data.get('type')}]"
            )
            self.alerts = self.alerts[-5:]   # keep last 5

        elif "/alerts/recover" in topic:
            self.alerts.append(
                f"RECOVER {data.get('zone')} back in range after "
                f"{data.get('duration_s')}s"
            )
            self.alerts = self.alerts[-5:]

    def _print(self):
        os.system("cls" if os.name=="nt" else "clear")
        print("╔══════════════════════════════════════════════════════╗")
        print("║      COLD CHAIN TEMPERATURE REGULATOR — LIVE        ║")
        print("╠══════════╦══════════╦══════╦════════╦═══════════════╣")
        print("║  Node    ║  Zone    ║ Temp ║  Hum   ║  Status       ║")
        print("╠══════════╬══════════╬══════╬════════╬═══════════════╣")
        zones = {"S1":"A Freezer","S2":"A Freezer",
                 "S3":"B Chiller","S4":"B Chiller",
                 "S5":"C Dock",   "S6":"C Dock"}
        safe  = {"zoneA":(-25,-15),"zoneB":(0,5),"zoneC":(2,8)}
        for node in ["S1","S2","S3","S4","S5","S6"]:
            d  = self.readings.get(node, {})
            t  = d.get("temp","--")
            h  = d.get("hum", "--")
            z  = d.get("zone","")
            lo,hi = safe.get(z,(0,100))
            if t == "--":
                st = "offline"
            elif isinstance(t,float) and (thi):
                st = "!! BREACH"
            else:
                st = "ok"
            print(f"║  {node:<8}║  {zones.get(node,''):<8}║ {str(t):<4} ║  {str(h):<5} ║  {st:<13}║")
        print("╠══════════╩══════════╩══════╩════════╩═══════════════╣")
        print("║  ALERTS                                               ║")
        for a in (self.alerts or ["none"]):
            print(f"║  {a[:52]:<52}║")
        print("╚══════════════════════════════════════════════════════╝")

    def start(self):
        self.client.loop_start()
        while True:
            self._print()
            time.sleep(5)