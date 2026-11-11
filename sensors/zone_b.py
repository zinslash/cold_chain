# sensors/zone_b.py  — CHILLER  (0°C to 5°C, optimum +2°C)
from sensors.base_node import SensorNode

def create_zone_b_nodes(broker="localhost"):
    s3 = SensorNode("S3","zoneB", base_temp=2,
                    temp_range=(0,5), optimum=2, broker=broker)
    s4 = SensorNode("S4","zoneB", base_temp=4,
                    temp_range=(0,5), optimum=2, broker=broker)
    return [s3, s4]