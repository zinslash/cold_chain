# sensors/zone_a.py  — FREEZER  (−25°C to −15°C, optimum −18°C)
from sensors.base_node import SensorNode

def create_zone_a_nodes(broker="localhost"):
    s1 = SensorNode("S1","zoneA", base_temp=-18,
                    temp_range=(-25,-15), optimum=-18, broker=broker)
    s2 = SensorNode("S2","zoneA", base_temp=-20,
                    temp_range=(-25,-15), optimum=-18, broker=broker)
    return [s1, s2]