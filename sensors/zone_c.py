
from sensors.base_node import SensorNode


def create_zone_c_nodes(broker="127.0.0.1"):
    s5 = SensorNode(
        "S5",
        "zoneC",
        base_temp=6,
        temp_range=(2, 8),
        optimum=5,
        broker=broker
    )

    s6 = SensorNode(
        "S6",
        "zoneC",
        base_temp=6,
        temp_range=(2, 8),
        optimum=5,
        broker=broker
    )

    return [s5, s6]
