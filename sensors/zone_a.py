
from sensors.base_node import SensorNode


def create_zone_a_nodes(broker="broker.hivemq.com"):

    s2 = SensorNode(
        "S2",
        "zoneA",
        base_temp=-20,
        temp_range=(-25, -15),
        optimum=-18,
        broker=broker
    )

    return [s2]
