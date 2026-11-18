
from sensors.base_node import SensorNode


def create_zone_b_nodes(broker="127.0.0.1"):
    s3 = SensorNode(
        "S3",
        "zoneB",
        base_temp=2,
        temp_range=(0, 5),
        optimum=2,
        broker=broker
    )

    s4 = SensorNode(
        "S4",
        "zoneB",
        base_temp=4,
        temp_range=(0, 5),
        optimum=2,
        broker=broker
    )

    return [s3, s4]
