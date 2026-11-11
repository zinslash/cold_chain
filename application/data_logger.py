import threading, time
from sensors.zone_a import create_zone_a_nodes
from sensors.zone_b import create_zone_b_nodes
from sensors.zone_c import create_zone_c_nodes
from rpl.dodag       import DODAG
from rpl.visualizer  import DODAGVisualizer
from application.alert_engine import AlertEngine
from application.data_logger  import DataLogger
from application.dashboard    import Dashboard

BROKER = "localhost"

if __name__ == "__main__":
    print("=== Cold Chain Temperature Regulator ===")
    print("Make sure mosquitto -c mosquitto.conf -v is running!")
    print()

    # 1. Build DODAG
    dodag = DODAG()
    dodag.print_tree()

    # 2. Start sensor nodes
    all_nodes = (
        create_zone_a_nodes(BROKER) +
        create_zone_b_nodes(BROKER) +
        create_zone_c_nodes(BROKER)
    )
    nodes_dict = {n.node_id: n for n in all_nodes}
    for n in all_nodes:
        n.start()
    print(f"[main] {len(all_nodes)} sensor nodes started")

    # 3. Start application layer
    alert   = AlertEngine(BROKER)
    logger  = DataLogger(BROKER)
    dash    = Dashboard(BROKER)
    threading.Thread(target=alert.start,  daemon=True).start()
    threading.Thread(target=logger.start, daemon=True).start()
    threading.Thread(target=dash.start,   daemon=True).start()

    # 4. DODAG visualizer in background thread
    viz = DODAGVisualizer(dodag)
    threading.Thread(target=viz.animate, daemon=True).start()

    # 5. Demo command loop
    print("\nDemo commands:")
    print("  breach  → trigger S6 to +14°C")
    print("  kill S5 → kill node S5 (LWT demo)")
    print("  revive S5 → revive node S5")
    print("  tree    → print DODAG tree")
    print("  quit    → exit")
    print()

    while True:
        cmd = input("> ").strip().lower()
        if cmd == "breach":
            nodes_dict["S6"].base_temp = 14
            print("[Demo] S6 set to +14°C — breach should trigger in 5s")
        elif cmd.startswith("kill "):
            nid = cmd.split()[1].upper()
            if nid in nodes_dict:
                nodes_dict[nid].kill()
                dodag.kill_node(nid)
                print(f"[Demo] {nid} killed — LWT will fire")
        elif cmd.startswith("revive "):
            nid = cmd.split()[1].upper()
            if nid in nodes_dict:
                nodes_dict[nid].alive = True
                nodes_dict[nid].base_temp = {
                    "S1":-18,"S2":-20,"S3":2,
                    "S4":4,"S5":6,"S6":6
                }.get(nid, 6)
                nodes_dict[nid].start()
                dodag.revive_node(nid)
        elif cmd == "tree":
            dodag.print_tree()
        elif cmd == "quit":
            print("Shutting down.")
            break