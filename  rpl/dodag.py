import threading, time

class DODAGNode:
    def __init__(self, node_id, rank, parent=None):
        self.node_id  = node_id
        self.rank     = rank
        self.parent   = parent
        self.children = []
        self.alive    = True

class DODAG:
    def __init__(self):
        # Build the tree
        self.root   = DODAGNode("root",   rank=1)
        self.rA     = DODAGNode("routerA",rank=2, parent="root")
        self.rB     = DODAGNode("routerB",rank=2, parent="root")
        self.rC     = DODAGNode("routerC",rank=2, parent="root")
        self.nodes  = {
            "root":    self.root,
            "routerA": self.rA,
            "routerB": self.rB,
            "routerC": self.rC,
            "S1": DODAGNode("S1", rank=3, parent="routerA"),
            "S2": DODAGNode("S2", rank=3, parent="routerA"),
            "S3": DODAGNode("S3", rank=3, parent="routerB"),
            "S4": DODAGNode("S4", rank=3, parent="routerB"),
            "S5": DODAGNode("S5", rank=3, parent="routerC"),
            "S6": DODAGNode("S6", rank=3, parent="routerC"),
        }
        # Build child lists
        for nid, node in self.nodes.items():
            if node.parent and node.parent in self.nodes:
                self.nodes[node.parent].children.append(nid)

        print("[DODAG] DIO broadcast — DODAG formed")
        print("[DODAG] All nodes sent DAO — routes registered")

    def kill_node(self, node_id):
        node = self.nodes.get(node_id)
        if node:
            node.alive = False
            print(f"[DODAG] {node_id} dead — LWT fired — parent re-select starting")
            self._heal(node_id)

    def _heal(self, dead_id):
        # Find orphaned children and re-parent them
        parent_id = self.nodes[dead_id].parent
        for nid, node in self.nodes.items():
            if node.parent == dead_id and node.alive:
                # promote sibling or router as new parent
                node.parent = parent_id
                print(f"[DODAG] {nid} re-parented to {parent_id} via DIS/DAO")

    def revive_node(self, node_id):
        node = self.nodes.get(node_id)
        if node:
            node.alive = True
            print(f"[DODAG] {node_id} sent DIS — rejoined DODAG")

    def print_tree(self):
        print("\n── DODAG tree ──")
        def _show(nid, indent=0):
            n = self.nodes[nid]
            status = "alive" if n.alive else "DEAD"
            print(" " * indent + f"[{nid}] rank={n.rank} ({status})")
            for child in n.children:
                _show(child, indent+4)
        _show("root")
        print()