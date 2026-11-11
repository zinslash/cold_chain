import networkx as nx
import matplotlib.pyplot as plt
import matplotlib.animation as animation
import threading

class DODAGVisualizer:
    def __init__(self, dodag):
        self.dodag = dodag
        self.G     = nx.DiGraph()
        self._build_graph()

    def _build_graph(self):
        self.G.clear()
        for nid, node in self.dodag.nodes.items():
            self.G.add_node(nid, alive=node.alive, rank=node.rank)
            if node.parent:
                self.G.add_edge(nid, node.parent)

    def _get_colors(self):
        colors = []
        for nid in self.G.nodes():
            node = self.dodag.nodes[nid]
            if not node.alive:
                colors.append("#F09595")   # dead — red
            elif node.rank == 1:
                colors.append("#888780")   # root — gray
            elif node.rank == 2:
                colors.append("#AFA9EC")   # router — purple
            else:
                colors.append("#9FE1CB")   # leaf — teal
        return colors

    def _pos(self):
        # fixed layout matching DODAG structure
        return {
            "root":    (0.5, 0.95),
            "routerA": (0.17, 0.65),
            "routerB": (0.5,  0.65),
            "routerC": (0.83, 0.65),
            "S1": (0.08, 0.3), "S2": (0.26, 0.3),
            "S3": (0.42, 0.3), "S4": (0.58, 0.3),
            "S5": (0.74, 0.3), "S6": (0.92, 0.3),
        }

    def animate(self, interval_ms=3000):
        fig, ax = plt.subplots(figsize=(10, 6))
        fig.suptitle("Cold Chain — DODAG Live Topology", fontsize=13)

        def update(frame):
            ax.clear()
            self._build_graph()
            pos    = self._pos()
            colors = self._get_colors()
            nx.draw(
                self.G, pos, ax=ax,
                with_labels=True, node_color=colors,
                node_size=1800, font_size=9,
                arrows=True, arrowsize=18,
                edge_color="#D3D1C7",
            )
            ranks = {nid: f"rank {n.rank}" for nid, n in self.dodag.nodes.items()}
            nx.draw_networkx_labels(
                self.G, {k: (v[0], v[1]-0.07) for k,v in pos.items()},
                ranks, font_size=7, font_color="#888780", ax=ax
            )

        ani = animation.FuncAnimation(fig, update, interval=interval_ms)
        plt.tight_layout()
        plt.show()