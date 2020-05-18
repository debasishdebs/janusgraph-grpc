from janusgraph_python.structure.janusgraph import JanusGraph
from janusgraph_python.core.datatypes.geo_shape import Point


class GraphOfTheGodsFactory:
    traversal = None

    TITAN = "titan"
    LOCATION = "location"
    GOD = "god"
    DEMIGOD = "demigod"
    HUMAN = "human"
    MONSTER = "monster"

    def __init__(self):
        pass

    def load(self):
        return

    def load_without_mixed_index(self, graph):
        """

        Args:
            graph (JanusGraph):

        Returns:

        """
        self.traversal = graph.traversal()
        return self

    def _load_data_(self):
        tr = self.traversal
        self._load_schema_()
        self._load_vertices_and_edges_()
        return self

    def _load_vertices_and_edges_(self):
        tr = self.traversal

        saturn = tr.addV(self.TITAN).property("name", "saturn").property("age", 10000).next()
        sky = tr.addV(self.LOCATION).property("name", "sky").next()
        sea = tr.addV(self.LOCATION).property("name", "sea").next()
        jupiter = tr.addV(self.GOD).property("name", "jupiter").property("age", 5000).next()
        neptune = tr.addV(self.GOD).property("name", "neptune").property("age", 4500).next()
        pluto = tr.addV(self.GOD).property("name", "pluto").property("age", 4000).next()
        hercules = tr.addV(self.DEMIGOD).property("name", "hercules").property("age", 30).next()
        alcmene = tr.addV(self.HUMAN).property("name", "alcmene").property("age", 45).next()
        nemean = tr.addV(self.MONSTER).property("name", "nemean").next()
        hydra = tr.addV(self.MONSTER).property("name", "hydra").next()
        cereberus = tr.addV(self.MONSTER).property("name", "cerenerus").next()
        tartarus = tr.addV(self.LOCATION).property("name", "tartarus").next()

        tr.addE("father").from_(jupiter).to(saturn).next()
        tr.addE("lives").from_(jupiter).to(sky).next()
        tr.addE("brother").from_(jupiter).to(neptune).next()
        tr.addE("brother").from_(neptune).to(jupiter).next()
        tr.addE("lives").from_(neptune).to(sea).next()
        tr.addE("brother").from_(neptune).to(pluto).next()
        tr.addE("brother").from_(pluto).to(neptune).next()
        tr.addE("brother").from_(jupiter).to(pluto).next()
        tr.addE("brother").from_(pluto).to(jupiter).next()
        tr.addE("father").from_(hercules).to(jupiter).next()
        tr.addE("mother").from_(hercules).to(alcmene).next()
        tr.addE("battled").from_(hercules).to(nemean).property("time", 1).property("place", Point(38.1, 23.7)).next()
        tr.addE("battled").from_(hercules).to(hydra).property("time", 2).property("place", Point(37.7, 23.9)).next()
        tr.addE("battled").from_(hercules).to(cereberus).property("time", 12).property("place", Point(39, 22)).next()
        tr.addE("pet").from_(pluto).to(cereberus).next()
        tr.addE("lives").from_(pluto).to(tartarus).property("reason", "no fear of death").next()

        return self

    def _load_schema_(self):

        return
