# Copyright 2018 JanusGraph Python Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from gremlin_python.structure.io.graphsonV3d0 import GraphSONReader
from gremlin_python.structure.io.graphsonV3d0 import GraphSONUtil

from ....serializer.relation_identifier_deserializer import RelationIdentifierDeserializer
from ....serializer.geoshape_deserializer import GeoShapeDeserializer


class JanusGraphSONReaderBuilder(object):
    """Registers JanusGraph-specific deserializers so that objects like GeoShape and RelationIdentifier
    can be interpreted on Python client side.
    """

    RELATIONID_GRAPHSON_TYPE = "janusgraph:RelationIdentifier"
    GeoShape_GRAPHSON_TYPE = "janusgraph:Geoshape"

    deserializers = dict()

    def __init__(self):
        self.reader = None

    def __register_default_deserializers(self):
        """This method is used to register the Default deserializers for JanusGraph's python client.
            Currently the deserializer registers RelationIdentifier classes.

        Returns:
            None
        """
        janus_deserializers = self.__build_deserializers()
        self.deserializers.update(janus_deserializers)

    def __build_deserializers(self):
        """The actual method which takes care of adding JanusGraph specific deserializers.
        Returns:
            dict
        """
        # Currently the default de-serializers registered.

        janus_deserializers = {
            self.RELATIONID_GRAPHSON_TYPE: RelationIdentifierDeserializer,
            self.GeoShape_GRAPHSON_TYPE: GeoShapeDeserializer,
        }

        return janus_deserializers

    def build(self):
        """The method registers JanusGraph specific deserializers into Gremlin GraphSON Reader class.

        Returns:
            GraphSONReader
        """

        self.__register_default_deserializers()
        self.reader = GraphSONReader(self.deserializers)
        return self.reader

    def register_deserializer(self, type_id, prefix, deserializer):
        """This method is used to registering any additional JanusGraph de-serializers.

        Args:
            type_id (str): The identifier to be used to register the De-serializer against.
            prefix (str): The prefix to use in typeID
            deserializer: The De-serializer class.

        Returns:

        """

        object_identifier = GraphSONUtil.formatType(prefix, type_id)
        self.deserializers[object_identifier] = deserializer
        return self
