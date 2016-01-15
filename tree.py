import logging
import pickle
import constants as cns


logger = logging.getLogger(__name__)


class Tree:

    def __init__(self, redis_client):
        self.redis_client = redis_client

    def add_root(self, position):
        n = Node(position)
        self._set(n)
        return n

    def add_nodes(self, nodes):
        self._set(nodes)

    def get_node(self, position):
        key = cns.NODE_KEY_PREFIX + position
        binary_node = self.redis_client.get(key)
        if binary_node:
            node = pickle.loads(binary_node)
            return node
        return None

    def get_parents(self, position):
        parents = []
        while True:
            node = self.get_node(position)
            if not node:
                break
            parents.append(node)
            if not node.parent:
                break
            position = node.parent
        return parents

    def _set(self, nodes):
        if not isinstance(nodes, (list, tuple, set)):
            nodes = (nodes, )
        keys_binary_nodes = {cns.NODE_KEY_PREFIX + node.position: pickle.dumps(node) for node in nodes}
        if keys_binary_nodes:
            self.redis_client.mset(keys_binary_nodes)


class Node(object):
    """Сущность узла дерева необходима только лишь для простого доступа к данным родительских узлов(в редисе) в
    распределённой системе при сохранении их в БД обобщенным документом. Т.е. документ будет содержать данные из узлов
    дерева от листа до корня.
    """

    __slots__ = ('position', 'data', 'level', 'parent')

    def __init__(self, position=None, data=None, parent=None, level=cns.NODE_FORUM_LEVEL):
        self.position = position
        self.data = data
        self.level = level
        self.parent = parent

    def __str__(self):
        return '<{}.level={}>'.format(self.__class__.__name__, self.level)

    def __repr__(self):
        return '<{}.level={}>'.format(self.__class__.__name__, self.level)