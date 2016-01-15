import unittest
import constants as cns
from redis import Redis
from tree import Tree, Node


class TreeTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        Redis().flushdb()

    def setUp(self):
        self.redis_client = Redis()
        self.tree = Tree(self.redis_client)

    def test_set_get(self):
        position = 'some_node'
        level = cns.NODE_TOPIC_LEVEL
        data = {'name': 'topic name', 'description': '\tописание топика\n'}
        parent = 'root'
        node = Node(position, data, parent, level)
        self.tree._set(node)

        node = self.tree.get_node(position)
        self.assertEqual(node.position, position)
        self.assertEqual(node.level, level)
        self.assertEqual(node.data, data)
        self.assertEqual(node.parent, parent)

    def test_get_parents(self):
        root = Node(position='root')
        subcategory = Node(position='subcategory position', level=cns.NODE_SUBCATEGORY_LEVEL,
                     data={'name': 'subcategory name', 'description': '\tописание топика\n'},
                     parent=root.position)
        topic = Node(position='topic position', level=cns.NODE_TOPIC_LEVEL,
                     data={'name': 'topic name', 'author': 'Auth1'},
                     parent=subcategory.position)
        self.tree.add_nodes((root, subcategory, topic))

        nodes = self.tree.get_parents(topic.position)
        self.assertEqual(nodes[0].position, topic.position)
        self.assertEqual(nodes[0].level, topic.level)
        self.assertEqual(nodes[0].data, topic.data)
        self.assertEqual(nodes[0].parent, topic.parent)

        self.assertEqual(nodes[1].position, subcategory.position)
        self.assertEqual(nodes[1].level, subcategory.level)
        self.assertEqual(nodes[1].data, subcategory.data)
        self.assertEqual(nodes[1].parent, subcategory.parent)

        self.assertEqual(nodes[2].position, root.position)
        self.assertEqual(nodes[2].level, root.level)
        self.assertEqual(nodes[2].data, root.data)
        self.assertEqual(nodes[2].parent, root.parent)

    def tearDown(self):
        self.redis_client.flushdb()