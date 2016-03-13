import unittest
import logging
from subprocess import Popen, PIPE
import constants as cns
from redis import Redis
from tree import Tree, Node

logger = logging.getLogger(__name__)
redis_server = None


def setUpModule():
    global redis_server
    redis_server = Popen(['redis-server', 'configs/redis.conf'], stdin=PIPE, stdout=PIPE, stderr=PIPE)
    while True:
        logger.info('Waiting for redis server')
        if redis_server.stdout: break


@unittest.skip('')
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


class GetParentsTestCase(unittest.TestCase):

    @classmethod
    def setUpClass(cls):
        with open('getparents.lua') as file:
            cls.lua_script = file.read()

    def setUp(self):
        self .redis = Redis()

    def test_get_parents(self):
        forum = dict(position='root', data=None, parent=None)
        subcategory = dict(position='subcategory1', data=[1,2,3], parent=forum['position'])
        topic = dict(position='topic1', data='SomeДата', parent=subcategory['position'])

        pipeline = self.redis.pipeline()
        pipeline.hmset(forum['position'], forum)
        pipeline.hmset(subcategory['position'], subcategory)
        pipeline.hmset(topic['position'], topic)
        pipeline.execute()

        nodes = self.redis.eval(self.lua_script, 1, topic['position'])
        node1 = dict((nodes[0][i], nodes[0][i+1]) for i in range(0, len(nodes[0])-1, 2))
        node2 = dict((nodes[1][i], nodes[1][i+1]) for i in range(0, len(nodes[1])-1, 2))
        node3 = dict((nodes[2][i], nodes[2][i+1]) for i in range(0, len(nodes[2])-1, 2))
        self.assertEqual(topic['position'], node1['position'])
        self.assertEqual(topic['data'], node1['data'])
        self.assertEqual(topic['parent'], node1['parent'])
        self.assertEqual(subcategory['position'], node2['position'])
        self.assertEqual(subcategory['data'], node2['data'])
        self.assertEqual(subcategory['parent'], node2['parent'])
        self.assertEqual(forum['position'], node3['position'])
        self.assertEqual(forum['data'], node3['data'])
        self.assertEqual(forum['parent'], node3['parent'])

def tearDownModule():
    global redis_server
    redis_server.terminate()