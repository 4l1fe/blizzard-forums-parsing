import socket
import fcntl
import struct
import pymongo
import constants as cns
from logging import Handler, NOTSET


def get_lan_ip():
    """возвращает строку с IP адресом"""

    def get_interface_ip(ifname):
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        return socket.inet_ntoa(fcntl.ioctl(s.fileno(), 0x8915, struct.pack('256s', ifname[:15]))[20:24])

    ip = socket.gethostbyname(socket.gethostname())
    if ip.startswith("127."):
        interfaces = [b"eth0", b"eth1", b"eth2", b"wlan0", b"wlan1", b"wifi0", b"ath0", b"ath1", b"ppp0"]
        for ifname in interfaces:
            try:
                ip = get_interface_ip(ifname)
                break
            except IOError:
                pass
    return ip


class MongoHandler(Handler):
     #todo время сделать объектом UTC datetime
     #todo добавить параметры логера extra в поля документа
     #todo создать соединение при инициализации и пересоединяться при разрывах
    """Обработчик создает структуру документа с полями, соответствующими атрибутам объекта LogRecord,
    дополнительно добавляя локальный ip адресс, если он указан в параметре fields при инициализации,
    и записывает в коллекцию БД, указанных в константах.
    Параметры подключения к БД могут указываться при инициализации обработчика.
    """

    def __init__(self, level=NOTSET, host='localhost', port=27017, fields=('ip', 'message'), **kwargs):
        super().__init__(level=level)
        self.ip = get_lan_ip()
        self.host = host
        self.port = port
        if not fields:
            raise AttributeError('There is no any fields name')
        self.fields = fields

    def emit(self, record):
        try:
            mc = pymongo.MongoClient(host=self.host, port=self.port)
            db = mc[cns.MONGO_DB]
            collection = db[cns.MONGO_LOG_COLLECTION]

            document = {}
            for field in self.fields:
                if field == 'ip':
                    document[field] = self.ip
                elif field == 'message':
                    document[field] = record.getMessage()
                else:
                    document[field] = getattr(record, field)

            collection.insert_one(document)
        except Exception:
            self.handleError(record)


class Tree:
    """Сущность дерева
    """

    class Node:
        """Сущность узла дерева необходима только лишь для простого доступа к данным родительских узлов(в редисе) в
        распределённой системе при сохранении их в БД обобщенным документом. Т.е. документ будет содержать данные из узлов
        дерева от листа до корня.
        """

        def __init__(self, position=None, data=None, parent=None, level=cns.NODE_FORUM_LEVEL):
            self.position = position
            self.data = data
            self.level = level
            self._parent = parent

        def get_ancestors(self):
            p = self._parent
            while p:
                yield p
                p = p.parent

    def __init__(self, redis_client):
        self.redis_client = redis_client

    def add_node(self, ):
        if not isinstance(nodes, (list, tuple, set)):
            nodes = (nodes, )
        pipeline = self.redis_client.pipeline()
        for node in nodes:
            pipeline.hmset(node.position, 'data', node.data, 'level', node.level, 'parent', node.parent)
        pipeline.execute()

    def get(self, position):
        data, level, parent = self.redis_client.hmget(position, 'data', 'level', 'parent')
        if not all((data, level, parent,)):
            return None
        return self.Node(position=position, data=data, level=level, parent=parent)
