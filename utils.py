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
