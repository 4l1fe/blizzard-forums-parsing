MONGO_DB = 'blizzard'
MONGO_COLLECTION = 'posts'
MONGO_LOG_COLLECTION = 'logs'

INSERT_BATCH_SIZE = 100
BLOCKING_TIMEOUT = 3
REQUEST_TIMEOUT = 20
CONNECT_TIMEOUT = 20

NAMESPACE = 'forum-parser:'
URL_QUEUE_KEY = NAMESPACE + 'urls'
DATA_QUEUE_KEY = NAMESPACE + 'data-urls'
NODE_QUEUE_KEY = NAMESPACE + 'data-nodes'
PARSED_URLS_KEY = NAMESPACE + 'parsed-urls'
DATA_KEY_PREFIX = NAMESPACE + 'url-data:'
NODE_KEY_PREFIX = NAMESPACE + 'node-data:'

NODE_FORUM_LEVEL = 0
NODE_SUBCATEGORY_LEVEL = 1
NODE_TOPIC_LEVEL = 2
