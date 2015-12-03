# coding:utf-8

from pykafka.client import KafkaClient
import logging
from pykafka.common import OffsetType
import urllib2
import json
import urllib
import threading
import SimpleHTTPServer
import SocketServer
import sys
from kazoo.client import KazooClient
from time import sleep
from _socket import gethostname

logging.basicConfig(level = logging.INFO)

consumer_logger = logging.getLogger('consumer')

if len(sys.argv) < 2:
    consumer_logger.error('python consumer.py http_port')
    sys.exit(-1)
    
# 1, 将自己的HTTP监听地址注册到ZK上
http_port = sys.argv[1]

zkclient = KazooClient('localhost:3000,localhost:3001,localhost:3002/kafka')
zkclient.start()

zk_path = '/consumers/balance-consumer/nodes/' + gethostname() + ":" + str(sys.argv[1])
registed = False

for times in range(0, 5):
    try:
        try:
            node_dir = zkclient.get('/consumers/balance-consumer/nodes') # 目录不存在
        except:
            zkclient.create('/consumers/balance-consumer/nodes') # 创建目录
        registed = zkclient.create(zk_path, ephemeral = True)
        consumer_logger.info('register to zookeeper, path={}'.format(zk_path))
        break
    except:
        consumer_logger.warning('fail to register to zookeeper[{}], retry 5 seconds later...'.format(zk_path))
        sleep(5)

if not registed:
    consumer_logger.error('finally failed to register to zookeeper, path={}'.format(zk_path))
    sys.exit(-1)

# 2, 连接kafka集群
client = KafkaClient('localhost:8990,localhost:8991,localhost:8992')

nmq = client.topics['nmq']

consumer = nmq.get_balanced_consumer('balance-consumer', zookeeper_connect = 'localhost:3000,localhost:3001,localhost:3002/kafka', 
                                     auto_offset_reset = OffsetType.LATEST, auto_commit_enable = True, num_consumer_fetchers = 3)

# 3, 启动HTTP服务
def httpd_main(consumer):
    class ResetOffsetRequestHandler(SimpleHTTPServer.SimpleHTTPRequestHandler):
        def __init__(self, request, client_addr, server):
            SimpleHTTPServer.SimpleHTTPRequestHandler.__init__(self, request, client_addr, server)
            self.consumer = consumer
        def do_GET(self):
            if self.path.startswith('/reset-offset'):
                try:
                    consumer.reset_offsets()
                    self.send_response(200)
                    self.end_headers()
                    self.wfile.write("succeed")
                except:
                    self.send_response(500)
                    self.end_headers()
                    self.wfile.write("fail")
    SocketServer.TCPServer.allow_reuse_address = True
    httpd = SocketServer.TCPServer(("", int(http_port)), ResetOffsetRequestHandler)
    httpd.serve_forever()

httpd_thread = threading.Thread(target = httpd_main, args = (consumer, ))
httpd_thread.setDaemon(True)
httpd_thread.start()

# 4, 开始consume消费消息
while True:
    msg = consumer.consume()
    try:
        request = json.loads(msg.value)
        url = request['url'] if request['url'].startswith('http') else "http://" + request['url']
        body = {'content' : request['body']}
        for i in range(0, 3):
            try:
                handle = urllib2.urlopen(url, urllib.urlencode(body), 5000)
                code = handle.getcode()
                response = handle.read()
                if code == 200:
                    consumer_logger.info('finish {} wihtin {} times. {}'.format(url, i, response))
                    break
            except:
                pass
            consumer_logger.warning("retry {} for {} times.".format(url, i))
    except:
        pass
