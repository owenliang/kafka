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

logging.basicConfig(level = logging.INFO)

consumer_logger = logging.getLogger('consumer')

client = KafkaClient('localhost:8990,localhost:8991,localhost:8992')

nmq = client.topics['nmq']

consumer = nmq.get_balanced_consumer('balance-consumer', zookeeper_connect = 'localhost:3000,localhost:3001,localhost:3002/kafka', 
                                     auto_offset_reset = OffsetType.LATEST, auto_commit_enable = True, num_consumer_fetchers = 3)

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
    httpd = SocketServer.TCPServer(("", 8765), ResetOffsetRequestHandler)
    httpd.serve_forever()

httpd_thread = threading.Thread(target = httpd_main, args = (consumer, ))
httpd_thread.setDaemon(True)
httpd_thread.start()

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
