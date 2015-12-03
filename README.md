##项目说明：

    使用pykafka实现基于kafka的Http rpc。    
    
    consumer采用balanced consumer group实现，可以启动任意多实例，基于ZK协调负载均衡和容灾。
    
    producer可以是任意语言，只需按照协议向kafka投递message，consumer会从message中获取url和body，并向url发起HTTP POST请求，收到200返回码认为请求成功。

##环境：
* kafka 0.8.2（基于topic实现的consumer group offset) 
* pip install pykafka
* python 2.7.10

##代码说明：
* producer.py：命令行测试工具，发送一条消息
* consumer.py：负载均衡的consumer，可以启动多个实例，partition将自动在多进程间负载均衡，可通过Http通知其重置offset到队列末尾（运维常见场景）
* offset_check.py：检查topic的各partition的offset总量和消费的offset量
* offset_reset.py：根据consumer在zk上注册的http地址，逐个调用/reset-offset请求将consume offset调整至队列末尾