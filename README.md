环境：
1, kafka 0.8.2（基于topic实现的consumer group offset) 
2, pip install pykafka
3, python 2.7.10

项目说明：
	使用pykafka实现基于kafka的Http rpc

代码说明：
	producer.py：命令行测试工具，发送一条消息
	consumer.py：负载均衡的consumer，可以启动多个实例，partition将自动在多进程间负载均衡
	offset_check.py：检查topic的各partition的offset总量和消费的offset量

