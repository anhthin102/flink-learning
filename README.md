# PushMessageToTopic

com.task.Kafka_Flink_Sender
--topic test_topic1 --bootstrap.servers 10.1.12.183:9092 --group.id group1

com.task.Kafka_Flink_Apply
--output-topic test_topic2 --bootstrap.servers 10.1.12.183:9092 --group.id group1 --input-topic test_topic1
