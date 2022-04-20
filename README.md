1. com.task.Kafka_Flink_Sender
1. --topic test_topic1 --bootstrap.servers 10.1.12.183:9092 --group.id group1
1. --topic test_topic3 --bootstrap.servers 10.1.12.183:9092 --group.id grouptest


1. com.task.Kafka_Flink_Apply
1. --output-topic test_topic2 --bootstrap.servers 10.1.12.183:9092 --group.id group1 --input-topic test_topic1
1. --output-topic test_topic4 --bootstrap.servers 10.1.12.183:9092 --group.id grouptest --input-topic test_topic3

1. com.task.StatefulTask
1. --output-topic test_topic2 --bootstrap.servers 10.1.12.183:9092 --group.id group1 --input-topic test_topic1
1. --output-topic test_topic4 --bootstrap.servers 10.1.12.183:9092 --group.id grouptest --input-topic test_topic3

