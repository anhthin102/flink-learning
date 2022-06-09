1. com.task.Kafka_Flink_Sender 
2. --topic thin-TH02-topic1 --bootstrap.servers 10.1.12.183:9092 --group.id TH02

3. KafkaFlinkFilter
4. --output-topic thin-TH03-topic1 --bootstrap.servers 10.1.12.183:9092 --group.id test --input-topic thin-TH02-topic1

5. com.task.Kafka_Flink_Apply
6. --output-topic test_topic2 --bootstrap.servers 10.1.12.183:9092 --group.id group1 --input-topic test_topic1
7. --output-topic test_topic4 --bootstrap.servers 10.1.12.183:9092 --group.id grouptest --input-topic test_topic3

8. com.task.StatefulTask
9. --output-topic test_topic2 --bootstrap.servers 10.1.12.183:9092 --group.id group1 --input-topic test_topic1
10. --output-topic test_topic4 --bootstrap.servers 10.1.12.183:9092 --group.id grouptest --input-topic test_topic3

