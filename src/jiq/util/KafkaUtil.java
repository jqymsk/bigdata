package jiq.util;

import java.util.Collections;
import java.util.List;

import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.TopicMetadataResponse;
import kafka.javaapi.consumer.SimpleConsumer;

public class KafkaUtil {

	public static int getPartitionNum(String host, int port, int soTimeout, int bufferSize, String clientId,
			String topic) {
		SimpleConsumer simpleConsumer = new SimpleConsumer(host, port, soTimeout, bufferSize, clientId);
		List<String> topics = Collections.singletonList(topic);
		TopicMetadataRequest req = new TopicMetadataRequest(topics);
		TopicMetadataResponse resp = simpleConsumer.send(req);
		List<TopicMetadata> topicMetadatas = resp.topicsMetadata();
		TopicMetadata topicMetadata = topicMetadatas.get(0);
		List<PartitionMetadata> partitionsMetadatas = topicMetadata.partitionsMetadata();
		int partitionNum = partitionsMetadatas.size();
		return partitionNum;
	}

}
