/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 * 
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jiq.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jiq.util.KafkaUtil;
import jiq.util.LoginUtil;
import jiq.util.PropertyUtil;

public class Producer implements Runnable {
	private static final Logger LOG = LoggerFactory.getLogger(Producer.class);

	private final String topic;
	private final Boolean isAsync;
	private final int partitionNum;
	private final String bootstrapServers;

	private static final Properties props = new Properties();

	public Producer(String produceToTopic, boolean asyncEnable, int partitionNum, String bootstrapServers) {
		this.topic = produceToTopic;
		this.isAsync = asyncEnable;
		this.partitionNum = partitionNum;
		this.bootstrapServers = bootstrapServers;
	}

	/**
	 * 启动多个线程进行发送
	 */
	@Override
	public void run() {
		// 指定的线程号，仅用于区分不同的线程
		for (int threadNum = 0; threadNum < partitionNum; threadNum++) {
			ProducerWorker producerThread = new ProducerWorker(topic, isAsync, threadNum, bootstrapServers);
			new Thread(producerThread).start();
		}
	}

	public static void securityPrepare() throws IOException {
		String filePath = System.getProperty("user.dir") + File.separator + "conf" + File.separator;
		String krbFile = filePath + "krb5.conf";
		String userKeyTableFile = filePath + "user.keytab";

		// windows路径下分隔符替换
		userKeyTableFile = userKeyTableFile.replace("\\", "\\\\");
		krbFile = krbFile.replace("\\", "\\\\");

		LoginUtil.setKrb5Config(krbFile);
		LoginUtil.setZookeeperServerPrincipal("zookeeper/hadoop.hadoop.com");
		LoginUtil.setJaasFile("jiq", userKeyTableFile);
	}

	public static void main(String[] args) {
		// 安全模式下启用
		try {
			LOG.info("Securitymode start.");
			securityPrepare();
		} catch (IOException e) {
			LOG.error("Security prepare failure.");
			return;
		}
		LOG.info("Security prepare success.");
		PropertyUtil property = PropertyUtil.getInstance();
		String bootstrapServers = property.getValue("bootstrap.servers",
				"190.15.116.189:21007,190.15.116.196:21007,190.15.116.190:21007");
		String topic = property.getValue("topic", "topic");
		String host = bootstrapServers.substring(0, bootstrapServers.indexOf(':'));
		int partitionNum = KafkaUtil.getPartitionNum(host, 21005, 100000, 64 * 1024, UUID.randomUUID().toString(),
				topic);

		// 是否使用异步发送模式
		final boolean asyncEnable = false;
		Producer producer = new Producer(topic, asyncEnable, partitionNum, bootstrapServers);
		new Thread(producer).start();
	}

	/**
	 * 生产者线程类
	 */
	private class ProducerWorker implements Runnable {
		private final KafkaProducer<Integer, String> producer;
		private String topic;
		private Boolean isAsync;
		private int sendThreadId = 0;

		/**
		 * 生产者线程类构造方法
		 * 
		 * @param topicName
		 *            Topic名称
		 * @param threadNum
		 *            线程号
		 */
		public ProducerWorker(String topicName, Boolean asyncEnable, int threadNum, String bootstrapServers) {
			// Broker地址列表
			props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
			// 客户端ID
			props.put(ProducerConfig.CLIENT_ID_CONFIG, UUID.randomUUID().toString());
			// Key序列化类
			props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.IntegerSerializer");
			// Value序列化类
			props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,
					"org.apache.kafka.common.serialization.StringSerializer");
			// 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
			props.put("security.protocol", "SASL_PLAINTEXT");
			// 服务名
			props.put("sasl.kerberos.service.name", "kafka");
			// 创建生产者对象
			producer = new KafkaProducer<>(props);
			this.sendThreadId = threadNum;
			this.topic = topicName;
			this.isAsync = asyncEnable;
		}

		public void run() {
			LOG.info("Producer: start.");

			// 用于记录消息条数
			int messageNo = 1;

			// 每个线程发送的消息条数
			int messagesPerThread = 50;
			while (messageNo <= messagesPerThread) {
				// 指定该record发到哪个partition中
				int partition = sendThreadId;
				// 时间戳
				long startTime = System.currentTimeMillis();
				int key = sendThreadId;
				// 待发送的消息内容
				String messageStr = new String("Message_" + sendThreadId + "_" + messageNo);

				// 构造消息记录
				ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topic, partition,
						startTime, key, messageStr);

				if (isAsync) { // 异步发送
					producer.send(record, new DemoCallBack(startTime, sendThreadId, messageStr));
				} else { // 同步发送
					try {
						producer.send(record).get();
					} catch (InterruptedException | ExecutionException e) {
						e.printStackTrace();
					}
				}

				LOG.info("Producer: send " + messageStr + " to " + topic + " partition " + partition + " with key: "
						+ sendThreadId);
				++messageNo;
			}

			try {
				producer.close();
				LOG.info("Producer " + this.sendThreadId + " closed.");
			} catch (Throwable e) {
				LOG.error("Error when closing producer", e);
			}

		}
	}

}

class DemoCallBack implements Callback {
	private static Logger LOG = LoggerFactory.getLogger(DemoCallBack.class);

	private long startTime;
	private int key;
	private String message;

	public DemoCallBack(long startTime, int key, String message) {
		this.startTime = startTime;
		this.key = key;
		this.message = message;
	}

	/**
	 * 回调函数，用于处理异步发送模式下，消息发送到服务端后的处理。
	 * 
	 * @param metadata
	 *            元数据信息
	 * @param exception
	 *            发送异常。如果没有错误发生则为Null。
	 */
	@Override
	public void onCompletion(RecordMetadata metadata, Exception exception) {
		long elapsedTime = System.currentTimeMillis() - startTime;
		if (metadata != null) {
			LOG.info("message(" + key + ", " + message + ") sent to partition(" + metadata.partition() + "), "
					+ "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
		} else if (exception != null) {
			exception.printStackTrace();
		}

	}

}
