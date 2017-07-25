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
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jiq.util.LoginUtil;
import jiq.util.PropertyUtil;

public class ProducerMultThread extends Thread {
	private static final Logger LOG = LoggerFactory.getLogger(ProducerMultThread.class);

	// 并发的线程数
	private static final int PRODUCER_THREAD_COUNT = 3;

	private final String topic;
	private final Boolean isAsync;

	private static final Properties props = new Properties();

	public ProducerMultThread(String produceToTopic, boolean asyncEnable) {
		topic = produceToTopic;
		isAsync = asyncEnable;
	}

	/**
	 * 启动多个线程进行发送
	 */
	public void run() {
		// 指定的线程号，仅用于区分不同的线程
		for (int threadNum = 0; threadNum < PRODUCER_THREAD_COUNT; threadNum++) {
			ProducerThread producerThread = new ProducerThread(topic, isAsync, threadNum);
			producerThread.start();
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
		LOG.info("Securitymode start.");
		try {
			LOG.info("Securitymode start.");
			securityPrepare();
		} catch (IOException e) {
			LOG.error("Security prepare failure.");
			LOG.error("The IOException occured.", e);
			return;
		}
		LOG.info("Security prepare success.");

		String topic = PropertyUtil.getInstance().getValue("topic", "topic");

		// 是否使用异步发送模式
		final boolean asyncEnable = false;
		ProducerMultThread producerMultThread = new ProducerMultThread(topic, asyncEnable);
		producerMultThread.start();
	}

	/**
	 * 生产者线程类
	 */
	private class ProducerThread extends Thread {
		private int sendThreadId = 0;
		private String sendTopic = null;
		private Boolean isAsync;
		private final KafkaProducer<Integer, String> producer;

		/**
		 * 生产者线程类构造方法
		 * 
		 * @param topicName
		 *            Topic名称
		 * @param threadNum
		 *            线程号
		 */
		public ProducerThread(String topicName, Boolean asyncEnable, int threadNum) {
			this.sendThreadId = threadNum;
			this.sendTopic = topicName;
			this.isAsync = asyncEnable;

			PropertyUtil property = PropertyUtil.getInstance();

			// Broker地址列表
			props.put("bootstrap.servers", property.getValue("bootstrap.servers", "localhost:21007"));

			// 客户端ID
			props.put("client.id", UUID.randomUUID().toString());

			// Key序列化类
			props.put("key.serializer",
					property.getValue("key.serializer", "org.apache.kafka.common.serialization.IntegerSerializer"));
			// Value序列化类
			props.put("value.serializer",
					property.getValue("value.serializer", "org.apache.kafka.common.serialization.StringSerializer"));
			// 协议类型:当前支持配置为SASL_PLAINTEXT或者PLAINTEXT
			props.put("security.protocol", property.getValue("security.protocol", "SASL_PLAINTEXT"));

			// 服务名
			props.put("sasl.kerberos.service.name", "kafka");

			// 创建生产者对象
			producer = new KafkaProducer<Integer, String>(props);
		}

		public void run() {
			LOG.info("Producer: start.");

			// 用于记录消息条数
			int messageCount = 1;

			// 每个线程发送的消息条数
			int messagesPerThread = 5;
			while (messageCount <= messagesPerThread) {
				// 指定该record发到哪个partition中
				int partition = sendThreadId;
				// 时间戳
				long startTime = System.currentTimeMillis();
				int key = sendThreadId;
				// 待发送的消息内容
				String value = new String("Message_" + sendThreadId + "_" + messageCount);

				// 构造消息记录
				ProducerRecord<Integer, String> record = new ProducerRecord<Integer, String>(topic, partition,
						startTime, key, value);

				if (isAsync) {
					// 异步发送
					producer.send(record, new DemoCallBack(startTime, sendThreadId, value));
				} else {
					try {
						// 同步发送
						producer.send(record).get();
					} catch (InterruptedException ie) {
						LOG.info("The InterruptedException occured : {}.", ie);
					} catch (ExecutionException ee) {
						LOG.info("The ExecutionException occured : {}.", ee);
					}
				}

				LOG.info("Producer: send " + value + " to " + sendTopic + " with key: " + sendThreadId);
				messageCount++;

				// 每隔1s，发送1条消息
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
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
	public void onCompletion(RecordMetadata metadata, java.lang.Exception exception) {
		long elapsedTime = System.currentTimeMillis() - startTime;
		if (metadata != null) {
			LOG.info("message(" + key + ", " + message + ") sent to partition(" + metadata.partition() + "), "
					+ "offset(" + metadata.offset() + ") in " + elapsedTime + " ms");
		} else if (exception != null) {
			LOG.error("The Exception occured.", exception);
		}

	}
}
