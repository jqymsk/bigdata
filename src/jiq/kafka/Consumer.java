package jiq.kafka;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import jiq.util.LoginUtil;
import jiq.util.PropertyUtil;

/**
 * 消费者类
 */
public class Consumer<K, V> {
	private static final Logger LOG = LoggerFactory.getLogger(Consumer.class);

	private final KafkaConsumer<K, V> consumer;
	private ExecutorService executors;
	private final Map<TopicPartition, OffsetAndMetadata> offsets = new HashMap<>();

	public Consumer(String topic) {
		PropertyUtil property = PropertyUtil.getInstance();
		Properties props = new Properties();
		// Broker连接地址
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, property.getValue("bootstrap.servers", "localhost:21007"));
		// 本Consumer的组id，从consumer.properties中获取
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "consumergroup");
		// 是否自动提交offset
		props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

		// // 是否自动提交offset
		// props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
		// // 自动提交offset的时间间隔
		// props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
		// // 会话超时时间
		// props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");

		// 消息Key值使用的反序列化类
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.IntegerDeserializer");
		// 消息内容使用的反序列化类
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
				"org.apache.kafka.common.serialization.StringDeserializer");
		// 当zookeeper中没有本Consumer组的offset或者offset超出合法范围后，应从何处开始消费数据。
		// "smallest"——从头开始消费; "largest"——从当前位置开始消费; 其他值——在Consumer客户端抛异常？
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		// 安全协议类型
		props.put("security.protocol", "SASL_PLAINTEXT");
		// 服务名
		props.put("sasl.kerberos.service.name", "kafka");

		consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic), new ConsumerRebalanceListener() {
			@Override
			public void onPartitionsRevoked(Collection<TopicPartition> arg0) {
				consumer.commitSync(offsets);
			}

			@Override
			public void onPartitionsAssigned(Collection<TopicPartition> arg0) {
				offsets.clear();
			}
		});
	}

	/**
	 * 消费主方法
	 * 
	 * @param threadNumber
	 *            线程池中线程数
	 */
	public void consume(int threadNumber) {
		executors = new ThreadPoolExecutor(threadNumber, threadNumber, 0L, TimeUnit.MILLISECONDS,
				new ArrayBlockingQueue<Runnable>(1000), new ThreadPoolExecutor.CallerRunsPolicy());
		try {
			while (true) {
				ConsumerRecords<K, V> records = consumer.poll(1000L);
				if (!records.isEmpty()) {
					executors.submit(new ConsumerWorker<>(records, offsets));
				}
				commitOffsets();
			}
		} catch (WakeupException e) {
			// swallow this exception
		} finally {
			commitOffsets();
			consumer.close();
		}
	}

	private void commitOffsets() {
		// 尽量降低synchronized块对offsets锁定的时间
		Map<TopicPartition, OffsetAndMetadata> unmodfiedMap;
		synchronized (offsets) {
			if (offsets.isEmpty()) {
				return;
			}
			unmodfiedMap = Collections.unmodifiableMap(new HashMap<>(offsets));
			offsets.clear();
		}
		consumer.commitSync(unmodfiedMap);
	}

	public void close() {
		consumer.wakeup();
		executors.shutdown();
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

		// 启动消费线程，其中KafkaProperties.topic为待消费的topic名称
		final Consumer<byte[], byte[]> consumer = new Consumer<>("topic");
		final int cpuCount = Runtime.getRuntime().availableProcessors();

		Runnable runnable = new Runnable() {
			@Override
			public void run() {
				consumer.consume(cpuCount);
			}
		};
		new Thread(runnable).start();

		try {
			// 20秒后自动停止该测试程序
			Thread.sleep(20000L);
		} catch (InterruptedException e) {
			// swallow this exception
		}
		System.out.println("Starting to close the consumer...");
		consumer.close();
	}

	/**
	 * 消费者线程类
	 *
	 */
	private class ConsumerWorker<K, V> implements Runnable {
		private final ConsumerRecords<K, V> records;
		private final Map<TopicPartition, OffsetAndMetadata> offsets;

		/**
		 * 消费者线程类构造方法
		 * 
		 * @param record
		 *            流
		 * @param offsets
		 *            位移
		 */
		public ConsumerWorker(ConsumerRecords<K, V> record, Map<TopicPartition, OffsetAndMetadata> offsets) {
			this.records = record;
			this.offsets = offsets;
		}

		@Override
		public void run() {
			for (TopicPartition partition : records.partitions()) {
				List<ConsumerRecord<K, V>> partitionRecords = records.records(partition);
				for (ConsumerRecord<K, V> record : partitionRecords) {
					// 插入消息处理逻辑，本例只是打印消息
					System.out.println(String.format("topic=%s, partition=%d, offset=%d, key=%s, value=%s",
							record.topic(), record.partition(), record.offset(), record.key(), record.value()));
				}

				// 上报位移信息
				long lastOffset = partitionRecords.get(partitionRecords.size() - 1).offset();
				synchronized (offsets) {
					if (!offsets.containsKey(partition)) {
						offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
					} else {
						long curr = offsets.get(partition).offset();
						if (curr <= lastOffset + 1) {
							offsets.put(partition, new OffsetAndMetadata(lastOffset + 1));
						}
					}
				}
			}
		}

	}

}
