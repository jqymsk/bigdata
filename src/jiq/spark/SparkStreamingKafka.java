package jiq.spark;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Properties;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;

import com.huawei.spark.streaming.kafka.JavaDStreamKafkaWriterFactory;

import kafka.producer.KeyedMessage;
import kafka.serializer.StringDecoder;
import scala.Tuple2;
import scala.Tuple3;

public class SparkStreamingKafka {
	public static void main(String[] args) throws Exception {
		// Streaming分批的处理间隔
		String batchTime = args[0];
		// 统计数据的时间跨度,时间单位都是秒。
		final String windowTime = args[1];
		// Kafka中订阅的主题，多以逗号分隔。
		String topics = args[2];
		// 获取元数据的kafka地址
		String brokers = args[3];

		Duration batchDuration = Durations.seconds(Integer.parseInt(batchTime));
		Duration windowDuration = Durations.seconds(Integer.parseInt(windowTime));

		// 创建一个配置类SparkConf，然后创建一个StreamingContext
		SparkConf conf = new SparkConf().setAppName("SparkStreamingKafka");
		JavaStreamingContext jssc = new JavaStreamingContext(conf, batchDuration);

		// 设置Streaming的CheckPoint目录，由于窗口概念存在，该参数必须设置
		jssc.checkpoint("checkpoint");

		// 组装Kafka的主题列表
		HashSet<String> topicsSet = new HashSet<String>(Arrays.asList(topics.split(",")));
		HashMap<String, String> kafkaParams = new HashMap<String, String>();
		kafkaParams.put("metadata.broker.list", brokers);

		// 通过brokers和topics直接创建kafka stream
		// 1.接收Kafka中数据，生成相应DStream
		JavaDStream<String> lines = KafkaUtils.createDirectStream(jssc, String.class, String.class, StringDecoder.class,
				StringDecoder.class, kafkaParams, topicsSet).map(new Function<Tuple2<String, String>, String>() {
					public String call(Tuple2<String, String> tuple2) {
						return tuple2._2();
					}
				});

		// 2.获取每一个行的字段属性
		JavaDStream<Tuple3<String, String, Integer>> records = lines
				.map(new Function<String, Tuple3<String, String, Integer>>() {
					public Tuple3<String, String, Integer> call(String line) throws Exception {
						String[] elems = line.split(",");
						return new Tuple3<String, String, Integer>(elems[0], elems[1], Integer.parseInt(elems[2]));
					}
				});

		// 3.筛选女性网民上网时间数据信息
		JavaDStream<Tuple2<String, Integer>> femaleRecords = records
				.filter(new Function<Tuple3<String, String, Integer>, Boolean>() {
					public Boolean call(Tuple3<String, String, Integer> line) throws Exception {
						if (line._2().equals("female")) {
							return true;
						} else {
							return false;
						}
					}
				}).map(new Function<Tuple3<String, String, Integer>, Tuple2<String, Integer>>() {
					public Tuple2<String, Integer> call(Tuple3<String, String, Integer> stringStringIntegerTuple3)
							throws Exception {
						return new Tuple2<String, Integer>(stringStringIntegerTuple3._1(),
								stringStringIntegerTuple3._3());
					}
				});

		// 4.汇总在一个时间窗口内每个女性上网时间
		JavaPairDStream<String, Integer> aggregateRecords = JavaPairDStream.fromJavaDStream(femaleRecords)
				.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer integer, Integer integer2) throws Exception {
						return integer + integer2;
					}
				}, new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer integer, Integer integer2) throws Exception {
						return integer - integer2;
					}
				}, windowDuration, batchDuration);

		// 5.筛选连续上网时间超过阈值的用户
		JavaDStream<Tuple2<String, Integer>> upTimeUser = aggregateRecords
				.filter(new Function<Tuple2<String, Integer>, Boolean>() {
					public Boolean call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
						if (stringIntegerTuple2._2() > 0.9 * Integer.parseInt(windowTime)) {
							return true;
						} else {
							return false;
						}
					}
				}).toJavaDStream();

		// 6.kafka属性配置
		Properties producerConf = new Properties();
		producerConf.put("serializer.class", "kafka.serializer.DefaultEncoder");
		producerConf.put("key.serializer.class", "kafka.serializer.StringEncoder");
		producerConf.put("metadata.broker.list", brokers);
		producerConf.put("request.required.acks", "1");

		// 7.将结果作为消息发送到kafka,消息主题名为“default”,随机发送到一个分区
		JavaDStreamKafkaWriterFactory.fromJavaDStream(upTimeUser).writeToKafka(producerConf, new ProcessingFunc());

		// 8.Streaming系统启动
		jssc.start();
		jssc.awaitTermination();
	}

	static class ProcessingFunc implements Function<Tuple2<String, Integer>, KeyedMessage<String, byte[]>> {
		public KeyedMessage<String, byte[]> call(Tuple2<String, Integer> stringIntegerTuple2) throws Exception {
			String words = stringIntegerTuple2._1() + "," + stringIntegerTuple2._2().toString();
			return new KeyedMessage<String, byte[]>("default", null, words.getBytes());
		}
	}

}
