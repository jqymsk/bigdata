package jiq.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;
import scala.Tuple3;

public class SparkCore {

	public static void main(String[] args) throws Exception {
		// 创建一个配置类SparkConf，然后创建一个SparkContext
		SparkConf conf = new SparkConf().setAppName("SparkCore");
		JavaSparkContext jsc = new JavaSparkContext(conf);

		// 读取原文件数据,每一行记录转成RDD里面的一个元素
		JavaRDD<String> data = jsc.textFile(args[0]);

		// 将每条记录的每列切割出来，生成一个Tuple
		JavaRDD<Tuple3<String, String, Integer>> person = data
				.map(new Function<String, Tuple3<String, String, Integer>>() {
					private static final long serialVersionUID = -2381522520231963249L;

					@Override
					public Tuple3<String, String, Integer> call(String s) throws Exception {
						// 按逗号分割一行数据
						String[] tokens = s.split(",");

						// 将分割后的三个元素组成一个三元Tuple
						Tuple3<String, String, Integer> person = new Tuple3<String, String, Integer>(tokens[0],
								tokens[1], Integer.parseInt(tokens[2]));
						return person;
					}
				});

		// 使用filter函数筛选出女性网民上网时间数据信息
		JavaRDD<Tuple3<String, String, Integer>> female = person
				.filter(new Function<Tuple3<String, String, Integer>, Boolean>() {
					private static final long serialVersionUID = -4210609503909770492L;

					@Override
					public Boolean call(Tuple3<String, String, Integer> person) throws Exception {
						// 根据第二列性别，筛选出是female的记录
						Boolean isFemale = person._2().equals("female");
						return isFemale;
					}
				});

		// 汇总每个女性上网总时间
		JavaPairRDD<String, Integer> females = female
				.mapToPair(new PairFunction<Tuple3<String, String, Integer>, String, Integer>() {
					private static final long serialVersionUID = 8313245377656164868L;

					@Override
					public Tuple2<String, Integer> call(Tuple3<String, String, Integer> female) throws Exception {
						// 取出姓名和停留时间两列，用于后面按名字求逗留时间的总和
						Tuple2<String, Integer> femaleAndTime = new Tuple2<String, Integer>(female._1(), female._3());
						return femaleAndTime;
					}
				});

		JavaPairRDD<String, Integer> femaleTime = females.reduceByKey(new Function2<Integer, Integer, Integer>() {
			private static final long serialVersionUID = -3271456048413349559L;

			@Override
			public Integer call(Integer integer, Integer integer2) throws Exception {
				// 将同一个女性的两次停留时间相加，求和
				return (integer + integer2);
			}
		});

		// 筛选出停留时间大于两个小时的女性网民信息
		JavaPairRDD<String, Integer> rightFemales = femaleTime.filter(new Function<Tuple2<String, Integer>, Boolean>() {
			private static final long serialVersionUID = -3178168214712105171L;

			@Override
			public Boolean call(Tuple2<String, Integer> s) throws Exception {
				// 取出女性用户的总停留时间，并判断是否大于2小时
				if (s._2() > (2 * 60)) {
					return true;
				}
				return false;
			}
		});

		// 对符合的female信息进行打印显示
		for (Tuple2<String, Integer> d : rightFemales.collect()) {
			System.out.println(d._1() + "," + d._2());
		}

		jsc.stop();
	}

}
