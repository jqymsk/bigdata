package jiq.spark;

import java.io.Serializable;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

public class SparkSQL {
	public static class FemaleInfo implements Serializable {
		private String name;
		private String gender;
		private Integer stayTime;

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public String getGender() {
			return gender;
		}

		public void setGender(String gender) {
			this.gender = gender;
		}

		public Integer getStayTime() {
			return stayTime;
		}

		public void setStayTime(Integer stayTime) {
			this.stayTime = stayTime;
		}
	}

	public static void main(String[] args) throws Exception {
		// 创建一个配置类SparkConf，然后创建一个SparkContext
		SparkConf conf = new SparkConf().setAppName("SparkSQL");
		JavaSparkContext jsc = new JavaSparkContext(conf);
		SQLContext sqlContext = new SQLContext(jsc);

		// 通过隐式转换，将RDD转换成DataFrame
		JavaRDD<FemaleInfo> femaleInfoJavaRDD = jsc.textFile(args[0]).map(new Function<String, FemaleInfo>() {
			@Override
			public FemaleInfo call(String line) throws Exception {
				String[] parts = line.split(",");

				FemaleInfo femaleInfo = new FemaleInfo();
				femaleInfo.setName(parts[0]);
				femaleInfo.setGender(parts[1]);
				femaleInfo.setStayTime(Integer.parseInt(parts[2].trim()));
				return femaleInfo;
			}
		});

		// 注册表
		DataFrame schemaFemaleInfo = sqlContext.createDataFrame(femaleInfoJavaRDD, FemaleInfo.class);
		schemaFemaleInfo.registerTempTable("FemaleInfoTable");

		// 执行SQL查询
		DataFrame femaleTimeInfo = sqlContext.sql(
				"select * from (select name,sum(stayTime) as totalStayTime from FemaleInfoTable where gender = 'female' group by name ) tmp where totalStayTime >120");

		// 显示结果
		List<String> result = femaleTimeInfo.javaRDD().map(new Function<Row, String>() {
			public String call(Row row) {
				return row.getString(0) + "," + row.getLong(1);
			}
		}).collect();
		System.out.println(result);
		jsc.stop();
	}

}
