/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple5;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
//import org.apache.flink.streaming.util.serialization.SimpleStringSchema;
import org.apache.flink.util.Collector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import java.util.Properties;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>To package your application into a JAR file for execution, run
 * 'mvn clean package' on the command line.
 *
 * <p>If you change the name of the main class (with the public static void main(String[] args))
 * method, change the respective entry in the POM.xml file (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.enableCheckpointing(5000);//5 seconds check
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "10.13.30.161:9092");
		properties.setProperty("zookeeper.connect", "10.13.30.161:2181");
		properties.setProperty("group.id","security_group");
		FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>("nginx", new SimpleStringSchema(), properties);
		//myConsumer.setStartFromEarliest();//start from the latest record
		myConsumer.setStartFromLatest();//start from the latest record
		DataStream<String> stream = env.addSource(myConsumer);
		DataStreamSource<String> kafkaData = env.addSource(myConsumer);
		//stream.print();
		//定义数据结构
		SingleOutputStreamOperator<Tuple5<Long, Long, Long, String, Long>> spiderData= kafkaData.map(new MapFunction<String, Tuple5<Long, Long, Long, String, Long>>() {
			@Override
			public Tuple5<Long, Long, Long, String, Long> map(String s) throws Exception {
				return null;
			}
		});
        //主处理逻辑
        DataStream<Tuple2<String, Integer>> counts = stream.flatMap(new LineSplitter()).keyBy(0).sum(1);
		//实例化Flink和Redis关联类
		FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("127.0.0.1").setPort(6379).setPassword("kMn9tdlTnB8dQsMw").build();
		//实例化RedisSink，并通过fllink的addSink的方式将flink计算的结果接入到Redis，Tuple2代表计算值2个
        counts.addSink(new RedisSink<Tuple2<String, Integer>>(conf,new RedisExampleMapper()));

		/*l
		 * Here, you can start creating your execution plan for Flink.
		 *
		 * Start with getting some data from the environment, like
		 * 	env.readTextFile(textPath);
		 *
		 * then, transform the resulting DataStream<String> using operations
		 * like
		 * 	.filter()
		 * 	.flatMap()
		 * 	.join()
		 * 	.coGroup()
		 *
		 * and many more.
		 * Have a look at the programming guide for the Java API:
		 *
		 * http://flink.apache.org/docs/latest/apis/streaming/index.html
		 *
		 */

		// execute program
		env.execute("WordCount from Kafka data");
	}

    //指定Redis key并将flink数据类型映射到Redis数据类型
    public static final class RedisExampleMapper implements RedisMapper<Tuple2<String,Integer>>{
        //设置使用的redis数据结构类型，和key的名词
		public RedisCommandDescription getCommandDescription() {
            return new RedisCommandDescription(RedisCommand.SET, null);//存储HASH类型进Redis
        }
		//设置value中的键值对 key值
        public String getKeyFromData(Tuple2<String, Integer> data) {
            return data.f0;
        }
		//设置value中的键值对value的值
        public String getValueFromData(Tuple2<String, Integer> data) {
            return data.f1.toString();
        }
    }

    public static final class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        private static final long serialVersionUID = 1L;
		//主逻辑函数
        public void flatMap(String value, Collector<Tuple2<String, Integer>> out) {
            String[] tokens = value.toLowerCase().split("\\s+");//字符串变小写后再匹配一个或多个空格分割
			//Pattern pattern = Pattern.compile("cookie");
			Pattern pattern =  Pattern.compile("^((?!uuid=).)*http://|https://*$");
			//Matcher matcher = new Matcher;
            for (String token : tokens) {
                 Matcher matcher = pattern.matcher(token);
                 while(matcher.find()) {
					out.collect(new Tuple2<String, Integer>(token, 1));
				}
            }
        }
    }
}
