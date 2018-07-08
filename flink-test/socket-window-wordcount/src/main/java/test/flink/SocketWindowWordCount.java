package test.flink;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.io.InputStream;

@Slf4j
public class SocketWindowWordCount {
	public static void main(String[] args) throws Exception {
		// the port to connect to
		final int port;
		try {
			final ParameterTool params = ParameterTool.fromArgs(args);
			port = params.getInt("port");
			log.info("socket port: {}", port);
		} catch (Exception e) {
			System.err.println("No port specified. Please run 'SocketWindowWordCount --port <port>'");
			return;
		}
		
		processPropertiesFile();
		
		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		
		// get input data by connecting to the socket
		DataStream<String> text = env.socketTextStream("localhost", port, "\n");
		
		// parse the data, group it, window it, and aggregate the counts
		DataStream<WordWithCount> windowCounts = text
				.flatMap(new FlatMapFunction<String, WordWithCount>() {
					@Override
					public void flatMap(String value, Collector<WordWithCount> out) {
						for (String word : value.split("\\s")) {
							out.collect(new WordWithCount(word, 1L));
						}
					}
				})
				.keyBy("word")
				.timeWindow(Time.seconds(5), Time.seconds(1))
				.reduce(new ReduceFunction<WordWithCount>() {
					@Override
					public WordWithCount reduce(WordWithCount a, WordWithCount b) {
						return new WordWithCount(a.word, a.count + b.count);
					}
				});
		
		// print the results with a single thread, rather than in parallel
		windowCounts.print().setParallelism(1);
		
		env.execute("Socket Window WordCount");
	}
	
	private static void processPropertiesFile() throws IOException {
		InputStream in = SocketWindowWordCount.class.getClassLoader().getResourceAsStream("test.properties");
		ParameterTool params = ParameterTool.fromPropertiesFile(in);
		log.info("params count: {}", params.getNumberOfParameters());
		params.toMap().entrySet().forEach(e -> log.info("\tkey: {} = value: {}", e.getKey(), e.getValue()));
	}
	
	// Data type for words with count
	public static class WordWithCount {
		
		public String word;
		public long count;
		
		public WordWithCount() {
		}
		
		public WordWithCount(String word, long count) {
			this.word = word;
			this.count = count;
		}
		
		@Override
		public String toString() {
			return word + " : " + count;
		}
	}
}