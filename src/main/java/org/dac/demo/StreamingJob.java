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

package org.dac.demo;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.dac.demo.util.Data;
import org.apache.flink.api.java.utils.*;

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

// *************************************************************************
	// PROGRAM
	// *************************************************************************

	private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd H:mm:ss");

	private static final String DUMMYDATE = "2019-12-14";

	private static final OutputTag<Tuple4<String, String, String, Integer>> oldVersion = new OutputTag<Tuple4<String, String, String, Integer>>("5.0007.510.011") {
	};

	private static final OutputTag<Tuple4<String, String, String, Integer>> newVersion = new OutputTag<Tuple4<String, String, String, Integer>>("5.0007.610.011") {
	};

	public static void main(String[] args) throws Exception {

		final ParameterTool params = ParameterTool.fromArgs(args);

		// Checking input parameters
		//final MultipleParameterTool params = MultipleParameterTool.fromArgs(args);

		// set up the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		// stream id cache
		HashMap<String, OutputTag<Tuple4<String, String, String, Integer>>> streamIdTable = new HashMap<>();

		// make parameters available in the web interface
		env.getConfig().setGlobalJobParameters(params);
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

		// get input data
		DataStream<String> text = null;
		if (params.has("input")) {
			text = env.readTextFile(params.get("input"));
			// union all the inputs from text files
/*			for (String input : params.get("input")) {
				if (text == null) {
					text = env.readTextFile(input);
				} else {
					text = text.union(env.readTextFile(input));
				}
			}*/
			Preconditions.checkNotNull(text, "Input DataStream should not be null.");
		} else {
			System.out.println("Executing ErrorDetection example with default input data set.");
			System.out.println("Use --input to specify file input.");
			// get default test text data
			text = env.fromElements(Data.ERROR_LOGS);
		}

		DataStream<Tuple4<String, String, String, Integer>> timestampEvents = text.flatMap(new Tokenizer()).assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<String, String, String, Integer>>() {
			@Override
			public long extractAscendingTimestamp(Tuple4<String, String, String, Integer> element) {
				LocalDateTime dateTime = LocalDateTime.parse(DUMMYDATE + ' ' + element.f0, FORMATTER);
				ZonedDateTime zdt = dateTime.atZone(ZoneId.of("America/Los_Angeles"));
				return zdt.toInstant().toEpochMilli();
			}
		});

		//outputStream: combined stream
		// split stream into multiple based on client version. Consider all possible versions, not limited to the given two.
		SingleOutputStreamOperator<Tuple4<String, String, String, Integer>> streams = timestampEvents.process(new ProcessFunction<Tuple4<String, String, String, Integer>, Tuple4<String, String, String, Integer>>() {
			@Override
			public void processElement(Tuple4<String, String, String, Integer> in, Context ctx, Collector<Tuple4<String, String, String, Integer>> out) throws Exception {
				out.collect(in);
				if (!streamIdTable.containsValue(in.f1)) {
					streamIdTable.put(in.f1, new OutputTag<Tuple4<String, String, String, Integer>>(in.f1) {
					});
				}
				ctx.output(streamIdTable.get(in.f1), in);
			}
		});

		// validate example data using default config
		final int windowSize = params.getInt("window", 1);
		final int slideSize = params.getInt("slide", 1);

//		final Boolean writeToElasticsearch = false;
//		Map<String, String> userConfig = new HashMap<>();
//		userConfig.put("cluster.name", "elasticsearch");
//		// This instructs the sink to emit after every element, otherwise they would be buffered
//		userConfig.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");

/*
		List<InetSocketAddress> transports = new ArrayList<>();
		transports.add(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 9300));
*/

		// emit result
		OutputTag[] vers = {oldVersion, newVersion};
		System.out.println("Processing...");
		for (OutputTag<Tuple4<String, String, String, Integer>> i : vers) {
			if (params.has("output")) {
				streams.getSideOutput(i)
					.keyBy(2)
					.timeWindow(Time.seconds(windowSize), Time.seconds(slideSize))
					// group by the tuple field "0" and sum up tuple field "1"
					.sum(3)
					.flatMap(new Trim())
					.writeAsText(params.get("output") + i.getId());
			} else {
				System.out.println("Printing result to stdout. Use --output to specify output path.");
				streams.getSideOutput(i).print();
			}

		/*	if (writeToElasticsearch) {
				// write to Elasticsearch
				streams.getSideOutput(i).addSink(new ElasticsearchSink<>(
					userConfig,
					transports,
					(Tuple4<String, String, String, Integer> element, RuntimeContext ctx, RequestIndexer indexer) -> {
						indexer.add(createIndexRequest(element, parameterTool));
					}));
			}*/
		}
		// execute program
		env.execute("Streaming ErrorDetection");
	}

	// *************************************************************************
	// USER FUNCTIONS
	// *************************************************************************

	/**
	 * Build tuple4 in the form of "(timestamp, version, error_code,1)".
	 */
	public static final class Tokenizer implements FlatMapFunction<String, Tuple4<String, String, String, Integer>> {

		@Override
		public void flatMap(String value, Collector<Tuple4<String, String, String, Integer>> out) {
			// normalize and split the line
			String[] tokens = value.toLowerCase().split("\\s+");
			System.out.println(tokens.toString());
			out.collect(new Tuple4<>(tokens[0], tokens[1], tokens[2], 1));
		}
	}

	/**
	 * Implements the string tokenizer that splits sentences into words as a
	 * user-defined FlatMapFunction. The function takes a line (String) and
	 * splits it into multiple pairs in the form of "(word,1)" ({@code Tuple2<String,
	 * Integer>}).
	 */
	public static final class Trim implements FlatMapFunction<Tuple4<String, String, String, Integer>, Tuple3<String, String, Integer>> {
		@Override
		public void flatMap(Tuple4<String, String, String, Integer> value, Collector<Tuple3<String, String, Integer>> out) {
			out.collect(new Tuple3<>(value.f0, value.f2, value.f3));
		}
	}

//	private static IndexRequest createIndexRequest(Tuple4<String, String, String, Integer> element, ParameterTool parameterTool) {
//		Map<String, Object> json = new HashMap<>();
//		json.put("timestamp", element.f0);
//		json.put("version", element.f1);
//		json.put("error_code", element.f2);
//		json.put("count", element.f3);
//
//		return Requests.indexRequest()
//			.index(parameterTool.getRequired("index"))
//			.type(parameterTool.getRequired("type"))
//			.id(element.toString())
//			.source(json);
//	}
}
