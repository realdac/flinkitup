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

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.api.functions.timestamps.AscendingTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch6.ElasticsearchSink;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;
import org.apache.flink.util.Preconditions;
import org.apache.http.HttpHost;
import org.dac.demo.util.Data;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;

import io.airlift.log.Logger;

import static org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE;

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
	private static final Logger log = Logger.get(StreamingJob.class);

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

		DataStream<String> text = null;
		if (params.has("input")) {
			//text = env.readTextFile(params.get("input"));

			// Equivalent:
			Path filePath = new File(params.get("input")).toPath();
			Charset charset = Charset.defaultCharset();
			List<String> stringList = Files.readAllLines(filePath, charset);
			text = env.fromElements(stringList.toArray(new String[]{}));

			Preconditions.checkNotNull(text, "Input DataStream should not be null.");
		} else {
			System.out.println("Executing ErrorDetection example with default input data set.");
			System.out.println("Use --input to specify file input.");

			// get default test text data
			text = env.fromElements(Data.ERROR_LOGS);
		}

		DataStream<Tuple4<String, String, String, Integer>> timestampEvents = text.flatMap(new Tokenizer())
				.assignTimestampsAndWatermarks(new AscendingTimestampExtractor<Tuple4<String, String, String, Integer>>() {
			@Override
			public long extractAscendingTimestamp(Tuple4<String, String, String, Integer> element) {
				LocalDateTime dateTime;
				if (element.f0 != null) {
					dateTime = LocalDateTime.parse(element.f0, FORMATTER);
				}
				else {
					dateTime = LocalDateTime.parse("Null token[0]", FORMATTER);
				}

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

		//final Boolean writeToElasticsearch = false;
		Map<String, String> userConfig = new HashMap<>();
		userConfig.put("cluster.name", "elasticsearch");
		// This instructs the sink to emit after every element, otherwise they would be buffered
		userConfig.put(ElasticsearchSink.CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, "1");

		List<HttpHost> httpHosts = new ArrayList<>();
		httpHosts.add(new HttpHost("172.19.0.3", 9200, "http"));
//		httpHosts.add(new HttpHost("10.2.3.1", 9300, "http"));

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
					.writeAsText(params.get("output") + i.getId(), OVERWRITE);
				log.info("Completed writing output file " + params.get("output") + i.getId());
				log.debug("test debug");
				System.out.println("Completed writing output file " + params.get("output") + i.getId());
			} else {
				log.debug("Printing result to stdout. Use --output to specify output path.");
				System.out.println("Printing result to stdout. Use --output to specify output path.");
				System.out.println(streams.getSideOutput(i));
				streams.getSideOutput(i).print();
			}

			if (params.has("writeToElasticsearch")) {
				log.info("Write to elasticSearch");
				ElasticsearchSink.Builder<Tuple4<String, String, String, Integer>> esSinkBuilder = new ElasticsearchSink.Builder<>(
						httpHosts, new ElasticsearchSinkFunction<Tuple4<String, String, String, Integer>> (){

					public IndexRequest createIndexRequest(Tuple4<String, String, String, Integer> element) {
						Map<String, Object> json = new HashMap<>();
						json.put("timestamp", element.f0);
						json.put("version", element.f1);
						json.put("error_code", element.f2);
						json.put("count", element.f3);

						return Requests.indexRequest()
								.index("nyc-places")
								.type("popular-locations")
								.source(json);
					}

					@Override
					public void process(Tuple4<String, String, String, Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
						indexer.add(createIndexRequest(element));
					}
				});

				// configuration for the bulk requests; this instructs the sink to emit after every element, otherwise they would be buffered
				esSinkBuilder.setBulkFlushMaxActions(1);

				// provide a RestClientFactory for custom configuration on the internally created REST client
//				esSinkBuilder.setRestClientFactory(
//						restClientBuilder -> {
//							restClientBuilder.setDefaultHeaders(...)
//							restClientBuilder.setMaxRetryTimeoutMillis(...)
//							restClientBuilder.setPathPrefix(...)
//							restClientBuilder.setHttpClientConfigCallback(...)
//						}
//				);

				streams.addSink(esSinkBuilder.build());
				// write to Elasticsearch
				//streams.getSideOutput(i).addSink(new ElasticsearchSink<Tuple4<String, String, String, Integer>>(userConfig, transports, new EntryInserter()));


//				streams.getSideOutput(i).addSink(new ElasticsearchSink<>(
//					userConfig,
//					transports,
//					(Tuple4<String, String, String, Integer> element, RuntimeContext ctx, RequestIndexer indexer) -> {
//						indexer.add(createIndexRequest(element, parameterTool));
//					}));
			}
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
			String[] tokens = value.toLowerCase().trim().split("\\s+");
			// ** token[0] has leading white space/tab but couldn't be removed by trim.
			// Use following workaround. Keep only non-alphanumeric characters.
			out.collect(new Tuple4<>(DUMMYDATE + ' ' + tokens[0].replaceAll("^[^a-zA-Z0-9\\s]+|[^a-zA-Z0-9\\s]+$", ""), tokens[1], tokens[2], 1));
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

	private static IndexRequest createIndexRequest(Tuple4<String, String, String, Integer> element, ParameterTool parameterTool) {
		Map<String, Object> json = new HashMap<>();
		json.put("timestamp", element.f0);
		json.put("version", element.f1);
		json.put("error_code", element.f2);
		json.put("count", element.f3);

		return Requests.indexRequest()
			.index(parameterTool.getRequired("index"))
			.type(parameterTool.getRequired("type"))
			.id(element.toString())
			.source(json);
	}
	public static class EntryInserter
    implements ElasticsearchSinkFunction<Tuple4<String, String, String, Integer>> {

		public IndexRequest createIndexRequest(Tuple4<String, String, String, Integer> element) {
			Map<String, Object> json = new HashMap<>();
			json.put("timestamp", element.f0);
			json.put("version", element.f1);
			json.put("error_code", element.f2);
			json.put("count", element.f3);

			return Requests.indexRequest()
					.index("nyc-places")
					.type("popular-locations")
					.source(json);
		}

		@Override
		public void process(Tuple4<String, String, String, Integer> element, RuntimeContext ctx, RequestIndexer indexer) {
			indexer.add(createIndexRequest(element));
		}
	}
}
