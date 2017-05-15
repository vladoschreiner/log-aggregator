/*
 * Copyright (c) 2008-2017, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import com.hazelcast.jet.DAG;
import com.hazelcast.jet.Jet;
import com.hazelcast.jet.JetInstance;
import com.hazelcast.jet.Vertex;
import com.hazelcast.jet.sample.Log;
import com.hazelcast.jet.sample.LogDurationFrameComparator;
import com.hazelcast.jet.stream.DistributedCollectors;
import com.hazelcast.jet.windowing.Frame;
import com.hazelcast.jet.windowing.WindowDefinition;
import com.hazelcast.jet.windowing.WindowOperation;
import com.hazelcast.jet.Distributed.Optional;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Paths;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.AbstractMap;
import java.util.Map;

import static com.hazelcast.jet.Edge.between;
import static com.hazelcast.jet.Partitioner.HASH_CODE;
import static com.hazelcast.jet.Processors.map;
import static com.hazelcast.jet.Processors.writeFile;
import static com.hazelcast.jet.impl.connector.ReadWithPartitionIteratorP.readMap;
import static com.hazelcast.jet.sample.GenerateLogsP.MAX_LAG;
import static com.hazelcast.jet.sample.GenerateLogsP.generateLogs;
import static com.hazelcast.jet.windowing.PunctuationPolicies.cappingEventSeqLagAndRetention;
import static com.hazelcast.jet.windowing.WindowDefinition.slidingWindowDef;
import static com.hazelcast.jet.windowing.WindowingProcessors.*;

/**
 * <pre>
 *             ------------
 *            | log-source |  read list of source systems
 *             ------------
 *                    |
 *              (source_system)
 *                    V
 *            -----------------
 *           | generate-logs   | generate random logs records with response times
 *            -----------------
 *                    |
 *       (timestamp, source, duration)
 *                    V
 *          --------------------
 *         | insert-punctuation |   (based on timestamp)
 *          --------------------
 *                    |
 *                    V             partitioned
 *            ----------------
 *           | group-by-frame |
 *            ----------------
 *                    |
 *          (timestamp, time, count)   partitioned, distribution
 *                    V
 *            ----------------
 *           | sliding-window |     average response per system, 1 sec thumbling window
 *            ----------------
 *                    |
 *       Frame<source system, second average>
 *                    |
 *                    V
 *          --------------------
 *         | reformat-frame     |   (for all records to have same key - we need global windows to do global maximum
 *          --------------------
 *                    |
 *       Frame<"global", MapEntry<source system, average>> partitioned, distributed
 *                    |
 *                    V
 *            ------------------
 *           | group-by-frame-2 |     group frames from all the systems having the same "second". Evict with punctuation.
 *            ------------------
 *                    |
 *                    |  partitioning, distribution
 *                    V
 *            ------------------
 *           | sliding-window-2 |    slowest system (based on average response) in last 3 sec, sliding by 1s
 *            ------------------
 *                    |
 *                    V
 *            ---------------
 *           | format-output |
 *            ---------------
 *                    |
 *          "Frame_time system time avg_response_duration"
 *                    V
 *                 ------
 *                | sink |
 *                 ------
 * </pre>
 */
public class LogAggregation {

    private static final String SYSTEM_MAP_NAME = "systems";
    private static final String OUTPUT_DIR_NAME = "output";

    private static final int SERVICE_TUMBLING_WINDOW_LENGTH_MILLIS = 1000;

    private static final int SLOWEST_SERVICE_SLIDING_WINDOW_LENGTH = 3000;
    private static final int SLOWEST_SERVICE_SLIDING_STEP = 1000;

    public static void main(String[] args) throws Exception {
        System.setProperty("hazelcast.logging.type", "log4j");
        JetInstance jet = Jet.newJetInstance();

        try {
            loadSystems(jet); // naplni mapu kodama stocku
            jet.newJob(buildDag()).execute();
            Thread.sleep(10_000);
        } finally {
            Jet.shutdownAll();
        }
    }

    private static DAG buildDag() {
        DAG dag = new DAG();
        WindowDefinition systemAverageWindowDef = slidingWindowDef(SERVICE_TUMBLING_WINDOW_LENGTH_MILLIS,
                SERVICE_TUMBLING_WINDOW_LENGTH_MILLIS);

        WindowDefinition slowestSystemWindowDef = slidingWindowDef(SLOWEST_SERVICE_SLIDING_WINDOW_LENGTH,
                SLOWEST_SERVICE_SLIDING_STEP);

        int logsPerSecPerMember = 1_000_000;
        DateTimeFormatter timeFormat = DateTimeFormatter.ofPattern("ss.SS");

        // reads list of services, batch read
        Vertex logSource = dag.newVertex("log-source", readMap(SYSTEM_MAP_NAME));
        logSource.localParallelism(1);

        // converts batch to stream
        // generates continuous Log stream for services provided
        // simulates unordered stream, no lag > maxLag, therefore no late events
        Vertex generateLogs = dag.newVertex("generate-logs", generateLogs(logsPerSecPerMember));
        generateLogs.localParallelism(1);


        // compute average response time per serivce in tumbling windows
        Vertex insertPunctuation = dag.newVertex("insert-punctuation",
                insertPunctuation(Log::getTime, () -> cappingEventSeqLagAndRetention(MAX_LAG, 100)
                        .throttleByFrame(systemAverageWindowDef)));

        Vertex groupByFrame = dag.newVertex("group-by-frame",
                groupByFrame(Log::getService, Log::getTime, systemAverageWindowDef, WindowOperation.fromCollector(
                        DistributedCollectors.averagingInt(Log::getDuration))));

        Vertex slidingWin = dag.newVertex("sliding-window", slidingWindow(systemAverageWindowDef,
                WindowOperation.fromCollector(
                        DistributedCollectors.averagingInt(Log::getDuration) )));

        // Output is Frame<source system, second average> f)
        // Reformat to Frame<"global", MapEntry<source system, average>>, to introduce single key

        // Slowest average response in last time period (sliding window)
        Vertex reformatFrame = dag.newVertex("reformat-frame",
                map( (Frame<String, Double> f) -> new Frame(f.getSeq(), "global",
                        new AbstractMap.SimpleEntry<String, Double>(f.getKey(), f.getValue()))));

        // the computation is not paralell from now -- we have to see all the frames to choose global maximum
        // (nevertheless, the flow is very small: just `service count` frames per SERVICE_TUMBLING_WINDOW_LENGTH_MILLIS

        // Assign avg frames to buckets based on it's seqId identifier from previous windowing step
        Vertex groupByFrame2 = dag.newVertex("group-by-frame-2",
                groupByFrame(Frame::getKey, Frame::getSeq, slowestSystemWindowDef, WindowOperation.fromCollector(
                        DistributedCollectors.maxBy(LogDurationFrameComparator.DURATION_FRAME_COMPARATOR))));
        groupByFrame2.localParallelism(1);

        // emit window with received punctuation, compute average
        Vertex slidingWin2 = dag.newVertex("sliding-window-2", slidingWindow(slowestSystemWindowDef,
                WindowOperation.fromCollector(
                        DistributedCollectors.maxBy(LogDurationFrameComparator.DURATION_FRAME_COMPARATOR) )));
        slidingWin2.localParallelism(1);

        // reformat to strings suitable for output
        Vertex formatOutput = dag.newVertex("format-output",
                map((Frame<String, Optional<Frame<String, Map.Entry<String, Double>>>> f) ->
                        String.format("Slot %s, service %s at %s, response time %4f",
                        timeFormat.format(Instant.ofEpochMilli(f.getSeq()).atZone(ZoneId.systemDefault())),
                        f.getValue().get().getValue().getKey(),
                        timeFormat.format(Instant.ofEpochMilli(f.getValue().get().getSeq()).atZone(ZoneId.systemDefault())),
                        f.getValue().get().getValue().getValue())));
        formatOutput.localParallelism(1);

        // write to output file
        Vertex sink = dag.newVertex("sink", writeFile(Paths.get(OUTPUT_DIR_NAME).toString()));
        sink.localParallelism(1);

        dag
                .edge(between(logSource, generateLogs).oneToMany())
                .edge(between(generateLogs, insertPunctuation).oneToMany())
                .edge(between(insertPunctuation, groupByFrame).partitioned(Log::getService, HASH_CODE))
                .edge(between(groupByFrame, slidingWin).partitioned(Frame<Object, Object>::getKey)
                        .distributed())
                .edge(between(slidingWin, reformatFrame))
                // key is "global" up this point, so everything will end up on one processor
                .edge(between(reformatFrame,groupByFrame2).partitioned(Frame<Object, Object>::getKey).distributed())
                .edge(between(groupByFrame2,slidingWin2).partitioned(Frame<Object, Object>::getKey).distributed())
                .edge(between(slidingWin2, formatOutput).oneToMany())
                .edge(between(formatOutput, sink).oneToMany());

        return dag;
    }

    private static void loadSystems(JetInstance jet) {
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(
                LogAggregation.class.getResourceAsStream("/sources.txt")))
        ) {
            Map<String, Integer> tickers = jet.getMap(SYSTEM_MAP_NAME);
            reader.lines()
                  .skip(1)
                  .limit(100)
                  .map(l -> l.split("\\|")[0])
                  .forEach(t -> tickers.put(t, 0));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
