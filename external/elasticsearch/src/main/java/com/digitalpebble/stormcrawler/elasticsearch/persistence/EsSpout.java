/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.digitalpebble.stormcrawler.elasticsearch.persistence;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Queue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.elasticsearch.hadoop.EsHadoopIllegalStateException;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.PartitionDefinition;
import org.elasticsearch.hadoop.rest.RestService;
import org.elasticsearch.hadoop.rest.RestService.MultiReaderIterator;
import org.elasticsearch.hadoop.serialization.builder.JdkValueReader;
import org.elasticsearch.storm.cfg.StormSettings;
import org.elasticsearch.storm.cfg.TupleFailureHandling;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.util.ConfUtils;

@SuppressWarnings({ "rawtypes", "unchecked" })
public class EsSpout extends BaseRichSpout {

    private static final String ESBoltType = "status";

    private static final String ESStatusIndexNameParamName = "es." + ESBoltType
            + ".index.name";
    private static final String ESStatusDocTypeParamName = "es." + ESBoltType
            + ".doc.type";

    private transient static Log log = LogFactory.getLog(EsSpout.class);

    private transient SpoutOutputCollector collector;
    private transient MultiReaderIterator iterator;

    private List<String> tupleFields;

    private boolean ackReads = false;
    private int queueSize = 0;
    private Map<Object, Object> inTransitQueue;
    private Queue<Object[]> replayQueue = null;
    private Map<Object, Integer> retries;
    // keep on trying
    private Integer tupleRetries = Integer.valueOf(-1);
    private TupleFailureHandling tupleFailure = null;

    @Override
    public void open(Map conf, TopologyContext context,
            SpoutOutputCollector collector) {
        this.collector = collector;

        StormSettings settings = new StormSettings(conf);

        InitializationUtils.setValueReaderIfNotSet(settings,
                JdkValueReader.class, log);

        String indexName = ConfUtils.getString(conf, ESStatusIndexNameParamName,
                "status");
        String docType = ConfUtils.getString(conf, ESStatusDocTypeParamName,
                "status");

        settings.setResourceRead(indexName + "/" + docType);

        tupleFields = new LinkedList<>();
        tupleFields.add("url");
        tupleFields.add("metadata");

        ackReads = settings.getStormSpoutReliable();

        if (ackReads) {
            inTransitQueue = new LinkedHashMap<Object, Object>();
            replayQueue = new LinkedList<Object[]>();
            retries = new HashMap<Object, Integer>();
            queueSize = settings.getStormSpoutReliableQueueSize();
            tupleRetries = settings.getStormSpoutReliableRetriesPerTuple();
            tupleFailure = settings.getStormSpoutReliableTupleFailureHandling();
        }

        int totalTasks = context.getComponentTasks(context.getThisComponentId())
                .size();
        int currentTask = context.getThisTaskIndex();

        // match the partitions based on the current topology
        List<PartitionDefinition> partitions = RestService
                .findPartitions(settings, log);
        List<PartitionDefinition> assigned = RestService
                .assignPartitions(partitions, currentTask, totalTasks);
        iterator = RestService.multiReader(settings, assigned, log);
    }

    @Override
    public void close() {
        if (replayQueue != null) {
            replayQueue.clear();
            replayQueue = null;
        }

        if (retries != null) {
            retries.clear();
            retries = null;
        }

        if (inTransitQueue != null) {
            inTransitQueue.clear();
            inTransitQueue = null;
        }

        if (iterator != null) {
            iterator.close();
        }
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public void nextTuple() {
        // 0 - docId, 1 - doc
        Object[] next = null;

        if (replayQueue != null && !replayQueue.isEmpty()) {
            next = replayQueue.poll();
        } else if (iterator.hasNext()) {
            next = iterator.next();
        }

        if (next != null) {
            List<Object> tuple = createTuple(next[1]);

            if (ackReads) {
                if (queueSize > 0) {
                    if (inTransitQueue.size() >= queueSize) {
                        throw new EsHadoopIllegalStateException(String.format(
                                "Ack-tuples queue has exceeded the specified size [%s]",
                                inTransitQueue.size()));
                    }
                    inTransitQueue.put(next[0], next[1]);
                }

                collector.emit(tuple, next[0]);
            } else {
                collector.emit(tuple);
            }
        } else {
            // per doc indication
            try {
                Thread.sleep(1);
            } catch (InterruptedException ex) {
                // interrupted sleep - go on
            }
        }
    }

    private List<Object> createTuple(Object value) {
        List<Object> tuple = new ArrayList<Object>(2);
        Map<String, Object> doc = (Map<String, Object>) value;
        String url = (String) doc.get("url");
        tuple.add(url);
        Metadata m = fromKeyValues((Map) doc.get("metadata"));
        tuple.add(m);
        return tuple;
    }

    protected final Metadata fromKeyValues(Map<String, Object> keyValues) {
        Map<String, List<String>> mdAsMap = (Map<String, List<String>>) keyValues
                .get("metadata");
        Metadata metadata = new Metadata();
        if (mdAsMap != null) {
            Iterator<Entry<String, List<String>>> mdIter = mdAsMap.entrySet()
                    .iterator();
            while (mdIter.hasNext()) {
                Entry<String, List<String>> mdEntry = mdIter.next();
                String key = mdEntry.getKey();
                // periods are not allowed in ES2 - replace with %2E
                key = key.replaceAll("%2E", "\\.");
                Object mdValObj = mdEntry.getValue();
                // single value
                if (mdValObj instanceof String) {
                    metadata.addValue(key, (String) mdValObj);
                }
                // multi valued
                else {
                    metadata.addValues(key, (List<String>) mdValObj);
                }
            }
        }
        return metadata;
    }

    @Override
    public void ack(Object msgId) {
        inTransitQueue.remove(msgId);
        retries.remove(msgId);
    }

    @Override
    public void fail(Object msgId) {
        Object tuple = inTransitQueue.remove(msgId);
        Integer attempts = retries.remove(msgId);
        if (attempts == null) {
            attempts = tupleRetries;
        }

        int primitive = attempts.intValue();
        if (primitive == 0) {
            switch (tupleFailure) {
            case ABORT:
                throw new EsHadoopIllegalStateException(String.format(
                        "Tuple [%s] has failed to be fully processed after [%d] retries; aborting...",
                        tuple, attempts));
            case WARN:
                log.warn(String.format(
                        "Tuple [%s] has failed to be fully processed after [%d] retries; aborting...",
                        tuple, attempts));
            case IGNORE: // move on
            }
            return;
        }
        if (primitive > 0) {
            primitive--;
        }
        // negative means keep on trying

        // retry
        retries.put(msgId, Integer.valueOf(primitive));
        replayQueue.add(new Object[] { msgId, tuple });
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("url", "metadata"));
    }
}