/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.stormcrawler.elasticsearch.persistence;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.Config;
import org.apache.storm.metric.api.IMetric;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.utils.TupleUtils;
import org.elasticsearch.hadoop.EsHadoopException;
import org.elasticsearch.hadoop.cfg.ConfigurationOptions;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.RestService;
import org.elasticsearch.hadoop.rest.RestService.PartitionWriter;
import org.elasticsearch.hadoop.serialization.MapFieldExtractor;
import org.elasticsearch.hadoop.serialization.builder.JdkValueWriter;
import org.elasticsearch.hadoop.util.unit.TimeValue;
import org.elasticsearch.storm.cfg.StormSettings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.Metadata;
import com.digitalpebble.stormcrawler.persistence.AbstractStatusUpdaterBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;
import com.digitalpebble.stormcrawler.util.URLPartitioner;

/**
 * Simple bolt which stores the status of URLs into ElasticSearch. Takes the
 * tuples coming from the 'status' stream. To be used in combination with a
 * Spout to read from the index.
 **/
@SuppressWarnings("serial")
public class StatusUpdaterBolt extends AbstractStatusUpdaterBolt {

    private static final Logger LOG = LoggerFactory
            .getLogger(StatusUpdaterBolt.class);

    private transient static Log log = LogFactory
            .getLog(StatusUpdaterBolt.class);

    private static final String ESBoltType = "status";

    private static final String ESStatusIndexNameParamName = "es." + ESBoltType
            + ".index.name";
    private static final String ESStatusDocTypeParamName = "es." + ESBoltType
            + ".doc.type";
    private static final String ESStatusRoutingParamName = "es." + ESBoltType
            + ".routing";
    private static final String ESStatusRoutingFieldParamName = "es."
            + ESBoltType + ".routing.fieldname";
    private static final String ESFlushInterParamName = "es." + ESBoltType
            + ".flushInterval";

    private boolean routingFieldNameInMetadata = false;

    private String indexName;
    private String docType;

    private URLPartitioner partitioner;

    /**
     * whether to apply the same partitioning logic used for politeness for
     * routing, e.g byHost
     **/
    private boolean doRouting;

    /** Store the key used for routing explicitly as a field in metadata **/
    private String fieldNameForRoutingKey = null;

    private MultiCountMetric eventCounter;

    private PartitionWriter writer;

    private transient List<Tuple> inflightTuples = null;

    private transient TimeValue flushInterval;

    private int numberOfEntries;

    private long timeSinceLastFlush = 0;

    @Override
    public void prepare(Map stormConf, TopologyContext context,
            OutputCollector collector) {

        super.prepare(stormConf, context, collector);

        indexName = ConfUtils.getString(stormConf,
                StatusUpdaterBolt.ESStatusIndexNameParamName, "status");
        docType = ConfUtils.getString(stormConf,
                StatusUpdaterBolt.ESStatusDocTypeParamName, "status");

        doRouting = ConfUtils.getBoolean(stormConf,
                StatusUpdaterBolt.ESStatusRoutingParamName, false);

        if (doRouting) {
            partitioner = new URLPartitioner();
            partitioner.configure(stormConf);
            fieldNameForRoutingKey = ConfUtils.getString(stormConf,
                    StatusUpdaterBolt.ESStatusRoutingFieldParamName);
            if (StringUtils.isNotBlank(fieldNameForRoutingKey)) {
                if (fieldNameForRoutingKey.startsWith("metadata.")) {
                    routingFieldNameInMetadata = true;
                    fieldNameForRoutingKey = fieldNameForRoutingKey
                            .substring("metadata.".length());
                }
                // periods are not allowed in ES2 - replace with %2E
                fieldNameForRoutingKey = fieldNameForRoutingKey
                        .replaceAll("\\.", "%2E");
            }
        }

        // create gauge for waitAck
        context.registerMetric("waitAck", new IMetric() {
            @Override
            public Object getValueAndReset() {
                return inflightTuples.size();
            }
        }, 30);

        this.eventCounter = context.registerMetric("counters",
                new MultiCountMetric(), 30);

        String flushIntervalString = ConfUtils.getString(stormConf,
                ESFlushInterParamName, "5s");

        flushInterval = TimeValue.parseTimeValue(flushIntervalString);

        StormSettings settings = new StormSettings(stormConf);

        // align Bolt / es-hadoop batch settings
        numberOfEntries = ConfUtils.getInt(stormConf,
                "es." + ESBoltType + ".bulkActions", 50);

        settings.setProperty(ConfigurationOptions.ES_BATCH_SIZE_ENTRIES,
                String.valueOf(numberOfEntries));

        inflightTuples = new ArrayList<Tuple>(numberOfEntries + 1);

        int totalTasks = context.getComponentTasks(context.getThisComponentId())
                .size();

        InitializationUtils.setValueWriterIfNotSet(settings,
                JdkValueWriter.class, log);

        InitializationUtils.setFieldExtractorIfNotSet(settings,
                MapFieldExtractor.class, log);

        // InitializationUtils.setBytesConverterIfNeeded(settings,
        // StormTupleBytesConverter.class, log);

        settings.setResourceWrite(indexName + "/" + docType);

        settings.setProperty("es.mapping.id", "sha256");

        writer = RestService.createWriter(settings, context.getThisTaskIndex(),
                totalTasks, log);
    }

    private void flush() {
        BitSet flush = null;

        try {
            flush = writer.repository.tryFlush().getLeftovers();
        } catch (EsHadoopException ex) {
            // fail all recorded tuples
            for (Tuple input : inflightTuples) {
                _collector.fail(input);
            }
            inflightTuples.clear();
            throw ex;
        }

        for (int index = 0; index < inflightTuples.size(); index++) {
            Tuple tuple = inflightTuples.get(index);
            // bit set means the entry hasn't been removed and thus wasn't
            // written to ES
            if (flush.get(index)) {
                _collector.fail(tuple);
            } else {
                _collector.ack(tuple);
            }
        }

        // clear everything in bulk to prevent 'noisy' remove()
        inflightTuples.clear();

        timeSinceLastFlush = System.currentTimeMillis();
    }

    @Override
    public void cleanup() {
        if (writer != null) {
            try {
                flush();
            } finally {
                writer.close();
                writer = null;
            }
        }
    }

    public void execute(Tuple tuple) {

        // set it to the first tuple received
        if (timeSinceLastFlush == 0) {
            timeSinceLastFlush = System.currentTimeMillis();
        }

        // do we need flushing?
        long now = System.currentTimeMillis();
        if (now - timeSinceLastFlush > flushInterval.getMillis()) {
            flush();
        }

        if (TupleUtils.isTick(tuple)) {
            _collector.ack(tuple);
            return;
        }

        super.execute(tuple);
    }

    @Override
    public void store(String url, Status status, Metadata metadata,
            Date nextFetch) throws Exception {

        String partitionKey = null;

        if (doRouting) {
            partitionKey = partitioner.getPartition(url, metadata);
        }

        Map<String, Object> builder = new HashMap<>();

        builder.put("url", url);
        builder.put("status", status.toString());

        // check that we don't overwrite an existing entry
        // When create is used, the index operation will fail if a document
        // by that id already exists in the index.
        boolean create = status.equals(Status.DISCOVERED);

        Map<String, Object> md = new HashMap<>();

        Iterator<String> mdKeys = metadata.keySet().iterator();
        while (mdKeys.hasNext()) {
            String mdKey = mdKeys.next();
            String[] values = metadata.getValues(mdKey);
            // periods are not allowed in ES2 - replace with %2E
            mdKey = mdKey.replaceAll("\\.", "%2E");
            md.put(mdKey, values);
        }

        // store routing key in metadata?
        if (StringUtils.isNotBlank(partitionKey)
                && StringUtils.isNotBlank(fieldNameForRoutingKey)
                && routingFieldNameInMetadata) {
            md.put(fieldNameForRoutingKey, partitionKey);
        }

        builder.put("metadata", md);

        // store routing key outside metadata?
        if (StringUtils.isNotBlank(partitionKey)
                && StringUtils.isNotBlank(fieldNameForRoutingKey)
                && !routingFieldNameInMetadata) {
            builder.put(fieldNameForRoutingKey, partitionKey);
        }

        builder.put("nextFetchDate", nextFetch);

        String sha256hex = org.apache.commons.codec.digest.DigestUtils
                .sha256Hex(url);

        builder.put("sha256", sha256hex);

        // TODO id based on sha + routing + create?

        // IndexRequestBuilder request = connection.getClient()
        // .prepareIndex(indexName, docType).setSource(builder)
        // .setCreate(create).setId(sha256hex);
        //
        // if (StringUtils.isNotBlank(partitionKey)) {
        // request.setRouting(partitionKey);
        // }

        writer.repository.writeToIndex(builder);

        LOG.debug("Sent to ES buffer {}", url);
    }

    /**
     * Do not ack the tuple straight away! wait to get the confirmation that it
     * worked
     **/
    public void ack(Tuple t, String url) {
        inflightTuples.add(t);
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // configure how often a tick tuple will be sent to our bolt
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
    }

}