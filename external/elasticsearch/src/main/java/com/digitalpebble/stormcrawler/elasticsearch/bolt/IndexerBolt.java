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

package com.digitalpebble.stormcrawler.elasticsearch.bolt;

import static com.digitalpebble.stormcrawler.Constants.StatusStreamName;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.Config;
import org.apache.storm.metric.api.MultiCountMetric;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
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
import com.digitalpebble.stormcrawler.indexing.AbstractIndexerBolt;
import com.digitalpebble.stormcrawler.persistence.Status;
import com.digitalpebble.stormcrawler.util.ConfUtils;

@SuppressWarnings("serial")
public class IndexerBolt extends AbstractIndexerBolt {

    private static final Logger LOG = LoggerFactory
            .getLogger(IndexerBolt.class);

    private transient static Log log = LogFactory.getLog(IndexerBolt.class);

    private static final String ESBoltType = "indexer";

    private static final String ESIndexNameParamName = "es." + ESBoltType
            + ".index.name";
    private static final String ESDocTypeParamName = "es." + ESBoltType
            + ".doc.type";
    private static final String ESFlushInterParamName = "es." + ESBoltType
            + ".flushInterval";

    private OutputCollector _collector;

    private String indexName;
    private String docType;

    private MultiCountMetric eventCounter;

    private transient PartitionWriter writer;

    private transient List<Tuple> inflightTuples = null;

    private transient TimeValue flushInterval;

    private int numberOfEntries;

    private long timeSinceLastFlush = 0;

    @SuppressWarnings({ "unchecked", "rawtypes" })
    @Override
    public void prepare(Map conf, TopologyContext context,
            OutputCollector collector) {
        super.prepare(conf, context, collector);
        _collector = collector;

        indexName = ConfUtils.getString(conf, IndexerBolt.ESIndexNameParamName,
                "fetcher");
        docType = ConfUtils.getString(conf, IndexerBolt.ESDocTypeParamName,
                "doc");

        this.eventCounter = context.registerMetric("ElasticSearchIndexer",
                new MultiCountMetric(), 10);

        String flushIntervalString = ConfUtils.getString(conf,
                ESFlushInterParamName, "5s");

        flushInterval = TimeValue.parseTimeValue(flushIntervalString);

        LinkedHashMap copy = new LinkedHashMap(conf);
        copy.putAll(conf);

        StormSettings settings = new StormSettings(copy);

        // align Bolt / es-hadoop batch settings
        numberOfEntries = ConfUtils.getInt(conf,
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

    @Override
    public void execute(Tuple tuple) {

        // set it to the first tuple received
        if (timeSinceLastFlush == 0) {
            timeSinceLastFlush = System.currentTimeMillis();
        }

        // do we need flushing?
        if (System.currentTimeMillis() - timeSinceLastFlush > flushInterval
                .getMillis()) {
            flush();
        }

        if (TupleUtils.isTick(tuple)) {
            _collector.ack(tuple);
            return;
        }

        String url = tuple.getStringByField("url");

        // Distinguish the value used for indexing
        // from the one used for the status
        String normalisedurl = valueForURL(tuple);

        Metadata metadata = (Metadata) tuple.getValueByField("metadata");
        String text = tuple.getStringByField("text");

        boolean keep = filterDocument(metadata);
        if (!keep) {
            eventCounter.scope("Filtered").incrBy(1);
            // treat it as successfully processed even if
            // we do not index it
            _collector.emit(StatusStreamName, tuple,
                    new Values(url, metadata, Status.FETCHED));
            _collector.ack(tuple);
            return;
        }

        Map<String, Object> builder = new HashMap<>();

        // display text of the document?
        if (fieldNameForText() != null) {
            builder.put(fieldNameForText(), text);
        }

        // send URL as field?
        if (fieldNameForURL() != null) {
            builder.put(fieldNameForURL(), normalisedurl);
        }

        // which metadata to display?
        builder.putAll(filterMetadata(metadata));

        String sha256hex = org.apache.commons.codec.digest.DigestUtils
                .sha256Hex(normalisedurl);

        // declare it as id but do not store it?
        builder.put("sha256", sha256hex);

        inflightTuples.add(tuple);

        writer.repository.writeToIndex(builder);

        eventCounter.scope("Indexed").incrBy(1);

        _collector.emit(StatusStreamName, tuple,
                new Values(url, metadata, Status.FETCHED));

        if (numberOfEntries > 0 && inflightTuples.size() >= numberOfEntries) {
            flush();
        }
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        // configure how often a tick tuple will be sent to our bolt
        Config conf = new Config();
        conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, 1);
        return conf;
    }

}
