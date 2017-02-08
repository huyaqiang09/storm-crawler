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

package com.digitalpebble.stormcrawler.elasticsearch.metrics;

import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.storm.metric.api.IMetricsConsumer;
import org.apache.storm.task.IErrorReporter;
import org.apache.storm.task.TopologyContext;
import org.elasticsearch.hadoop.rest.InitializationUtils;
import org.elasticsearch.hadoop.rest.RestService;
import org.elasticsearch.hadoop.rest.RestService.PartitionWriter;
import org.elasticsearch.storm.EsBolt;
import org.elasticsearch.storm.cfg.StormSettings;
import org.elasticsearch.storm.serialization.StormTupleBytesConverter;
import org.elasticsearch.storm.serialization.StormTupleFieldExtractor;
import org.elasticsearch.storm.serialization.StormValueWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.digitalpebble.stormcrawler.util.ConfUtils;

public class MetricsConsumer implements IMetricsConsumer {

    private transient static Log log = LogFactory.getLog(EsBolt.class);
    private static final Logger LOG = LoggerFactory.getLogger(EsBolt.class);

    private transient PartitionWriter writer;

    private static final String ESBoltType = "metrics";

    /** name of the index to use for the metrics (default : metrics) **/
    private static final String ESMetricsIndexNameParamName = "es." + ESBoltType
            + ".index.name";

    /**
     * name of the document type to use for the metrics (default : datapoint)
     **/
    private static final String ESmetricsDocTypeParamName = "es." + ESBoltType
            + ".doc.type";

    /**
     * List of whitelisted metrics. Only store metrics with names that are one
     * of these strings
     **/
    private static final String ESmetricsWhitelistParamName = "es." + ESBoltType
            + ".whitelist";

    /**
     * List of blacklisted metrics. Never store metrics with names that are one
     * of these strings
     **/
    private static final String ESmetricsBlacklistParamName = "es." + ESBoltType
            + ".blacklist";

    private String indexName;
    private String docType;

    private String[] whitelist = new String[0];
    private String[] blacklist = new String[0];

    @Override
    public void prepare(Map stormConf, Object registrationArgument,
            TopologyContext context, IErrorReporter errorReporter) {

        indexName = ConfUtils.getString(stormConf, ESMetricsIndexNameParamName,
                "metrics");
        docType = ConfUtils.getString(stormConf, ESmetricsDocTypeParamName,
                "datapoint");

        setWhitelist(ConfUtils.loadListFromConf(ESmetricsWhitelistParamName,
                stormConf));
        setBlacklist(ConfUtils.loadListFromConf(ESmetricsBlacklistParamName,
                stormConf));

        StormSettings settings = new StormSettings(stormConf);

        InitializationUtils.setValueWriterIfNotSet(settings,
                StormValueWriter.class, log);
        InitializationUtils.setBytesConverterIfNeeded(settings,
                StormTupleBytesConverter.class, log);
        InitializationUtils.setFieldExtractorIfNotSet(settings,
                StormTupleFieldExtractor.class, log);

        int totalTasks = context.getComponentTasks(context.getThisComponentId())
                .size();

        settings.setResourceWrite(indexName + "/" + docType);

        // TODO set the hosts

        writer = RestService.createWriter(settings, context.getThisTaskIndex(),
                totalTasks, log);
    }

    @Override
    public void cleanup() {
        if (writer != null) {
            try {
                writer.repository.flush();
            } finally {
                writer.close();
                writer = null;
            }
        }
    }

    @Override
    public void handleDataPoints(TaskInfo taskInfo,
            Collection<DataPoint> dataPoints) {

        final Iterator<DataPoint> datapointsIterator = dataPoints.iterator();
        while (datapointsIterator.hasNext()) {
            final DataPoint dataPoint = datapointsIterator.next();

            String name = dataPoint.name;

            Date now = new Date();

            if (dataPoint.value instanceof Map) {
                Iterator<Entry> keyValiter = ((Map) dataPoint.value).entrySet()
                        .iterator();
                while (keyValiter.hasNext()) {
                    Entry entry = keyValiter.next();
                    if (!(entry.getValue() instanceof Number)) {
                        LOG.error("Found data point value {} of class {}", name,
                                dataPoint.value.getClass().toString());
                        continue;
                    }
                    Double value = ((Number) entry.getValue()).doubleValue();
                    indexDataPoint(taskInfo, now, name + "." + entry.getKey(),
                            value);
                }
            } else if (dataPoint.value instanceof Number) {
                indexDataPoint(taskInfo, now, name,
                        ((Number) dataPoint.value).doubleValue());
            } else {
                LOG.error("Found data point value {} of class {}", name,
                        dataPoint.value.getClass().toString());
            }
        }
    }

    void setWhitelist(List<String> whitelist) {
        this.whitelist = whitelist.toArray(new String[whitelist.size()]);
    }

    void setBlacklist(List<String> blacklist) {
        this.blacklist = blacklist.toArray(new String[blacklist.size()]);
    }

    boolean shouldSkip(String name) {
        if (StringUtils.startsWithAny(name, blacklist)) {
            return true;
        }
        if (whitelist.length > 0) {
            return !StringUtils.startsWithAny(name, whitelist);
        }
        return false;
    }

    /**
     * Returns the name of the index that metrics will be written to.
     * 
     * @return elastic index name
     */
    protected String getIndexName() {
        return indexName;
    }

    private void indexDataPoint(TaskInfo taskInfo, Date timestamp, String name,
            double value) {
        if (shouldSkip(name)) {
            return;
        }

        // build a simple map and send it to ES
        Map<String, Object> keyVals = new HashMap<>();
        keyVals.put("srcComponentId", taskInfo.srcComponentId);
        keyVals.put("srcTaskId", taskInfo.srcTaskId);
        keyVals.put("srcWorkerHost", taskInfo.srcWorkerHost);
        keyVals.put("srcWorkerPort", taskInfo.srcWorkerPort);
        keyVals.put("name", name);
        keyVals.put("value", value);
        keyVals.put("timestamp", timestamp);

        // TODO flush?

        // TODO detect whether index name has changed and if so
        // flush, close and recreate the writer.
        // or use
        // https://www.elastic.co/guide/en/elasticsearch/hadoop/current/configuration.html#cfg-multi-writes-format
        writer.repository.writeToIndex(keyVals);

    }
}
