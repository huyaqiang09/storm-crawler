/**
 * Licensed to DigitalPebble Ltd under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * DigitalPebble licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.digitalpebble.stormcrawler.solr;

import java.io.IOException;
import java.util.Map;

import org.apache.solr.client.solrj.SolrClient;
import org.apache.solr.client.solrj.SolrServerException;
import org.apache.solr.client.solrj.impl.CloudSolrClient;
import org.apache.solr.client.solrj.impl.HttpSolrClient;
import org.apache.solr.client.solrj.request.UpdateRequest;
import org.apache.storm.shade.org.apache.commons.lang.StringUtils;

import com.digitalpebble.stormcrawler.util.ConfUtils;

@SuppressWarnings("serial")
public class SolrConnection {

    private SolrClient client;
    private UpdateRequest request;

    private SolrConnection(SolrClient sc, UpdateRequest r) {
        client = sc;
        request = r;
    }

    public SolrClient getClient() {
        return client;
    }

    public UpdateRequest getRequest() {
        return request;
    }

    public static SolrClient getClient(Map stormConf, String boltType) {
        String zkHost = ConfUtils.getString(stormConf, "solr." + boltType
                + ".zkhost", null);
        String solrUrl = ConfUtils.getString(stormConf, "solr." + boltType
                + ".url", null);
        String collection = ConfUtils.getString(stormConf, "solr." + boltType
                + ".collection", null);

        SolrClient client;

        if (StringUtils.isNotBlank(zkHost)) {
            client = new CloudSolrClient.Builder().withZkHost(zkHost).build();
            if (StringUtils.isNotBlank(collection)) {
                ((CloudSolrClient) client).setDefaultCollection(collection);
            }
        } else if (StringUtils.isNotBlank(solrUrl)) {
            client = new HttpSolrClient.Builder(solrUrl).build();
        } else {
            throw new RuntimeException(
                    "SolrClient should have zk or solr URL set up");
        }

        return client;
    }

    public static UpdateRequest getRequest(Map stormConf, String boltType) {
        int commitWithin = ConfUtils.getInt(stormConf, "solr." + boltType
                + ".commit.within", -1);

        UpdateRequest request = new UpdateRequest();

        if (commitWithin != -1) {
            request.setCommitWithin(commitWithin);
        }

        return request;
    }

    public static SolrConnection getConnection(Map stormConf, String boltType) {
        SolrClient client = getClient(stormConf, boltType);
        UpdateRequest request = getRequest(stormConf, boltType);

        return new SolrConnection(client, request);
    }

    public void close() throws IOException, SolrServerException {
        if (client != null) {
            client.commit();
            client.close();
        }
    }
}
