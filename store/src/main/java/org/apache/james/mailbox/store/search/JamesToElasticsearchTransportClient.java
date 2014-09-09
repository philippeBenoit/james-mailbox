package org.apache.james.mailbox.store.search;

import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.transport.TransportAddress;

public class JamesToElasticsearchTransportClient extends TransportClient {

    private static Settings settings = ImmutableSettings.settingsBuilder()
            .put("cluster.name", "james").build();

    public JamesToElasticsearchTransportClient(String hostname, int  port) throws ElasticsearchException {
        super(settings);
        addTransportAddress(new InetSocketTransportAddress(hostname, port));
    }
    
   public JamesToElasticsearchTransportClient(TransportAddress[] transportAddress) {
       super(settings);
       addTransportAddresses(transportAddress);
   }

}
