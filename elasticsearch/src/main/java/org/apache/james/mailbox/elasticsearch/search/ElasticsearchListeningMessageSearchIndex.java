package org.apache.james.mailbox.elasticsearch.search;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Iterator;

import javax.mail.Flags;

import org.apache.commons.lang.NotImplementedException;
import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.model.MessageRange;
import org.apache.james.mailbox.model.SearchQuery;
import org.apache.james.mailbox.store.mail.MessageMapperFactory;
import org.apache.james.mailbox.store.mail.model.Mailbox;
import org.apache.james.mailbox.store.mail.model.Message;
import org.apache.james.mailbox.store.search.ListeningMessageSearchIndex;
import org.elasticsearch.action.admin.indices.mapping.delete.DeleteMappingRequestBuilder;
import org.elasticsearch.action.deletebyquery.DeleteByQueryRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.Requests;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeQueryBuilder;

public class ElasticsearchListeningMessageSearchIndex<Id> extends ListeningMessageSearchIndex<Id> {
    
    private Client client;
    private final String INDEX_NAME = "messages";
    private final String TYPE_NAME = "message";


    public ElasticsearchListeningMessageSearchIndex(Client client) {
        super(null);
        this.client = client;
    }
    
    public ElasticsearchListeningMessageSearchIndex(MessageMapperFactory<Id> factory, Client client) {
        super(factory);
        this.client = client;
    }

    public Iterator<Long> search(MailboxSession session, Mailbox<Id> mailbox, SearchQuery searchQuery) throws MailboxException {
        throw new NotImplementedException();
    }

    @Override
    public void add(MailboxSession session, Mailbox<Id> mailbox, Message<Id> message) throws MailboxException {
        try {
            client.prepareIndex(INDEX_NAME, TYPE_NAME, message.getUid() + "")
                    .setSource(jsonBuilder()
                        .startObject()
                            .field("date", message.getInternalDate())
                            .field("id", message.getUid())
                        .endObject()                
                    )
                    .execute()
                    .actionGet();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void delete(MailboxSession session, Mailbox<Id> mailbox, MessageRange range) throws MailboxException {
        final MessageRange.Type type = range.getType();
        RangeQueryBuilder query = null;
        switch (type) {
            case ALL:
                client.admin().indices().prepareDeleteMapping(INDEX_NAME).setType(TYPE_NAME)
                    .execute()
                    .actionGet();
                break;
            case FROM:
                query = new RangeQueryBuilder("id").gte(range.getUidFrom());
                client.prepareDeleteByQuery(INDEX_NAME).setTypes(TYPE_NAME)
                    .setQuery(query)
                    .execute()
                    .actionGet();
                break;
            case RANGE:
                query = new RangeQueryBuilder("id").to(range.getUidTo()).from(range.getUidFrom());
                client.prepareDeleteByQuery(INDEX_NAME).setTypes(TYPE_NAME)
                    .setQuery(query)
                    .execute()
                    .actionGet();
                break;
            case ONE:
                client.prepareDelete(INDEX_NAME, TYPE_NAME, range.getUidTo() + "")
                    .execute()
                    .actionGet();
                break;
        }
    }

    @Override
    public void update(MailboxSession session, Mailbox<Id> mailbox, MessageRange range, Flags flags) throws MailboxException {
        throw new NotImplementedException();
    }

}
