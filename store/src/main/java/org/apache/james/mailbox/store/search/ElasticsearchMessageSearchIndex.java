package org.apache.james.mailbox.store.search;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Set;

import javax.mail.Flags;
import javax.mail.Flags.Flag;

import org.apache.commons.lang.NotImplementedException;
import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.model.MessageRange;
import org.apache.james.mailbox.model.SearchQuery;
import org.apache.james.mailbox.store.mail.MessageMapperFactory;
import org.apache.james.mailbox.store.mail.model.Mailbox;
import org.apache.james.mailbox.store.mail.model.Message;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;

public class ElasticsearchMessageSearchIndex<Id> extends ListeningMessageSearchIndex<Id> {
    
    private Client client;
    private final String INDEX_NAME = "messages";
    private final String TYPE_NAME = "message";

    public static interface Field {
        /**
         * Contain the unique id of the message in {@link Index}
         */
        public final static String ID = "id";

        /**
         * Contain uid of the {@link Message}
         */
        public final static String UID = "uid";

        /**
         * Contain the {@link Flags} of the {@link Message}
         */
        public final static String FLAGS = "flags";

        /**
         * Contain the size of the {@link Message}
         */
        public final static String SIZE = "size";

        /**
         * Contain the body of the {@link Message}
         */
        public final static String BODY = "body";

        /**
         * Contain the FROM-Address of the {@link Message}
         */
        public final static String FROM = "from";

        /**
         * Contain the internalDate of the message with YEAR-Resolution
         */
        public final static String INTERNAL_DATE = "internalDate";

        /**
         * Contain the id of the {@link Mailbox}
         */
        public final static String MAILBOX_ID = "mailboxIsd";
    }

    public ElasticsearchMessageSearchIndex(Client client) {
        super(null);
        this.client = client;
    }
    
    public ElasticsearchMessageSearchIndex(MessageMapperFactory<Id> factory, Client client) {
        super(factory);
        this.client = client;
    }

    public Iterator<Long> search(MailboxSession session, Mailbox<Id> mailbox, SearchQuery searchQuery) throws MailboxException {
        Set<Long> uids = new LinkedHashSet<Long>();
        // return uids.iterator();
        throw new NotImplementedException();
    }

    @Override
    public void add(MailboxSession session, Mailbox<Id> mailbox, Message<Id> message) throws MailboxException {
        try {
            client.prepareIndex(INDEX_NAME, TYPE_NAME, message.getUid() + "")
                    .setSource(jsonBuilder()
                        .startObject()
                            .field(Field.INTERNAL_DATE, message.getInternalDate())
                            .field(Field.ID, message.getUid())
                            .field(Field.SIZE, message.getFullContentOctets())
                            .field(Field.FLAGS, message.createFlags().getSystemFlags().toString())
               //             .field("MAILBOX_ID", mailbox.getMailboxId())
                        .endObject()                
                    )
                    .execute()
                    .actionGet();
        } catch (IOException e) {
            throw new MailboxException();
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
                query = new RangeQueryBuilder(Field.ID).gte(range.getUidFrom());
                client.prepareDeleteByQuery(INDEX_NAME).setTypes(TYPE_NAME)
                    .setQuery(query)
                    .execute()
                    .actionGet();
                break;
            case RANGE:
                query = new RangeQueryBuilder(Field.ID).to(range.getUidTo()).from(range.getUidFrom());
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
        QueryBuilder query;
        throw new NotImplementedException();
    }

}
