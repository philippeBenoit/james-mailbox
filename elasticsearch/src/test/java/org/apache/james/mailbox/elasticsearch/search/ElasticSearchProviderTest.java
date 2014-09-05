package org.apache.james.mailbox.elasticsearch.search;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.deleteAll;
import static org.junit.Assert.assertEquals;

import org.apache.commons.lang.NotImplementedException;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.model.MessageRange;
import org.apache.james.mailbox.store.mail.model.impl.SimpleMessage;
import org.elasticsearch.ElasticsearchException;
import org.junit.Before;
import org.junit.Test;

import com.github.tlrx.elasticsearch.test.EsSetup;

public class ElasticSearchProviderTest {
    
    private EsSetup esSetup;
    private final String INDEX_NAME = "messages";
    private ElasticsearchListeningMessageSearchIndex searchIndex;

    @Before
    public void setUp() throws Exception {
        esSetup = new EsSetup();
        esSetup.execute(
            deleteAll(),
            createIndex(INDEX_NAME)
        );
        searchIndex = new ElasticsearchListeningMessageSearchIndex(esSetup.client());
    }

    private void waitESUpdate() {
        try {
            Thread.sleep(2000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    @Test
    public void testAdd() throws ElasticsearchException, MailboxException {
        assertEquals(Long.valueOf(0), esSetup.countAll());
        searchIndex.add(null, null, new SimpleMessageMock(0));
        waitESUpdate();
        assertEquals(Long.valueOf(1), esSetup.countAll());
    }

    private void addTwoMessage() throws MailboxException {
            searchIndex.add(null, null, new SimpleMessageMock(0));
            searchIndex.add(null, null, new SimpleMessageMock(1));
            waitESUpdate();
            assertEquals(Long.valueOf(2), esSetup.countAll());

    }

    @Test
    public void testDeleteAll() throws MailboxException {
        addTwoMessage();
        searchIndex.delete(null, null, MessageRange.all());
        waitESUpdate();
        assertEquals(Long.valueOf(0), esSetup.countAll());
    }

    @Test
    public void testDeleteOne() throws MailboxException {
        addTwoMessage();
        searchIndex.delete(null, null, MessageRange.one(1));
        waitESUpdate();
        assertEquals(Long.valueOf(1), esSetup.countAll());
    }

    private void addTenMessage() {
        try {
            for (int i = 0; i < 10; i++) {
                searchIndex.add(null, null, new SimpleMessageMock(i));
            }
            waitESUpdate();
            assertEquals(Long.valueOf(10), esSetup.countAll());
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
    
    @Test
    public void testDeleteFrom() throws MailboxException {
        addTenMessage();
        searchIndex.delete(null, null, MessageRange.from(6));
        assertEquals(Long.valueOf(6), esSetup.countAll());
    }
    
    @Test
    public void testDeleteRange() throws MailboxException {
        addTenMessage();
        searchIndex.delete(null, null, MessageRange.range(4, 8));
        assertEquals(Long.valueOf(5), esSetup.countAll());
    }
    
    @Test
    public void testUpate() throws MailboxException {
        SimpleMessage oldMessage = new SimpleMessageMock(1);
        searchIndex.add(null, null, oldMessage);
        throw new NotImplementedException();
    }
}
