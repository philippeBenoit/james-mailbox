package org.apache.james.mailbox.store.search;

import static com.github.tlrx.elasticsearch.test.EsSetup.createIndex;
import static com.github.tlrx.elasticsearch.test.EsSetup.deleteAll;
import static org.junit.Assert.assertEquals;

import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.mock.MockMailboxSession;
import org.apache.james.mailbox.model.MessageRange;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.junit.Before;
import org.junit.Test;

import com.github.tlrx.elasticsearch.test.EsSetup;

public class ElasticSearchProviderTest {
    
    private EsSetup esSetup;
    private final String INDEX_NAME = "messages";
    private ElasticsearchMessageSearchIndex searchIndex;
    private MockMailboxSession mailboxSession = new MockMailboxSession("test");

    @Before
    public void setUp() throws Exception {
        esSetup = new EsSetup();
        esSetup.execute(
            deleteAll(),
            createIndex(INDEX_NAME)
        );
        searchIndex = new ElasticsearchMessageSearchIndex(esSetup.client());
    }

    private void forceESUpdate() throws InterruptedException {
        esSetup.client().admin().indices().refresh(new RefreshRequest(INDEX_NAME));
        Thread.sleep(200);
    }

    @Test
    public void testAdd() throws ElasticsearchException, MailboxException, InterruptedException {
        assertEquals(Long.valueOf(0), esSetup.countAll());
        searchIndex.add(mailboxSession, null, new SimpleMessageMock(0));
        forceESUpdate();
        assertEquals(Long.valueOf(1), esSetup.countAll());
    }

    private void addTwoMessage() throws MailboxException, InterruptedException {
        searchIndex.add(mailboxSession, null, new SimpleMessageMock(0));
        searchIndex.add(mailboxSession, null, new SimpleMessageMock(1));
        forceESUpdate();
        assertEquals(Long.valueOf(2), esSetup.countAll());
    }

    @Test
    public void testDeleteAll() throws MailboxException, InterruptedException {
        addTwoMessage();
        searchIndex.delete(mailboxSession, null, MessageRange.all());
        forceESUpdate();
        assertEquals(Long.valueOf(0), esSetup.countAll());
    }

    @Test
    public void testDeleteOne() throws MailboxException, InterruptedException {
        addTwoMessage();
        searchIndex.delete(mailboxSession, null, MessageRange.one(1));
        forceESUpdate();
        assertEquals(Long.valueOf(1), esSetup.countAll());
    }

    private void addTenMessage() throws InterruptedException, MailboxException {
        for (int i = 0; i < 10; i++) {
            searchIndex.add(mailboxSession, null, new SimpleMessageMock(i));
        }
        forceESUpdate();
        assertEquals(Long.valueOf(10), esSetup.countAll());
    }
    
    @Test
    public void testDeleteFrom() throws MailboxException, InterruptedException {
        addTenMessage();
        searchIndex.delete(mailboxSession, null, MessageRange.from(6));
        assertEquals(Long.valueOf(6), esSetup.countAll());
    }
    
    @Test
    public void testDeleteRange() throws MailboxException, InterruptedException {
        addTenMessage();
        searchIndex.delete(mailboxSession, null, MessageRange.range(4, 8));
        assertEquals(Long.valueOf(5), esSetup.countAll());
    }
    
    @Test
    public void testUpate() throws MailboxException {
     /*   SimpleMessage oldMessage = new SimpleMessageMock(1);
        searchIndex.add(null, null, oldMessage);
        throw new NotImplementedException(); */
    }
}
