/****************************************************************
 * Licensed to the Apache Software Foundation (ASF) under one   *
 * or more contributor license agreements.  See the NOTICE file *
 * distributed with this work for additional information        *
 * regarding copyright ownership.  The ASF licenses this file   *
 * to you under the Apache License, Version 2.0 (the            *
 * "License"); you may not use this file except in compliance   *
 * with the License.  You may obtain a copy of the License at   *
 *                                                              *
 *   http://www.apache.org/licenses/LICENSE-2.0                 *
 *                                                              *
 * Unless required by applicable law or agreed to in writing,   *
 * software distributed under the License is distributed on an  *
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY       *
 * KIND, either express or implied.  See the License for the    *
 * specific language governing permissions and limitations      *
 * under the License.                                           *
 ****************************************************************/
package org.apache.james.mailbox.cassandra.mail;

import static org.junit.Assert.assertEquals;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.UUID;

import javax.mail.Flags;
import javax.mail.internet.SharedInputStream;
import javax.mail.util.SharedByteArrayInputStream;

import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.cassandra.CassandraClusterSingleton;
import org.apache.james.mailbox.mock.MockMailboxSession;
import org.apache.james.mailbox.model.MailboxPath;
import org.apache.james.mailbox.store.mail.model.Mailbox;
import org.apache.james.mailbox.store.mail.model.Message;
import org.apache.james.mailbox.store.mail.model.impl.PropertyBuilder;
import org.apache.james.mailbox.store.mail.model.impl.SimpleMailbox;
import org.apache.james.mailbox.store.mail.model.impl.SimpleMessage;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;

/**
 * Unit tests for CassandraMessageMapper.
 * 
 */
public class CassandraMessageMapperTest {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraMailboxMapperTest.class);
    public static final CassandraClusterSingleton CLUSTER = CassandraClusterSingleton.build();
    private static CassandraUidProvider uidProvider;
    private static CassandraModSeqProvider modSeqProvider;
    private static CassandraMessageMapper messageMapper;
    private static final List<MailboxPath> MBOX_PATHS = new ArrayList<MailboxPath>();
    private static final List<Mailbox<UUID>> MBOXES = new ArrayList<Mailbox<UUID>>();
    private static final List<Message<UUID>> MESSAGE_NO = new ArrayList<Message<UUID>>();
    private static final int COUNT = 5;
    private static Session session;
    /*
     * we mock a simple message content
     */
    private static final String messageTemplate = "Date: Mon, 7 Feb 1994 21:52:25 -0800 (PST)\n" + "From: Fred Foobar <foobar@Blurdybloop.COM>\n" + "Subject: Test 02\n" + "To: mooch@owatagu.siam.edu\n" + "Message-Id: <B27397-0100000@Blurdybloop.COM>\n" + "MIME-Version: 1.0\n"
            + "Content-Type: TEXT/PLAIN; CHARSET=US-ASCII\n" + "\n" + "Test\n" + "\n.";

    private static SharedInputStream content = new SharedByteArrayInputStream(messageTemplate.getBytes());

    @Before
    public void setUp() throws Exception {
        CLUSTER.ensureAllTables();
        CLUSTER.clearAllTables();
        session = CLUSTER.getConf();
        uidProvider = new CassandraUidProvider(session);
        modSeqProvider = new CassandraModSeqProvider(session);
        generateTestData();
        final MailboxSession mailboxSession = new MockMailboxSession("ieugen");
        messageMapper = new CassandraMessageMapper(session, uidProvider, modSeqProvider, mailboxSession);
        for (int i = 0; i < MESSAGE_NO.size(); i++) {
            messageMapper.add(MBOXES.get(1), MESSAGE_NO.get(i));
        }
    }

    public static void generateTestData() {
        final Random random = new Random();
        MailboxPath mboxPath;
        final PropertyBuilder propBuilder = new PropertyBuilder();
        SimpleMailbox<UUID> mailbox;

        for (int i = 0; i < COUNT; i++) {
            if (i % 2 == 0) {
                mboxPath = new MailboxPath("gsoc", "ieugen" + i, "INBOX");
            } else {
                mboxPath = new MailboxPath("gsoc", "ieugen" + i, "INBOX.box" + i);
            }
            MBOX_PATHS.add(mboxPath);
            mailbox = new SimpleMailbox<UUID>(MBOX_PATHS.get(i), random.nextLong());
            mailbox.setMailboxId(UUID.randomUUID());
            MBOXES.add(mailbox);
            propBuilder.setProperty("gsoc", "prop" + i, "value");
        }
        propBuilder.setMediaType("text");
        propBuilder.setSubType("html");
        propBuilder.setTextualLineCount(2L);

        SimpleMessage<UUID> myMsg;
        final Flags flags = new Flags(Flags.Flag.RECENT);
        final Date today = new Date();

        for (int i = 0; i < COUNT * 2; i++) {
            myMsg = new SimpleMessage<UUID>(today, messageTemplate.getBytes().length, messageTemplate.getBytes().length - 20, content, flags, propBuilder, MBOXES.get(1).getMailboxId());
            if (i == COUNT * 2 - 1) {
                flags.add(Flags.Flag.SEEN);
                flags.remove(Flags.Flag.RECENT);
                myMsg.setFlags(flags);
            }
            MESSAGE_NO.add(myMsg);
        }
    }

    /**
     * Test an ordered scenario with count, find, add... methods.
     * 
     * @throws Exception
     */
    @Test
    public void testMessageMapperScenario() throws Exception {
        testCountMessagesInMailbox();
        testCountUnseenMessagesInMailbox();
        testFindFirstUnseenMessageUid();
        testFindRecentMessageUidsInMailbox();
        testAdd();
        testGetLastUid();
        testGetHighestModSeq();
    }

    /**
     * Test of countMessagesInMailbox method, of class CassandraMessageMapper.
     */
    private void testCountMessagesInMailbox() throws Exception {
        LOG.info("countMessagesInMailbox");
        long messageCount = messageMapper.countMessagesInMailbox(MBOXES.get(1));
        assertEquals(MESSAGE_NO.size(), messageCount);
    }

    /**
     * Test of countUnseenMessagesInMailbox method, of class
     * CassandraMessageMapper.
     */
    private void testCountUnseenMessagesInMailbox() throws Exception {
        LOG.info("countUnseenMessagesInMailbox");
        long unseen = messageMapper.countUnseenMessagesInMailbox(MBOXES.get(1));
        assertEquals(MESSAGE_NO.size() - 1, unseen);
    }

    /**
     * Test of findFirstUnseenMessageUid method, of class
     * CassandraMessageMapper.
     */
    private void testFindFirstUnseenMessageUid() throws Exception {
        LOG.info("findFirstUnseenMessageUid");
        final long uid = messageMapper.findFirstUnseenMessageUid(MBOXES.get(1));
        assertEquals(1, uid);
    }

    /**
     * Test of findRecentMessageUidsInMailbox method, of class
     * CassandraMessageMapper.
     */
    private void testFindRecentMessageUidsInMailbox() throws Exception {
        LOG.info("findRecentMessageUidsInMailbox");
        List<Long> recentMessages = messageMapper.findRecentMessageUidsInMailbox(MBOXES.get(1));
        assertEquals(MESSAGE_NO.size() - 1, recentMessages.size());
    }

    /**
     * Test of add method, of class CassandraMessageMapper.
     */
    private void testAdd() throws Exception {
        LOG.info("add");
        // The tables should be deleted every time the tests run.
        long msgCount = messageMapper.countMessagesInMailbox(MBOXES.get(1));
        LOG.info(msgCount + " " + MESSAGE_NO.size());
        assertEquals(MESSAGE_NO.size(), msgCount);
    }

    /**
     * Test of getLastUid method, of class CassandraMessageMapper.
     */
    private void testGetLastUid() throws Exception {
        LOG.info("getLastUid");
        long lastUid = messageMapper.getLastUid(MBOXES.get(1));
        assertEquals(MESSAGE_NO.size(), lastUid);
    }

    /**
     * Test of getHighestModSeq method, of class CassandraMessageMapper.
     */
    private void testGetHighestModSeq() throws Exception {
        LOG.info("getHighestModSeq");
        long highestModSeq = messageMapper.getHighestModSeq(MBOXES.get(1));
        assertEquals(MESSAGE_NO.size(), highestModSeq);
    }
}
