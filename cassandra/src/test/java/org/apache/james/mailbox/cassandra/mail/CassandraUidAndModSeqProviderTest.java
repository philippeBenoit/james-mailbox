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
import java.util.List;
import java.util.UUID;

import org.apache.james.mailbox.cassandra.CassandraClusterSingleton;
import org.apache.james.mailbox.model.MailboxPath;
import org.apache.james.mailbox.store.mail.model.impl.SimpleMailbox;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;

/**
 * Unit tests for UidProvider and ModSeqProvider.
 * 
 */
public class CassandraUidAndModSeqProviderTest {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraUidAndModSeqProviderTest.class);
    private static final CassandraClusterSingleton CLUSTER = CassandraClusterSingleton.build();
    private static Session session;
    private static CassandraUidProvider uidProvider;
    private static CassandraModSeqProvider modSeqProvider;
    private static CassandraMailboxMapper mapper;
    private static List<SimpleMailbox<UUID>> mailboxList;
    private static List<MailboxPath> pathsList;
    private static final int NAMESPACES = 5;
    private static final int USERS = 5;
    private static final int MAILBOX_NO = 5;
    private static final char SEPARATOR = '%';

    @Before
    public void setUpClass() throws Exception {
        CLUSTER.ensureAllTables();
        CLUSTER.ensureAllTables();
        session = CLUSTER.getConf();
        uidProvider = new CassandraUidProvider(session);
        modSeqProvider = new CassandraModSeqProvider(session);
        mapper = new CassandraMailboxMapper(session);
        fillMailboxList();
        for (SimpleMailbox<UUID> mailbox : mailboxList) {
            mapper.save(mailbox);
        }
    }

    private static void fillMailboxList() {
        mailboxList = new ArrayList<SimpleMailbox<UUID>>();
        pathsList = new ArrayList<MailboxPath>();
        MailboxPath path;
        String name;
        for (int i = 0; i < NAMESPACES; i++) {
            for (int j = 0; j < USERS; j++) {
                for (int k = 0; k < MAILBOX_NO; k++) {
                    if (j == 3) {
                        name = "test" + SEPARATOR + "subbox" + k;
                    } else {
                        name = "mailbox" + k;
                    }
                    path = new MailboxPath("namespace" + i, "user" + j, name);
                    pathsList.add(path);
                    mailboxList.add(new SimpleMailbox<UUID>(path, 13));
                }
            }
        }

        LOG.info("Created test case with {} mailboxes and {} paths", mailboxList.size(), pathsList.size());
    }

    /**
     * Test of lastUid method, of class CassandraUidProvider.
     */
    @Test
    public void testLastUid() throws Exception {
        LOG.info("lastUid");
        final MailboxPath path = new MailboxPath("gsoc", "ieugen", "Trash");
        final SimpleMailbox<UUID> newBox = new SimpleMailbox<UUID>(path, 1234);
        mapper.save(newBox);
        mailboxList.add(newBox);
        pathsList.add(path);

        final long result = uidProvider.lastUid(null, newBox);
        assertEquals(0, result);
        for (int i = 1; i < 10; i++) {
            final long uid = uidProvider.nextUid(null, newBox);
            assertEquals(uid, uidProvider.lastUid(null, newBox));
        }
    }

    /**
     * Test of nextUid method, of class CassandraUidProvider.
     */
    @Test
    public void testNextUid() throws Exception {
        LOG.info("nextUid");
        final SimpleMailbox<UUID> mailbox = mailboxList.get(mailboxList.size() / 2);
        final long lastUid = uidProvider.lastUid(null, mailbox);
        long result;
        for (int i = (int) lastUid + 1; i < (lastUid + 10); i++) {
            result = uidProvider.nextUid(null, mailbox);
            assertEquals(i, result);
        }
    }

    /**
     * Test of highestModSeq method, of class CassandraModSeqProvider.
     */
    @Test
    public void testHighestModSeq() throws Exception {
        LOG.info("highestModSeq");
        LOG.info("lastUid");
        MailboxPath path = new MailboxPath("gsoc", "ieugen", "Trash");
        SimpleMailbox<UUID> newBox = new SimpleMailbox<UUID>(path, 1234);
        mapper.save(newBox);
        mailboxList.add(newBox);
        pathsList.add(path);

        long result = modSeqProvider.highestModSeq(null, newBox);
        assertEquals(0, result);
        for (int i = 1; i < 10; i++) {
            long uid = modSeqProvider.nextModSeq(null, newBox);
            assertEquals(uid, modSeqProvider.highestModSeq(null, newBox));
        }
    }

    /**
     * Test of nextModSeq method, of class CassandraModSeqProvider.
     */
    @Test
    public void testNextModSeq() throws Exception {
        LOG.info("nextModSeq");
        final SimpleMailbox<UUID> mailbox = mailboxList.get(mailboxList.size() / 2);
        final long lastUid = modSeqProvider.highestModSeq(null, mailbox);
        long result;
        for (int i = (int) lastUid + 1; i < (lastUid + 10); i++) {
            result = modSeqProvider.nextModSeq(null, mailbox);
            assertEquals(i, result);
        }
    }
}
