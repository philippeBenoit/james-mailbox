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
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import org.apache.james.mailbox.cassandra.CassandraClusterSingleton;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.exception.MailboxNotFoundException;
import org.apache.james.mailbox.model.MailboxPath;
import org.apache.james.mailbox.store.mail.model.Mailbox;
import org.apache.james.mailbox.store.mail.model.impl.SimpleMailbox;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;

/**
 * CassandraMailboxMapper unit tests.
 * 
 */
public class CassandraMailboxMapperTest {

    private static final Logger LOG = LoggerFactory.getLogger(CassandraMailboxMapperTest.class);
    public static final CassandraClusterSingleton CLUSTER = CassandraClusterSingleton.build();
    private static CassandraMailboxMapper mapper;
    private static List<SimpleMailbox<UUID>> mailboxList;
    private static List<MailboxPath> pathsList;
    private static final int NAMESPACES = 5;
    private static final int USERS = 5;
    private static final int MAILBOX_NO = 5;
    private static final char SEPARATOR = '%';
    private Session session;

    @Before
    public void setUp() throws Exception {
        CLUSTER.ensureAllTables();
        CLUSTER.clearAllTables();
        session = CLUSTER.getConf();
        fillMailboxList();
        mapper = new CassandraMailboxMapper(session);
        for (SimpleMailbox<UUID> mailbox : mailboxList) {
            mapper.save(mailbox);
        }
    }

    /**
     * Test an ordered scenario with list, delete... methods.
     * 
     * @throws Exception
     */
    @Test
    public void testMailboxMapperScenario() throws Exception {
        testFindMailboxByPath();
        testFindMailboxWithPathLike();
        testList();
        testSave();
        testDelete();
        testHasChildren();
        // testDeleteAllMemberships(); // Ignore this test
    }

    /**
     * Test of findMailboxByPath method, of class CassandraMailboxMapper.
     */
    private void testFindMailboxByPath() throws Exception {
        LOG.info("findMailboxByPath");
        SimpleMailbox<UUID> mailbox;
        for (MailboxPath path : pathsList) {
            LOG.info("Searching for " + path);
            mailbox = (SimpleMailbox<UUID>) mapper.findMailboxByPath(path);
            assertEquals(path, new MailboxPath(mailbox.getNamespace(), mailbox.getUser(), mailbox.getName()));
        }
    }

    /**
     * Test of findMailboxWithPathLike method, of class CassandraMailboxMapper.
     */
    private void testFindMailboxWithPathLike() throws Exception {
        LOG.info("findMailboxWithPathLike");
        MailboxPath path = pathsList.get(pathsList.size() / 2);

        List<Mailbox<UUID>> result = mapper.findMailboxWithPathLike(path);
        assertEquals(1, result.size());

        int start = 3;
        int end = 7;
        MailboxPath newPath;

        for (int i = start; i < end; i++) {
            newPath = new MailboxPath(path);
            newPath.setName(i + newPath.getName() + " " + i);
            // test for paths with null user
            if (i % 2 == 0) {
                newPath.setUser(null);
            }
            addMailbox(new SimpleMailbox<UUID>(newPath, 1234));
        }
        result = mapper.findMailboxWithPathLike(path);
        assertEquals(end - start + 1, result.size());
    }

    /**
     * Test of list method, of class CassandraMailboxMapper.
     */
    private void testList() throws Exception {
        LOG.info("list");
        List<Mailbox<UUID>> result = mapper.list();
        assertEquals(mailboxList.size(), result.size());

    }

    /**
     * Test of save method and list method, of class CassandraMailboxMapper.
     */
    private void testSave() throws Exception {
        LOG.info("save and mailboxFromResult");
        final List<Mailbox<UUID>> mailboxes = mapper.list();
        final SimpleMailbox<UUID> mlbx = mailboxList.get(mailboxList.size() / 2);
        Mailbox<UUID> newValue = mailboxes.get(0);

        for (Mailbox<UUID> mailbox : mailboxes) {
            if (mlbx.getMailboxId().equals(mailbox.getMailboxId())) {
                newValue = mailbox;
            }
        }

        assertEquals(mlbx, newValue);
        assertEquals(mlbx.getUser(), newValue.getUser());
        assertEquals(mlbx.getName(), newValue.getName());
        assertEquals(mlbx.getNamespace(), newValue.getNamespace());
        assertEquals(mlbx.getMailboxId(), newValue.getMailboxId());
    }

    /**
     * Test of delete method, of class CassandraMailboxMapper.
     */
    private void testDelete() throws Exception {
        LOG.info("delete");
        // delete last 5 mailboxes from mailboxList
        int offset = 5;
        int notFoundCount = 0;

        Iterator<SimpleMailbox<UUID>> iterator = mailboxList.subList(mailboxList.size() - offset, mailboxList.size()).iterator();

        while (iterator.hasNext()) {
            SimpleMailbox<UUID> mailbox = iterator.next();
            mapper.delete(mailbox);
            iterator.remove();
            MailboxPath path = new MailboxPath(mailbox.getNamespace(), mailbox.getUser(), mailbox.getName());
            pathsList.remove(path);
            LOG.info("Removing mailbox: {}", path);
            try {
                mapper.findMailboxByPath(path);
            } catch (MailboxNotFoundException e) {
                LOG.info("Succesfully removed {}", mailbox);
                notFoundCount++;
            }
        }
        assertEquals(offset, notFoundCount);
        assertEquals(mailboxList.size(), mapper.list().size());
    }

    /**
     * Test of hasChildren method, of class CassandraMailboxMapper.
     */
    private void testHasChildren() throws Exception {
        LOG.info("hasChildren");
        String oldName;
        for (MailboxPath path : pathsList) {
            final SimpleMailbox<UUID> mailbox = new SimpleMailbox<UUID>(path, 12455);
            oldName = mailbox.getName();
            if (path.getUser().equals("user3")) {
                mailbox.setName("test");
            }
            boolean result = mapper.hasChildren(mailbox, SEPARATOR);
            mailbox.setName(oldName);
            if (path.getUser().equals("user3")) {
                assertTrue(result);
            } else {
                assertFalse(result);
            }

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

    private void addMailbox(SimpleMailbox<UUID> mailbox) throws MailboxException {
        mailboxList.add(mailbox);
        pathsList.add(new MailboxPath(mailbox.getNamespace(), mailbox.getUser(), mailbox.getName()));
        mapper.save(mailbox);
        LOG.info("Added new mailbox: {} paths: {}", mailboxList.size(), pathsList.size());
    }
}
