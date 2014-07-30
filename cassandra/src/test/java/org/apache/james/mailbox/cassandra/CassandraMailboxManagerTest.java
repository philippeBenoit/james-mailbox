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
package org.apache.james.mailbox.cassandra;

import java.util.UUID;

import org.apache.james.mailbox.AbstractMailboxManagerTest;
import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.acl.GroupMembershipResolver;
import org.apache.james.mailbox.acl.MailboxACLResolver;
import org.apache.james.mailbox.acl.SimpleGroupMembershipResolver;
import org.apache.james.mailbox.acl.UnionMailboxACLResolver;
import org.apache.james.mailbox.cassandra.mail.CassandraMailboxMapper;
import org.apache.james.mailbox.cassandra.mail.CassandraMessageMapper;
import org.apache.james.mailbox.cassandra.mail.CassandraModSeqProvider;
import org.apache.james.mailbox.cassandra.mail.CassandraUidProvider;
import org.apache.james.mailbox.cassandra.user.CassandraSubscriptionMapper;
import org.apache.james.mailbox.exception.BadCredentialsException;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.store.MockAuthenticator;
import org.apache.james.mailbox.store.StoreMailboxManager;
import org.cassandraunit.CassandraCQLUnit;
import org.cassandraunit.dataset.cql.ClassPathCQLDataSet;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.slf4j.LoggerFactory;

/**
 * InMemoryMailboxManagerTest that extends the MailboxManagerTest.
 */
public class CassandraMailboxManagerTest extends AbstractMailboxManagerTest {

    @Rule
    public CassandraCQLUnit cassandraCQLUnit = new CassandraCQLUnit(new ClassPathCQLDataSet("mailbox.cql", "mailbox"));

    @Before
    public void setup() throws Exception {
        createMailboxManager();
    }

    @After
    public void tearDown() throws BadCredentialsException, MailboxException {
        MailboxSession session = getMailboxManager().createSystemSession("test", LoggerFactory.getLogger("Test"));
        session.close();
    }

    @Override
    protected void createMailboxManager() throws MailboxException {
        CassandraMailboxMapper mailboxMapper = new CassandraMailboxMapper(cassandraCQLUnit.session);
        CassandraMessageMapper messageMapper = new CassandraMessageMapper(cassandraCQLUnit.session, new CassandraUidProvider(cassandraCQLUnit.session), new CassandraModSeqProvider(cassandraCQLUnit.session));
        CassandraSubscriptionMapper subscriptionMapper = new CassandraSubscriptionMapper();
        CassandraMailboxSessionMapperFactory factory = new CassandraMailboxSessionMapperFactory(mailboxMapper, messageMapper, subscriptionMapper);
        MailboxACLResolver aclResolver = new UnionMailboxACLResolver();
        GroupMembershipResolver groupMembershipResolver = new SimpleGroupMembershipResolver();

        StoreMailboxManager<UUID> mailboxManager = new StoreMailboxManager<UUID>(factory, new MockAuthenticator(), aclResolver, groupMembershipResolver);
        mailboxManager.init();

        setMailboxManager(mailboxManager);

    }

}
