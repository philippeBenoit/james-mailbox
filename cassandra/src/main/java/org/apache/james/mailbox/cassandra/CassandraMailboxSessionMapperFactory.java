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

import javax.inject.Inject;
import javax.inject.Singleton;

import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.cassandra.mail.CassandraMailboxMapper;
import org.apache.james.mailbox.cassandra.mail.CassandraMessageMapper;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.exception.SubscriptionException;
import org.apache.james.mailbox.store.MailboxSessionMapperFactory;
import org.apache.james.mailbox.store.mail.MailboxMapper;
import org.apache.james.mailbox.store.mail.MessageMapper;
import org.apache.james.mailbox.store.user.SubscriptionMapper;

import com.google.common.annotations.VisibleForTesting;

@Singleton
public class CassandraMailboxSessionMapperFactory extends MailboxSessionMapperFactory<UUID> {

    private MailboxMapper<UUID> mailboxMapper;
    private MessageMapper<UUID> messageMapper;
    private SubscriptionMapper subscriptionMapper;

    @Inject
    @VisibleForTesting
    CassandraMailboxSessionMapperFactory(CassandraMailboxMapper mailboxMapper, CassandraMessageMapper messageMapper, SubscriptionMapper subscriptionMapper) {
        this.mailboxMapper = mailboxMapper;
        this.messageMapper = messageMapper;
        this.subscriptionMapper = subscriptionMapper;
    }

    @Override
    public MailboxMapper<UUID> createMailboxMapper(MailboxSession session) throws MailboxException {
        return mailboxMapper;
    }

    @Override
    public MessageMapper<UUID> createMessageMapper(MailboxSession session) throws MailboxException {
        return messageMapper;
    }

    @Override
    public SubscriptionMapper createSubscriptionMapper(MailboxSession session) throws SubscriptionException {
        return subscriptionMapper;
    }

}
