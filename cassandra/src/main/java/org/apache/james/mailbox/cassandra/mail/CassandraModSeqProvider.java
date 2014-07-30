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

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.incr;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

import java.util.UUID;

import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.store.mail.ModSeqProvider;
import org.apache.james.mailbox.store.mail.model.Mailbox;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;

public class CassandraModSeqProvider implements ModSeqProvider<UUID> {

    private Session session;

    public CassandraModSeqProvider(Session session) {
        this.session = session;
    }

    @Override
    public long nextModSeq(MailboxSession mailboxSession, Mailbox<UUID> mailbox) throws MailboxException {
        session.execute(update(MailboxCountersTable.TABLE_NAME).with(incr(MailboxCountersTable.NEXT_MOD_SEQ)).where(eq(MailboxCountersTable.MAILBOX_ID, mailbox.getMailboxId())));
        return highestModSeq(mailboxSession, mailbox);
    }

    @Override
    public long highestModSeq(MailboxSession mailboxSession, Mailbox<UUID> mailbox) throws MailboxException {
        ResultSet result = session.execute(select(MailboxCountersTable.NEXT_MOD_SEQ).from(MailboxCountersTable.TABLE_NAME).where(eq(MailboxCountersTable.MAILBOX_ID, mailbox.getMailboxId())));
        if (result.isExhausted()) {
            return 0;
        } else {
            return result.one().getLong(MailboxCountersTable.NEXT_MOD_SEQ);
        }
    }
}
