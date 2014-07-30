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

package org.apache.james.mailbox.cassandra.user;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.apache.james.mailbox.cassandra.table.CassandraSubscriptionTable.FIELDS;
import static org.apache.james.mailbox.cassandra.table.CassandraSubscriptionTable.MAILBOX;
import static org.apache.james.mailbox.cassandra.table.CassandraSubscriptionTable.TABLE_NAME;
import static org.apache.james.mailbox.cassandra.table.CassandraSubscriptionTable.USER;

import java.util.List;

import org.apache.james.mailbox.store.transaction.NonTransactionalMapper;
import org.apache.james.mailbox.store.user.SubscriptionMapper;
import org.apache.james.mailbox.store.user.model.Subscription;
import org.apache.james.mailbox.store.user.model.impl.SimpleSubscription;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;

public class CassandraSubscriptionMapper extends NonTransactionalMapper implements SubscriptionMapper {
    private Session session;

    public CassandraSubscriptionMapper(Session session) {
        this.session = session;
    }

    @Override
    public synchronized void delete(Subscription subscription) {
        session.execute(QueryBuilder.delete().from(TABLE_NAME).where(eq(USER, subscription.getUser())).and(eq(MAILBOX, subscription.getMailbox())));
    }

    @Override
    public Subscription findMailboxSubscriptionForUser(String user, String mailbox) {
        ResultSet results = session.execute(select(MAILBOX).from(TABLE_NAME).where(eq(USER, user)).and(eq(MAILBOX, mailbox)));
        return !results.isExhausted() ? new SimpleSubscription(user, mailbox) : null;
    }

    @Override
    public List<Subscription> findSubscriptionsForUser(String user) {
        Builder<Subscription> result = ImmutableList.<Subscription> builder();
        Select query = select(MAILBOX).from(TABLE_NAME);
        query.where(eq(USER, user));
        query.allowFiltering();
        for (Row row : session.execute(query)) {
            result.add(new SimpleSubscription(user, row.getString(MAILBOX)));
        }
        return result.build();
    }

    @Override
    public synchronized void save(Subscription subscription) {
        Insert query = insertInto(TABLE_NAME).value(USER, subscription.getUser()).value(MAILBOX, subscription.getMailbox());
        session.execute(query);
    }

    public List<SimpleSubscription> list() {
        Builder<SimpleSubscription> result = ImmutableList.<SimpleSubscription> builder();
        for (Row row : session.execute(select(FIELDS).from(TABLE_NAME))) {
            result.add(new SimpleSubscription(row.getString(USER), row.getString(MAILBOX)));
        }
        return result.build();
    }

    @Override
    public void endRequest() {
        // nothing todo
    }

}
