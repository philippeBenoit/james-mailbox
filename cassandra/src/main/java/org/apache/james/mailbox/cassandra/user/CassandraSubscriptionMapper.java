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
        final String user = subscription.getUser();
        final List<Subscription> subscriptions = subscriptionsByUser.get(user);
        if (subscriptions != null) {
            subscriptions.remove(subscription);
        }
    }

    @Override
    public Subscription findMailboxSubscriptionForUser(String user, String mailbox) {
        final List<Subscription> subscriptions = subscriptionsByUser.get(user);
        Subscription result = null;
        if (subscriptions != null) {
            for (Subscription subscription : subscriptions) {
                if (subscription.getMailbox().equals(mailbox)) {
                    result = subscription;
                    break;
                }
            }
        }
        return result;
    }

    @Override
    public List<Subscription> findSubscriptionsForUser(String user) {
        final List<Subscription> subcriptions = subscriptionsByUser.get(user);
        final List<Subscription> results;
        if (subcriptions == null) {
            results = Collections.EMPTY_LIST;
        } else {
            // Make a copy to prevent concurrent modifications
            results = new ArrayList<Subscription>(subcriptions);
        }
        return results;
    }

    @Override
    public synchronized void save(Subscription subscription) {
        final String user = subscription.getUser();
        final List<Subscription> subscriptions = subscriptionsByUser.get(user);
        if (subscriptions == null) {
            final List<Subscription> newSubscriptions = new ArrayList<Subscription>();
            newSubscriptions.add(subscription);
            subscriptionsByUser.put(user, newSubscriptions);
        } else {
            subscriptions.add(subscription);
        }
    }

    @Override
    public void endRequest() {
        // nothing todo
    }

}
