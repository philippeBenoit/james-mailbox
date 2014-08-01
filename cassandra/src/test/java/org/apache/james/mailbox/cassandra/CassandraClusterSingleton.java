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

import org.apache.commons.lang.NotImplementedException;
import org.cassandraunit.utils.EmbeddedCassandraServerHelper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.Session;

/**
 * Class that will creates a single instance of Cassandra session.
 */
public final class CassandraClusterSingleton {
    private final static String CLUSTER_IP = "localhost";
    private final static int CLUSTER_PORT_TEST = 9142;
    private final static String KEYSPACE_NAME = "apache_james";
    private final static int DEFAULT_REPLICATION_FACTOR = 1;

    private static final Logger LOG = LoggerFactory.getLogger(CassandraClusterSingleton.class);
    private static CassandraClusterSingleton cluster = null;
    private CassandraSession session;

    /**
     * Builds a MiniCluster instance.
     * 
     * @return the {@link CassandraClusterSingleton} instance
     * @throws RuntimeException
     */
    public static synchronized CassandraClusterSingleton build() throws RuntimeException {
        LOG.info("Retrieving cluster instance.");
        if (cluster == null) {
            cluster = new CassandraClusterSingleton();
        }
        return cluster;
    }

    private CassandraClusterSingleton() throws RuntimeException {
       try {
            EmbeddedCassandraServerHelper.startEmbeddedCassandra();
            // Let Cassandra initialization before creating
            // the session. Solve very fast computer tests run.
            Thread.sleep(2000);
            this.session = new CassandraSession(CLUSTER_IP, CLUSTER_PORT_TEST, KEYSPACE_NAME, DEFAULT_REPLICATION_FACTOR);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Return a configuration for the runnning MiniCluster.
     * 
     * @return
     */
    public Session getConf() {
        return session;
    }

    /**
     * Create a specific table.
     * 
     * @param tableName
     *            the table name
     */
    public void ensureTable(String tableName) {
        if (tableName.equals("mailbox")) {
            session.execute("CREATE TABLE IF NOT EXISTS " + session.getLoggedKeyspace() + ".mailbox (" + "id uuid PRIMARY KEY," + "name text, namespace text," + "uidvalidity bigint," + "user text," + "path text" + ");");

            session.execute("CREATE INDEX IF NOT EXISTS ON " + session.getLoggedKeyspace() + ".mailbox(path);");
        } else if (tableName.equals("messageCounter")) {
            session.execute("CREATE TABLE IF NOT EXISTS " + session.getLoggedKeyspace() + ".messageCounter (" + "mailboxId UUID PRIMARY KEY," + "nextUid bigint," + ");");
        } else if (tableName.equals("mailboxCounters")) {
            session.execute("CREATE TABLE IF NOT EXISTS " + session.getLoggedKeyspace() + ".mailboxCounters (" + "mailboxId UUID PRIMARY KEY," + "count counter," + "unseen counter," + "nextModSeq counter" + ");");
        } else if (tableName.equals("message")) {
            session.execute("CREATE TABLE IF NOT EXISTS " + session.getLoggedKeyspace() + ".message (" + "mailboxId UUID," + "uid bigint," + "internalDate timestamp," + "bodyStartOctet int," + "content blob," + "modSeq bigint," + "mediaType text," + "subType text," + "fullContentOctets int,"
                    + "bodyOctets int," + "textualLineCount bigint," + "bodyContent blob," + "headerContent blob," + "flagAnswered boolean," + "flagDeleted boolean," + "flagDraft boolean," + "flagRecent boolean," + "flagSeen boolean," + "flagFlagged boolean," + "flagUser boolean,"
                    + "PRIMARY KEY (mailboxId, uid)" + ");");
        } else if (tableName.equals("subscription")) {
            session.execute("CREATE TABLE IF NOT EXISTS " + session.getLoggedKeyspace() + ".subscription (" + "user text," + "mailbox text," + "PRIMARY KEY (mailbox, user)" + ");");
        } else {
            throw new NotImplementedException("We don't support the class " + tableName);
        }
    }

    /**
     * Ensure all tables
     */
    public void ensureAllTables() {
        ensureTable("mailbox");
        ensureTable("mailboxCounters");
        ensureTable("message");
        ensureTable("subscription");
    }

    /**
     * Delete all rows from specified table.
     * 
     * @param tableName
     */
    public void clearTable(String tableName) {
        session.execute("TRUNCATE " + tableName + ";");
    }

    /**
     * Delete all rows for all tables.
     */
    public void clearAllTables() {
        clearTable("mailbox");
        clearTable("mailboxCounters");
        clearTable("message");
        clearTable("subscription");
    }

}
