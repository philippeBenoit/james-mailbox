/* ***** BEGIN LICENSE BLOCK *****
 * 
 * Copyright (C) 2013 Linagora
 *
 * This program is free software: you can redistribute it and/or 
 * modify it under the terms of the GNU Affero General Public License as 
 * published by the Free Software Foundation, either version 3 of the 
 * License, or (at your option) any later version, provided you comply 
 * with the Additional Terms applicable for OBM connector by Linagora 
 * pursuant to Section 7 of the GNU Affero General Public License, 
 * subsections (b), (c), and (e), pursuant to which you must notably (i) retain 
 * the “Message sent thanks to OBM, Free Communication by Linagora” 
 * signature notice appended to any and all outbound messages 
 * (notably e-mail and meeting requests), (ii) retain all hypertext links between 
 * OBM and obm.org, as well as between Linagora and linagora.com, and (iii) refrain 
 * from infringing Linagora intellectual property rights over its trademarks 
 * and commercial brands. Other Additional Terms apply, 
 * see <http://www.linagora.com/licenses/> for more details. 
 *
 * This program is distributed in the hope that it will be useful, 
 * but WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY 
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License 
 * for more details. 
 *
 * You should have received a copy of the GNU Affero General Public License 
 * and its applicable Additional Terms for OBM along with this program. If not, 
 * see <http://www.gnu.org/licenses/> for the GNU Affero General Public License version 3 
 * and <http://www.linagora.com/licenses/> for the Additional Terms applicable to 
 * OBM connectors. 
 * 
 * ***** END LICENSE BLOCK ***** */

package org.apache.james.mailbox.cassandra;

import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * A Cassandra session with the default keyspace
 * 
 */
public class CassandraSession implements Session {
    private final static String DEFAULT_CLUSTER_IP = "localhost";
    private final static int DEFAULT_CLUSTER_PORT = 9042;
    private final static String DEFAULT_KEYSPACE_NAME = "apache_james";
    private final static int DEFAULT_REPLICATION_FACTOR = 1;

    private Session session;

    public CassandraSession(String ip, int port, String keyspace, int replicationFactor) {
        Cluster cluster = Cluster.builder().addContactPoint(ip).withPort(port).build();
        if (cluster.getMetadata().getKeyspace(keyspace) == null) {
            initDatabase(cluster, keyspace, replicationFactor);
        }
        session = cluster.connect(keyspace);
    }

    private void initDatabase(Cluster cluster, String keyspace, int replicationFactor) {
        session = cluster.connect();
        session.execute("CREATE KEYSPACE IF NOT EXISTS " + keyspace + " WITH replication " + "= {'class':'SimpleStrategy', 'replication_factor':" + replicationFactor + "};");
        session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".mailbox (" + "id uuid PRIMARY KEY," + "name text, namespace text," + "uidvalidity bigint," + "user text," + "path text" + ");");
        session.execute("CREATE INDEX IF NOT EXISTS ON " + keyspace + ".mailbox(path);");
        session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".messageCounter (" + "mailboxId UUID PRIMARY KEY," + "nextUid bigint," + ");");
        session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".mailboxCounters (" + "mailboxId UUID PRIMARY KEY," + "count counter," + "unseen counter," + "nextModSeq counter" + ");");
        session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".message (" + "mailboxId UUID," + "uid bigint," + "internalDate timestamp," + "bodyStartOctet int," + "content blob," + "modSeq bigint," + "mediaType text," + "subType text," + "fullContentOctets int," + "bodyOctets int,"
                + "textualLineCount bigint," + "bodyContent blob," + "headerContent blob," + "flagAnswered boolean," + "flagDeleted boolean," + "flagDraft boolean," + "flagRecent boolean," + "flagSeen boolean," + "flagFlagged boolean," + "flagUser boolean," + "PRIMARY KEY (mailboxId, uid)" + ");");
        session.execute("CREATE TABLE IF NOT EXISTS " + keyspace + ".subscription (" + "user text," + "mailbox text," + "PRIMARY KEY (mailbox, user)" + ");");
        session.close();
    }

    public CassandraSession() {
        this(DEFAULT_CLUSTER_IP, DEFAULT_CLUSTER_PORT, DEFAULT_KEYSPACE_NAME, DEFAULT_REPLICATION_FACTOR);
    }

    @Override
    public String getLoggedKeyspace() {
        return session.getLoggedKeyspace();
    }

    @Override
    public Session init() {
        return session.init();
    }

    @Override
    public ResultSet execute(String query) {
        return session.execute(query);
    }

    @Override
    public ResultSet execute(String query, Object... values) {
        return session.execute(query, values);
    }

    @Override
    public ResultSet execute(Statement statement) {
        return session.execute(statement);
    }

    @Override
    public ResultSetFuture executeAsync(String query) {
        return session.executeAsync(query);
    }

    @Override
    public ResultSetFuture executeAsync(String query, Object... values) {
        return session.executeAsync(query, values);
    }

    @Override
    public ResultSetFuture executeAsync(Statement statement) {
        return session.executeAsync(statement);
    }

    @Override
    public PreparedStatement prepare(String query) {
        return session.prepare(query);
    }

    @Override
    public PreparedStatement prepare(RegularStatement statement) {
        return session.prepare(statement);
    }

    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(String query) {
        return session.prepareAsync(query);
    }

    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
        return session.prepareAsync(statement);
    }

    @Override
    public CloseFuture closeAsync() {
        return session.closeAsync();
    }

    @Override
    public void close() {
        session.close();
    }

    @Override
    public boolean isClosed() {
        return session.isClosed();
    }

    @Override
    public Cluster getCluster() {
        return session.getCluster();
    }

}
