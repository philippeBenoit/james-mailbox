package org.apache.james.mailbox.cassandra.table;

public interface CassandraSubscriptionTable {

    String TABLE_NAME = "subscription";
    String USER = "user";
    String MAILBOX = "mailbox";
    String[] FIELDS = { MAILBOX, USER };

}
