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

import static com.datastax.driver.core.querybuilder.QueryBuilder.asc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.decr;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.gt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.incr;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.lt;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.BODY_CONTENT;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.BODY_OCTECTS;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.BODY_START_OCTET;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.FIELDS;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.FULL_CONTENT_OCTETS;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.HEADER_CONTENT;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.IMAP_UID;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.INTERNAL_DATE;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.MAILBOX_ID;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.MEDIA_TYPE;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.MOD_SEQ;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.SUB_TYPE;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.TABLE_NAME;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.TEXTUAL_LINE_COUNT;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.ANSWERED;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.DELETED;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.DRAFT;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.FLAGGED;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.JAVAX_MAIL_FLAG;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.RECENT;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.SEEN;
import static org.apache.james.mailbox.cassandra.table.CassandraMessageTable.Flag.USER;

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.mail.Flags;
import javax.mail.Flags.Flag;
import javax.mail.util.SharedByteArrayInputStream;

import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.cassandra.table.CassandraMailboxCountersTable;
import org.apache.james.mailbox.cassandra.table.CassandraMessageTable;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.model.MessageMetaData;
import org.apache.james.mailbox.model.MessageRange;
import org.apache.james.mailbox.model.UpdatedFlags;
import org.apache.james.mailbox.store.SimpleMessageMetaData;
import org.apache.james.mailbox.store.mail.MessageMapper;
import org.apache.james.mailbox.store.mail.ModSeqProvider;
import org.apache.james.mailbox.store.mail.UidProvider;
import org.apache.james.mailbox.store.mail.model.Mailbox;
import org.apache.james.mailbox.store.mail.model.Message;
import org.apache.james.mailbox.store.mail.model.impl.PropertyBuilder;
import org.apache.james.mailbox.store.mail.model.impl.SimpleMessage;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Assignment;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select.Where;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.ImmutableSortedSet.Builder;
import com.google.common.io.ByteStreams;
import com.google.common.primitives.Bytes;

/**
 * Cassandra implementation of a {@link MessageMapper}.
 */
public class CassandraMessageMapper implements MessageMapper<UUID> {

    private Session session;
    private ModSeqProvider<UUID> modSeqProvider;
    private MailboxSession mailboxSession;
    private UidProvider<UUID> uidProvider;

    public CassandraMessageMapper(Session session, CassandraUidProvider uidProvider, ModSeqProvider<UUID> modSeqProvider) {
        this.session = session;
        this.uidProvider = uidProvider;
        this.modSeqProvider = modSeqProvider;
    }

    public CassandraMessageMapper(Session session, CassandraUidProvider uidProvider, CassandraModSeqProvider modSeqProvider, MailboxSession mailboxSession) {
        this(session, uidProvider, modSeqProvider);
        this.mailboxSession = mailboxSession;
    }

    @Override
    public long countMessagesInMailbox(Mailbox<UUID> mailbox) throws MailboxException {
        ResultSet results = session.execute(select(CassandraMailboxCountersTable.COUNT).from(CassandraMailboxCountersTable.TABLE_NAME).where(eq(CassandraMailboxCountersTable.MAILBOX_ID, mailbox.getMailboxId())));
        return results.isExhausted() ? 0 : results.one().getLong(CassandraMailboxCountersTable.COUNT);
    }

    @Override
    public long countUnseenMessagesInMailbox(Mailbox<UUID> mailbox) throws MailboxException {
        ResultSet results = session.execute(select(CassandraMailboxCountersTable.UNSEEN).from(CassandraMailboxCountersTable.TABLE_NAME).where(eq(CassandraMailboxCountersTable.MAILBOX_ID, mailbox.getMailboxId())));
        if (!results.isExhausted()) {
            Row row = results.one();
            if (row.getColumnDefinitions().contains(CassandraMailboxCountersTable.UNSEEN)) {
                return row.getLong(CassandraMailboxCountersTable.UNSEEN);
            }
        }
        return 0;
    }

    @Override
    public void delete(Mailbox<UUID> mailbox, Message<UUID> message) throws MailboxException {
        session.execute(QueryBuilder.delete().from(TABLE_NAME).where(eq(MAILBOX_ID, mailbox.getMailboxId())).and(eq(IMAP_UID, message.getUid())));
        decrementCount(mailbox);
        if (!message.isSeen()) {
            decrementUnseen(mailbox);
        }
    }

    private void decrementCount(Mailbox<UUID> mailbox) {
        updateMailbox(mailbox, decr(CassandraMailboxCountersTable.COUNT));
    }

    private void incrementCount(Mailbox<UUID> mailbox) {
        updateMailbox(mailbox, incr(CassandraMailboxCountersTable.COUNT));
    }

    private void decrementUnseen(Mailbox<UUID> mailbox) {
        updateMailbox(mailbox, decr(CassandraMailboxCountersTable.UNSEEN));
    }

    private void incrementUnseen(Mailbox<UUID> mailbox) {
        updateMailbox(mailbox, incr(CassandraMailboxCountersTable.UNSEEN));
    }

    private void updateMailbox(Mailbox<UUID> mailbox, Assignment operation) {
        session.execute(update(CassandraMailboxCountersTable.TABLE_NAME).with(operation).where(eq(CassandraMailboxCountersTable.MAILBOX_ID, mailbox.getMailboxId())));
    }

    @Override
    public Iterator<Message<UUID>> findInMailbox(Mailbox<UUID> mailbox, MessageRange set, FetchType ftype, int max) throws MailboxException {
        Builder<Message<UUID>> result = ImmutableSortedSet.<Message<UUID>> naturalOrder();
        ResultSet rows = session.execute(buildQuery(mailbox, set));
        for (Row row : rows) {
            result.add(message(row));
        }
        return result.build().iterator();
    }

    private byte[] getFullContent(Row row) {
        byte[] headerContent = new byte[row.getBytes(HEADER_CONTENT).remaining()];
        byte[] bodyContent = new byte[row.getBytes(BODY_CONTENT).remaining()];
        row.getBytes(HEADER_CONTENT).get(headerContent);
        row.getBytes(BODY_CONTENT).get(bodyContent);
        return Bytes.concat(headerContent, bodyContent);
    }

    private Flags getFlags(Row row) {
        Flags flags = new Flags();
        for (String flag : CassandraMessageTable.Flag.ALL) {
            if (row.getBool(flag)) {
                flags.add(JAVAX_MAIL_FLAG.get(flag));
            }
        }
        return flags;
    }

    private PropertyBuilder getPropertyBuilder(Row row) {
        PropertyBuilder property = new PropertyBuilder();
        property.setSubType(row.getString(SUB_TYPE));
        property.setMediaType(row.getString(MEDIA_TYPE));
        property.setTextualLineCount(row.getLong(TEXTUAL_LINE_COUNT));
        return property;
    }

    private Message<UUID> message(Row row) {
        SimpleMessage<UUID> message = new SimpleMessage<UUID>(row.getDate(INTERNAL_DATE), row.getInt(FULL_CONTENT_OCTETS), row.getInt(BODY_START_OCTET), new SharedByteArrayInputStream(getFullContent(row)), getFlags(row), getPropertyBuilder(row), row.getUUID(MAILBOX_ID));
        message.setUid(row.getLong(IMAP_UID));
        return message;
    }

    private Where buildQuery(Mailbox<UUID> mailbox, MessageRange set) {
        final MessageRange.Type type = set.getType();
        switch (type) {
        case ALL:
            return selectAll(mailbox);
        case FROM:
            return selectFrom(mailbox, set.getUidFrom());
        case RANGE:
            return selectRange(mailbox, set.getUidFrom(), set.getUidTo());
        case ONE:
            return selectMessage(mailbox, set.getUidFrom());
        }
        throw new UnsupportedOperationException();
    }

    private Where selectAll(Mailbox<UUID> mailbox) {
        return select(FIELDS).from(TABLE_NAME).where(eq(MAILBOX_ID, mailbox.getMailboxId()));
    }

    private Where selectFrom(Mailbox<UUID> mailbox, long uid) {
        return select(FIELDS).from(TABLE_NAME).where(eq(MAILBOX_ID, mailbox.getMailboxId())).and(gt(IMAP_UID, uid));
    }

    private Where selectRange(Mailbox<UUID> mailbox, long from, long to) {
        return select(FIELDS).from(TABLE_NAME).where(eq(MAILBOX_ID, mailbox.getMailboxId())).and(gt(IMAP_UID, from)).and(lt(IMAP_UID, to));
    }

    private Where selectMessage(Mailbox<UUID> mailbox, long uid) {
        return select(FIELDS).from(TABLE_NAME).where(eq(MAILBOX_ID, mailbox.getMailboxId())).and(eq(IMAP_UID, uid));
    }

    @Override
    public List<Long> findRecentMessageUidsInMailbox(Mailbox<UUID> mailbox) throws MailboxException {
        ImmutableList.Builder<Long> result = ImmutableList.<Long> builder();
        ResultSet rows = session.execute(selectAll(mailbox).orderBy(asc(IMAP_UID)));
        for (Row row : rows) {
            if (row.getBool(RECENT)) {
                result.add(row.getLong(IMAP_UID));
            }
        }
        return result.build();
    }

    @Override
    public Long findFirstUnseenMessageUid(Mailbox<UUID> mailbox) throws MailboxException {
        ResultSet rows = session.execute(selectAll(mailbox).orderBy(asc(IMAP_UID)));
        for (Row row : rows) {
            if (!row.getBool(SEEN)) {
                return row.getLong(IMAP_UID);
            }
        }
        return null;
    }

    @Override
    public Map<Long, MessageMetaData> expungeMarkedForDeletionInMailbox(final Mailbox<UUID> mailbox, MessageRange set) throws MailboxException {
        ImmutableMap.Builder<Long, MessageMetaData> deletedMessages = ImmutableMap.builder();
        ResultSet messages = session.execute(buildQuery(mailbox, set));
        for (Row row : messages) {
            if (row.getBool(DELETED)) {
                Message<UUID> message = message(row);
                delete(mailbox, message);
                deletedMessages.put(message.getUid(), new SimpleMessageMetaData(message));
            }
        }
        return deletedMessages.build();
    }

    @Override
    public MessageMetaData move(Mailbox<UUID> mailbox, Message<UUID> original) throws MailboxException {
        throw new UnsupportedOperationException("Not implemented - see https://issues.apache.org/jira/browse/IMAP-370");
    }

    @Override
    public void endRequest() {
        // Do nothing
    }

    @Override
    public long getHighestModSeq(Mailbox<UUID> mailbox) throws MailboxException {
        return modSeqProvider.highestModSeq(mailboxSession, mailbox);
    }

    @Override
    public <T> T execute(Transaction<T> transaction) throws MailboxException {
        return transaction.run();
    }

    @Override
    public MessageMetaData add(Mailbox<UUID> mailbox, Message<UUID> message) throws MailboxException {
        message.setUid(uidProvider.nextUid(mailboxSession, mailbox));
        message.setModSeq(modSeqProvider.nextModSeq(mailboxSession, mailbox));
        MessageMetaData messageMetaData = save(mailbox, message);
        if (!message.isSeen()) {
            incrementUnseen(mailbox);
        }
        incrementCount(mailbox);
        return messageMetaData;
    }

    private MessageMetaData save(Mailbox<UUID> mailbox, Message<UUID> message) throws MailboxException {
        try {
            Insert query = insertInto(TABLE_NAME).value(MAILBOX_ID, mailbox.getMailboxId()).value(IMAP_UID, message.getUid()).value(MOD_SEQ, message.getModSeq()).value(INTERNAL_DATE, message.getInternalDate()).value(MEDIA_TYPE, message.getMediaType())
                    .value(BODY_START_OCTET, message.getFullContentOctets() - message.getBodyOctets()).value(SUB_TYPE, message.getSubType()).value(FULL_CONTENT_OCTETS, message.getFullContentOctets()).value(BODY_OCTECTS, message.getBodyOctets()).value(ANSWERED, message.isAnswered())
                    .value(DELETED, message.isDeleted()).value(DRAFT, message.isDraft()).value(FLAGGED, message.isFlagged()).value(RECENT, message.isRecent()).value(SEEN, message.isSeen()).value(USER, message.createFlags().contains(Flag.USER)).value(BODY_CONTENT, bindMarker())
                    .value(HEADER_CONTENT, bindMarker()).value(TEXTUAL_LINE_COUNT, message.getTextualLineCount());
            PreparedStatement preparedStatement = session.prepare(query.toString());
            BoundStatement boundStatement = preparedStatement.bind(toByteBuffer(message.getBodyContent()), toByteBuffer(message.getHeaderContent()));
            session.execute(boundStatement);
            return new SimpleMessageMetaData(message);
        } catch (IOException e) {
            throw new MailboxException("Error saving mail", e);
        }
    }

    private ByteBuffer toByteBuffer(InputStream stream) throws IOException {
        return ByteBuffer.wrap(ByteStreams.toByteArray(stream));
    }

    @Override
    public Iterator<UpdatedFlags> updateFlags(Mailbox<UUID> mailbox, Flags flags, boolean value, boolean replace, MessageRange set) throws MailboxException {
        ImmutableList.Builder<UpdatedFlags> result = ImmutableList.builder();
        for (Row row : session.execute(buildQuery(mailbox, set))) {
            Message<UUID> message = message(row);
            Flags originFlags = message.createFlags();
            Flags updatedFlags = buildFlags(message, flags, value, replace);
            message.setFlags(updatedFlags);
            message.setModSeq(modSeqProvider.nextModSeq(mailboxSession, mailbox));
            save(mailbox, message);
            result.add(new UpdatedFlags(message.getUid(), message.getModSeq(), originFlags, updatedFlags));
        }
        return result.build().iterator();
    }

    private Flags buildFlags(Message<UUID> message, Flags flags, boolean value, boolean replace) {
        if (replace) {
            return message.createFlags();
        } else {
            Flags updatedFlags = message.createFlags();
            if (value) {
                updatedFlags.add(flags);
            } else {
                updatedFlags.remove(flags);
            }
            return updatedFlags;
        }
    }

    @Override
    public MessageMetaData copy(Mailbox<UUID> mailbox, Message<UUID> original) throws MailboxException {
        original.setUid(uidProvider.nextUid(mailboxSession, mailbox));
        original.setModSeq(modSeqProvider.nextModSeq(mailboxSession, mailbox));
        return save(mailbox, original);
    }

    @Override
    public long getLastUid(Mailbox<UUID> mailbox) throws MailboxException {
        return uidProvider.lastUid(mailboxSession, mailbox);
    }

}
