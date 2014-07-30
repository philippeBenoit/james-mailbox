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

import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import javax.inject.Inject;
import javax.mail.Flags;
import javax.mail.Flags.Flag;
import javax.mail.util.SharedByteArrayInputStream;

import org.apache.james.mailbox.MailboxSession;
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

public class CassandraMessageMapper implements MessageMapper<UUID> {

    private static final String[] FIELDS = { MessageTable.MAILBOX_ID, MessageTable.IMAP_UID, MessageTable.INTERNAL_DATE, MessageTable.MOD_SEQ, MessageTable.BODY_START_OCTET, MessageTable.MEDIA_TYPE, MessageTable.SUB_TYPE, MessageTable.FULL_CONTENT_OCTETS, MessageTable.BODY_OCTECTS,
            MessageTable.Flag.ANSWERED, MessageTable.Flag.DELETED, MessageTable.Flag.DRAFT, MessageTable.Flag.FLAGGED, MessageTable.Flag.RECENT, MessageTable.Flag.SEEN, MessageTable.Flag.USER, MessageTable.BODY_CONTENT, MessageTable.HEADER_CONTENT };
    private Session session;
    private ModSeqProvider<UUID> modSeqProvider;
    private MailboxSession mailboxSession;
    private UidProvider<UUID> uidProvider;

    @Inject
    public CassandraMessageMapper(Session session, UidProvider<UUID> uidProvider, ModSeqProvider<UUID> modSeqProvider) {
        this.session = session;
        this.uidProvider = uidProvider;
        this.modSeqProvider = modSeqProvider;
    }

    @Override
    public long countMessagesInMailbox(Mailbox<UUID> mailbox) throws MailboxException {
        ResultSet results = session.execute(select(MailboxCountersTable.COUNT).from(MailboxCountersTable.TABLE_NAME).where(eq(MailboxCountersTable.MAILBOX_ID, mailbox.getMailboxId())));
        if (results.isExhausted()) {
            return 0;
        } else {
            return results.one().getLong(MailboxCountersTable.COUNT);
        }
    }

    @Override
    public long countUnseenMessagesInMailbox(Mailbox<UUID> mailbox) throws MailboxException {
        ResultSet results = session.execute(select(MailboxCountersTable.COUNT).from(MailboxCountersTable.TABLE_NAME).where(eq(MailboxCountersTable.MAILBOX_ID, mailbox.getMailboxId())));
        if (!results.isExhausted()) {
            Row row = results.one();
            if (row.getColumnDefinitions().contains(MailboxCountersTable.UNSEEN)) {
                return row.getLong(MailboxCountersTable.UNSEEN);
            }
        }
        return 0;
    }

    @Override
    public void delete(Mailbox<UUID> mailbox, Message<UUID> message) throws MailboxException {
        session.execute(QueryBuilder.delete().from(MessageTable.TABLE_NAME).where(eq(MessageTable.MAILBOX_ID, mailbox.getMailboxId())).and(eq(MessageTable.IMAP_UID, message.getUid())));
        decrementCount(mailbox);
        if (!message.isSeen()) {
            decrementUnseen(mailbox);
        }
    }

    private void decrementCount(Mailbox<UUID> mailbox) {
        updateMailbox(mailbox, decr(MailboxCountersTable.COUNT));
    }

    private void incrementCount(Mailbox<UUID> mailbox) {
        updateMailbox(mailbox, incr(MailboxCountersTable.COUNT));
    }

    private void decrementUnseen(Mailbox<UUID> mailbox) {
        updateMailbox(mailbox, decr(MailboxCountersTable.UNSEEN));
    }

    private void incrementUnseen(Mailbox<UUID> mailbox) {
        updateMailbox(mailbox, incr(MailboxCountersTable.UNSEEN));
    }

    private void updateMailbox(Mailbox<UUID> mailbox, Assignment operation) {
        session.execute(update(MailboxCountersTable.TABLE_NAME).with(operation).where(eq(MailboxCountersTable.MAILBOX_ID, mailbox.getMailboxId())));
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

    private Message<UUID> message(Row row) {
        SimpleMessage<UUID> message = new SimpleMessage<UUID>(row.getDate(MessageTable.INTERNAL_DATE), row.getInt(MessageTable.FULL_CONTENT_OCTETS), row.getInt(MessageTable.BODY_START_OCTET), new SharedByteArrayInputStream(row.getBytes(MessageTable.BODY_CONTENT).array()), new Flags(),
                new PropertyBuilder(), row.getUUID(MessageTable.MAILBOX_ID));
        message.setUid(row.getLong(MessageTable.IMAP_UID));
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
        return select(FIELDS).from(MessageTable.TABLE_NAME).where(eq(MessageTable.MAILBOX_ID, mailbox.getMailboxId()));
    }

    private Where selectFrom(Mailbox<UUID> mailbox, long uid) {
        return select(FIELDS).from(MessageTable.TABLE_NAME).where(eq(MessageTable.MAILBOX_ID, mailbox.getMailboxId())).and(gt(MessageTable.IMAP_UID, uid));
    }

    private Where selectRange(Mailbox<UUID> mailbox, long from, long to) {
        return select(FIELDS).from(MessageTable.TABLE_NAME).where(eq(MessageTable.MAILBOX_ID, mailbox.getMailboxId())).and(gt(MessageTable.IMAP_UID, from)).and(lt(MessageTable.IMAP_UID, to));
    }

    private Where selectMessage(Mailbox<UUID> mailbox, long uid) {
        return select(FIELDS).from(MessageTable.TABLE_NAME).where(eq(MessageTable.MAILBOX_ID, mailbox.getMailboxId())).and(eq(MessageTable.IMAP_UID, uid));
    }

    @Override
    public List<Long> findRecentMessageUidsInMailbox(Mailbox<UUID> mailbox) throws MailboxException {
        ImmutableList.Builder<Long> result = ImmutableList.<Long> builder();
        ResultSet rows = session.execute(selectAll(mailbox).orderBy(asc(MessageTable.IMAP_UID)));
        for (Row row : rows) {
            Message<UUID> message = message(row);
            if (message.isRecent()) {
                result.add(message.getUid());
            }
        }
        return result.build();
    }

    @Override
    public Long findFirstUnseenMessageUid(Mailbox<UUID> mailbox) throws MailboxException {
        ResultSet rows = session.execute(selectAll(mailbox).orderBy(asc(MessageTable.IMAP_UID)));
        for (Row row : rows) {
            Message<UUID> message = message(row);
            if (!message.isSeen()) {
                return message.getUid();
            }
        }
        return null;
    }

    @Override
    public Map<Long, MessageMetaData> expungeMarkedForDeletionInMailbox(final Mailbox<UUID> mailbox, MessageRange set) throws MailboxException {
        ImmutableMap.Builder<Long, MessageMetaData> deletedMessages = ImmutableMap.builder();
        ResultSet messages = session.execute(buildQuery(mailbox, set));
        for (Row row : messages) {
            Message<UUID> message = message(row);
            if (message.isDeleted()) {
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
        incrementUnseen(mailbox);
        incrementCount(mailbox);
        return messageMetaData;
    }

    private MessageMetaData save(Mailbox<UUID> mailbox, Message<UUID> message) throws MailboxException {
        try {
            Insert query = insertInto(MessageTable.TABLE_NAME).value(MessageTable.MAILBOX_ID, mailbox.getMailboxId()).value(MessageTable.IMAP_UID, message.getUid()).value(MessageTable.MOD_SEQ, message.getModSeq()).value(MessageTable.INTERNAL_DATE, message.getInternalDate())
                    .value(MessageTable.MEDIA_TYPE, message.getMediaType()).value(MessageTable.SUB_TYPE, message.getSubType()).value(MessageTable.FULL_CONTENT_OCTETS, message.getFullContentOctets()).value(MessageTable.BODY_OCTECTS, message.getBodyOctets())
                    .value(MessageTable.Flag.ANSWERED, message.isAnswered()).value(MessageTable.Flag.DELETED, message.isDeleted()).value(MessageTable.Flag.DRAFT, message.isDraft()).value(MessageTable.Flag.FLAGGED, message.isFlagged()).value(MessageTable.Flag.RECENT, message.isRecent())
                    .value(MessageTable.Flag.SEEN, message.isSeen()).value(MessageTable.Flag.USER, message.createFlags().contains(Flag.USER)).value(MessageTable.BODY_CONTENT, bindMarker()).value(MessageTable.HEADER_CONTENT, bindMarker());
            if (message.getTextualLineCount() != null) {
                query.value(MessageTable.TEXTUAL_LINE_COUNT, message.getTextualLineCount());
            }
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
        ResultSet messages = session.execute(buildQuery(mailbox, set));
        for (Row row : messages) {
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
