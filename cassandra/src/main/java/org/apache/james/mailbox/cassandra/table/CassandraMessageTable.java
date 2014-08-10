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

package org.apache.james.mailbox.cassandra.table;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;

public interface CassandraMessageTable {

    String TABLE_NAME = "message";
    String MAILBOX_ID = "mailboxId";
    String IMAP_UID = "uid";
    String INTERNAL_DATE = "internalDate";
    String BODY_START_OCTET = "bodyStartOctet";
    String CONTENT = "content";
    String MOD_SEQ = "modSeq";
    String MEDIA_TYPE = "mediaType";
    String SUB_TYPE = "subType";
    String FULL_CONTENT_OCTETS = "fullContentOctets";
    String BODY_OCTECTS = "bodyOctets";
    String TEXTUAL_LINE_COUNT = "textualLineCount";
    String BODY_CONTENT = "bodyContent";
    String HEADER_CONTENT = "headerContent";
    String[] FIELDS = { MAILBOX_ID, IMAP_UID, INTERNAL_DATE, MOD_SEQ, BODY_START_OCTET, MEDIA_TYPE, SUB_TYPE, FULL_CONTENT_OCTETS, BODY_OCTECTS, Flag.ANSWERED, Flag.DELETED, Flag.DRAFT, Flag.FLAGGED, Flag.RECENT, Flag.SEEN, Flag.USER, BODY_CONTENT, HEADER_CONTENT, TEXTUAL_LINE_COUNT };

    interface Flag {
        String ANSWERED = "flagAnswered";
        String DELETED = "flagDeleted";
        String DRAFT = "flagDraft";
        String RECENT = "flagRecent";
        String SEEN = "flagSeen";
        String FLAGGED = "flagFlagged";
        String USER = "flagUser";
        String[] ALL = { ANSWERED, DELETED, DRAFT, RECENT, SEEN, FLAGGED, USER };

        ImmutableMap<String, javax.mail.Flags.Flag> JAVAX_MAIL_FLAG = new Builder<String, javax.mail.Flags.Flag>().put(ANSWERED, javax.mail.Flags.Flag.ANSWERED).put(DELETED, javax.mail.Flags.Flag.DELETED).put(DRAFT, javax.mail.Flags.Flag.DRAFT).put(RECENT, javax.mail.Flags.Flag.RECENT)
                .put(SEEN, javax.mail.Flags.Flag.SEEN).put(FLAGGED, javax.mail.Flags.Flag.FLAGGED).put(USER, javax.mail.Flags.Flag.USER).build();

    }
}
