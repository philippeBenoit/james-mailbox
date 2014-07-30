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