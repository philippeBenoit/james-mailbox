package org.apache.james.mailbox.store.search;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;
import java.util.TimeZone;

import javax.mail.Flags;

import org.apache.commons.lang.NotImplementedException;
import org.apache.james.mailbox.MailboxSession;
import org.apache.james.mailbox.exception.MailboxException;
import org.apache.james.mailbox.model.MessageRange;
import org.apache.james.mailbox.model.SearchQuery;
import org.apache.james.mailbox.store.mail.MessageMapperFactory;
import org.apache.james.mailbox.store.mail.model.Mailbox;
import org.apache.james.mailbox.store.mail.model.Message;
import org.apache.james.mime4j.MimeException;
import org.apache.james.mime4j.dom.Header;
import org.apache.james.mime4j.dom.address.Address;
import org.apache.james.mime4j.dom.address.AddressList;
import org.apache.james.mime4j.dom.address.Group;
import org.apache.james.mime4j.dom.address.MailboxList;
import org.apache.james.mime4j.dom.datetime.DateTime;
import org.apache.james.mime4j.dom.field.DateTimeField;
import org.apache.james.mime4j.field.address.AddressFormatter;
import org.apache.james.mime4j.field.address.LenientAddressBuilder;
import org.apache.james.mime4j.field.datetime.parser.DateTimeParser;
import org.apache.james.mime4j.message.SimpleContentHandler;
import org.apache.james.mime4j.parser.MimeStreamParser;
import org.apache.james.mime4j.stream.BodyDescriptor;
import org.apache.james.mime4j.stream.MimeConfig;
import org.apache.james.mime4j.util.MimeUtil;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.index.Index;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.RangeQueryBuilder;

public class ElasticsearchMessageSearchIndex<Id> extends ListeningMessageSearchIndex<Id> {
    
    private Client client;
    private final String INDEX_NAME = "messages";
    private final String TYPE_NAME = "message";
    
    private final static String MEDIA_TYPE_TEXT = "text"; 
    private final static String MEDIA_TYPE_MESSAGE = "message";
    private final static String DEFAULT_ENCODING = "US-ASCII";

    public static interface Field {
        /**
         * Contain the unique id of the message in {@link Index}
         */
        public final static String ID = "id";

        /**
         * Contain uid of the {@link Message}
         */
        public final static String UID = "uid";

        /**
         * Contain the {@link Flags} of the {@link Message}
         */
        public final static String FLAGS = "flags";

        /**
         * Contain the size of the {@link Message}
         */
        public final static String SIZE = "size";

        /**
         * Contain the body of the {@link Message}
         */
        public final static String BODY = "body";

        /**
         * Contain the FROM-Address of the {@link Message}
         */
        public final static String FROM = "from";

        /**
         * Contain the internalDate of the message with YEAR-Resolution
         */
        public final static String INTERNAL_DATE = "internalDate";

        /**
         * Contain the id of the {@link Mailbox}
         */
        public final static String MAILBOX_ID = "mailboxIsd";
        
        /**
         * Contain the TO-Address of the message
         */
        public final static String TO = "to";
        
        /**
         * Contain the CC-Address of the message
         */
        public final static String CC = "cc";
        
        /**
         * Contain the BCC-Address of the message
         */
        public final static String BCC = "bcc";
        
        /**
         * Prefix which will be used for each message header to store
         */
        public final static String PREFIX_HEADER ="header_";
        
        /**
         * Contain the whole message header of the {@link Message}
         */
        public final static String HEADERS ="headers";
        
        public final static String BASE_SUBJECT = "baseSubject";
        
        public final static String FIRST_FROM_MAILBOX_NAME = "firstFromMailboxName";
        
        public final static String FIRST_FROM_MAILBOX_DISPLAY = "firstFromMailboxDisplay";
        
        public final static String FIRST_TO_MAILBOX_NAME = "firstToMailboxName";

        public final static String FIRST_TO_MAILBOX_DISPLAY = "firstToMailboxDisplay";
        
        public final static String FIRST_CC_MAILBOX_NAME = "firstCcMailboxName";
        
        public final static String SENT_DATE_SORT_FIELD_MILLISECOND_RESOLUTION = "sentdateSort";

        /**
         * Contain the Date header of the message with YEAR-Resolution
         */
        public final static String SENT_DATE_YEAR_RESOLUTION = "sentdateYearResolution";

        /**
         * Contain the Date header of the message with MONTH-Resolution
         */
        public final static String SENT_DATE_MONTH_RESOLUTION = "sentdateMonthResolution";

        /**
         * Contain the Date header of the message with DAY-Resolution
         */
        public final static String SENT_DATE_DAY_RESOLUTION = "sentdateDayResolution";

        /**
         * Contain the Date header of the message with HOUR-Resolution
         */
        public final static String SENT_DATE_HOUR_RESOLUTION = "sentdateHourResolution";

        /**
         * Contain the Date header of the message with MINUTE-Resolution
         */
        public final static String SENT_DATE_MINUTE_RESOLUTION = "sentdateMinuteResolution";

        /**
         * Contain the Date header of the message with SECOND-Resolution
         */
        public final static String SENT_DATE_SECOND_RESOLUTION = "sentdateSecondResolution";

        /**
         * Contain the Date header of the message with MILLISECOND-Resolution
         */
        public final static String SENT_DATE_MILLISECOND_RESOLUTION = "sentdateMillisecondResolution";
    }

    public ElasticsearchMessageSearchIndex(Client client) {
        super(null);
        this.client = client;
    }
    
    public ElasticsearchMessageSearchIndex(MessageMapperFactory<Id> factory, Client client) {
        super(factory);
        this.client = client;
    }

    public Iterator<Long> search(MailboxSession session, Mailbox<Id> mailbox, SearchQuery searchQuery) throws MailboxException {
        Set<Long> uids = new LinkedHashSet<Long>();
        // return uids.iterator();
        throw new NotImplementedException();
    }
    
    private static Calendar getGMT() {
        return Calendar.getInstance(TimeZone.getTimeZone("GMT"), Locale.ENGLISH);
    }

@Override
public void add(final MailboxSession session, Mailbox<Id> mailbox, final Message<Id> message) throws MailboxException {
    try {
        final XContentBuilder  document = jsonBuilder()
                .startObject()
                .field(Field.INTERNAL_DATE, message.getInternalDate())
                .field(Field.ID, message.getUid())
                .field(Field.SIZE, message.getFullContentOctets())
                .field(Field.FLAGS, message.createFlags().getUserFlags())
                .endObject();

        // content handler which will index the headers and the body of the message
        SimpleContentHandler handler = new SimpleContentHandler() {
            private void add(String name, Object value) {
                try {
                    document.field(name, value);
                } catch (IOException e) {
                    // do nothing
                }
            }

            public void headers(Header header) {
                Date sentDate = null;
                String firstFromMailbox = "";
                String firstToMailbox = "";
                String firstCcMailbox = "";
                String firstFromDisplay = "";
                String firstToDisplay = "";

                Iterator<org.apache.james.mime4j.stream.Field> fields = header.iterator();
                while(fields.hasNext()) {
                    org.apache.james.mime4j.stream.Field f = fields.next();
                    String headerName = f.getName().toUpperCase(Locale.ENGLISH);
                    String headerValue = f.getBody().toUpperCase(Locale.ENGLISH);
                    String fullValue =  f.toString().toUpperCase(Locale.ENGLISH);
                    add(Field.HEADERS, fullValue);
                    add(Field.PREFIX_HEADER, headerName);

                    if (f instanceof DateTimeField) {
                        // We need to make sure we convert it to GMT
                        final StringReader reader = new StringReader(f.getBody());
                        try {
                            DateTime dateTime = new DateTimeParser(reader).parseAll();
                            Calendar cal = getGMT();
                            cal.set(dateTime.getYear(), dateTime.getMonth() - 1, dateTime.getDay(), dateTime.getHour(), dateTime.getMinute(), dateTime.getSecond());
                            sentDate =  cal.getTime();

                        } catch (org.apache.james.mime4j.field.datetime.parser.ParseException e) {
                            session.getLog().debug("Unable to parse Date header for proper indexing", e);
                            // This should never happen anyway fallback to the already parsed field
                            sentDate = ((DateTimeField) f).getDate();
                        }

                    }

                    String field = null;
                    if ("To".equalsIgnoreCase(headerName)) {
                        field = Field.TO;
                    } else if ("From".equalsIgnoreCase(headerName)) {
                        field = Field.FROM;
                    } else if ("Cc".equalsIgnoreCase(headerName)) {
                        field = Field.CC;
                    } else if ("Bcc".equalsIgnoreCase(headerName)) {
                        field = Field.BCC;
                    }

                    // Check if we can index the the address in the right manner
                    if (field != null) {
                        // not sure if we really should reparse it. It maybe be better to check just for the right type.
                        // But this impl was easier in the first place
                        AddressList aList = LenientAddressBuilder.DEFAULT.parseAddressList(MimeUtil.unfold(f.getBody()));
                        for (int i = 0; i < aList.size(); i++) {
                            Address address = aList.get(i);
                            if (address instanceof org.apache.james.mime4j.dom.address.Mailbox) {
                                org.apache.james.mime4j.dom.address.Mailbox mailbox = (org.apache.james.mime4j.dom.address.Mailbox) address;
                                String value = AddressFormatter.DEFAULT.encode(mailbox).toUpperCase(Locale.ENGLISH);
                                add(field, value);
                                if (i == 0) {
                                    String mailboxAddress = SearchUtil.getMailboxAddress(mailbox);
                                    String mailboxDisplay = SearchUtil.getDisplayAddress(mailbox);

                                    if ("To".equalsIgnoreCase(headerName)) {
                                        firstToMailbox = mailboxAddress;
                                        firstToDisplay = mailboxDisplay;
                                    } else if ("From".equalsIgnoreCase(headerName)) {
                                        firstFromMailbox = mailboxAddress;
                                        firstFromDisplay = mailboxDisplay;
                                    } else if ("Cc".equalsIgnoreCase(headerName)) {
                                        firstCcMailbox = mailboxAddress;
                                    }

                                }
                            } else if (address instanceof Group) {
                                MailboxList mList = ((Group) address).getMailboxes();
                                for (int a = 0; a < mList.size(); a++) {
                                    org.apache.james.mime4j.dom.address.Mailbox mailbox = mList.get(a);
                                    String value = AddressFormatter.DEFAULT.encode(mailbox).toUpperCase(Locale.ENGLISH);
                                    add(field, value);

                                    if (i == 0 && a == 0) {
                                        String mailboxAddress = SearchUtil.getMailboxAddress(mailbox);
                                        String mailboxDisplay = SearchUtil.getDisplayAddress(mailbox);

                                        if ("To".equalsIgnoreCase(headerName)) {
                                            firstToMailbox = mailboxAddress;
                                            firstToDisplay = mailboxDisplay;
                                        } else if ("From".equalsIgnoreCase(headerName)) {
                                            firstFromMailbox = mailboxAddress;
                                            firstFromDisplay = mailboxDisplay;
                                        } else if ("Cc".equalsIgnoreCase(headerName)) {
                                            firstCcMailbox = mailboxAddress;
                                        }
                                    }
                                }
                            }
                        }
                        add(field, headerValue);
                    } else if (headerName.equalsIgnoreCase("Subject")) {
                        add(Field.BASE_SUBJECT, SearchUtil.getBaseSubject(headerValue));
                    } 
                }
                if (sentDate == null) {
                    sentDate = message.getInternalDate();
                } else {   
                    add(Field.SENT_DATE_YEAR_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.YEAR));
                    add(Field.SENT_DATE_MONTH_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.MONTH));
                    add(Field.SENT_DATE_DAY_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.DAY));
                    add(Field.SENT_DATE_HOUR_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.HOUR));
                    add(Field.SENT_DATE_MINUTE_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.MINUTE));
                    add(Field.SENT_DATE_SECOND_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.SECOND));
                    add(Field.SENT_DATE_MILLISECOND_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.MILLISECOND));
                }
                add(Field.SENT_DATE_SORT_FIELD_MILLISECOND_RESOLUTION, DateTools.dateToString(sentDate, DateTools.Resolution.MILLISECOND));
                add(Field.FIRST_FROM_MAILBOX_NAME, firstFromMailbox);
                add(Field.FIRST_FROM_MAILBOX_DISPLAY, firstFromDisplay);
                add(Field.FIRST_TO_MAILBOX_NAME, firstToMailbox);
                add(Field.FIRST_TO_MAILBOX_DISPLAY, firstToDisplay);
                add(Field.FIRST_CC_MAILBOX_NAME, firstCcMailbox);
            }

            @Override
            public void body(BodyDescriptor desc, InputStream in) throws MimeException, IOException {
                String mediaType = desc.getMediaType();
                if (MEDIA_TYPE_TEXT.equalsIgnoreCase(mediaType) || MEDIA_TYPE_MESSAGE.equalsIgnoreCase(mediaType)) {
                    String cset = desc.getCharset();
                    if (cset == null) {
                        cset = DEFAULT_ENCODING;
                    }
                    Charset charset;
                    try {
                        charset = Charset.forName(cset);
                    } catch (Exception e) {
                        // Invalid charset found so fallback toe the DEFAULT_ENCODING
                        charset = Charset.forName(DEFAULT_ENCODING);
                    }

                    // Read the content one line after the other and add it to the document
                    BufferedReader bodyReader = new BufferedReader(new InputStreamReader(in, charset));
                    String line = null;
                    while((line = bodyReader.readLine()) != null) {
                        add(Field.BODY, line.toUpperCase(Locale.ENGLISH));
                    }
                }
            }    
        };

        MimeConfig config = new MimeConfig();
        config.setMaxLineLen(-1);
        config.setMaxContentLen(-1);
        MimeStreamParser parser = new MimeStreamParser(config);
        parser.setContentDecoding(true);
        parser.setContentHandler(handler);

        try {
            // parse the message to index headers and body
            parser.parse(message.getFullContent());
        } catch (MimeException e) {
            // This should never happen as it was parsed before too without problems.
            throw new MailboxException("Unable to index content of message", e);
        } catch (IOException e) {
            // This should never happen as it was parsed before too without problems.
            // anyway let us just skip the body and headers in the index
            throw new MailboxException("Unable to index content of message", e);
        }

        client.prepareIndex(INDEX_NAME, TYPE_NAME, message.getUid() + "")
        .setSource(document)
        .execute()
        .actionGet();
    } catch (IOException e) {
        throw new MailboxException();
    }
}

    @Override
    public void delete(MailboxSession session, Mailbox<Id> mailbox, MessageRange range) throws MailboxException {
        final MessageRange.Type type = range.getType();
        RangeQueryBuilder query = null;
        switch (type) {
            case ALL:
                client.admin().indices().prepareDeleteMapping(INDEX_NAME).setType(TYPE_NAME)
                    .execute()
                    .actionGet();
                break;
            case FROM:
                query = new RangeQueryBuilder(Field.ID).gte(range.getUidFrom());
                client.prepareDeleteByQuery(INDEX_NAME).setTypes(TYPE_NAME)
                    .setQuery(query)
                    .execute()
                    .actionGet();
                break;
            case RANGE:
                query = new RangeQueryBuilder(Field.ID).to(range.getUidTo()).from(range.getUidFrom());
                client.prepareDeleteByQuery(INDEX_NAME).setTypes(TYPE_NAME)
                    .setQuery(query)
                    .execute()
                    .actionGet();
                break;
            case ONE:
                client.prepareDelete(INDEX_NAME, TYPE_NAME, range.getUidTo() + "")
                    .execute()
                    .actionGet();
                break;
        }
    }

    @Override
    public void update(MailboxSession session, Mailbox<Id> mailbox, MessageRange range, Flags flags) throws MailboxException {
        QueryBuilder query;
        throw new NotImplementedException();
    }

}
