package org.apache.james.mailbox.store.search;

import java.util.Date;

import javax.mail.Flags;
import javax.mail.internet.SharedInputStream;
import javax.mail.util.SharedByteArrayInputStream;

import org.apache.james.mailbox.store.mail.model.impl.PropertyBuilder;
import org.apache.james.mailbox.store.mail.model.impl.SimpleMessage;

public class SimpleMessageMock extends SimpleMessage<Integer> {
    private final static String date = "Date: Mon, 7 Feb 1994 21:52:25 -0800 (PST)\n";
    private static String messageTemplate = date + "From: Fred Foobar <foobar@Blurdybloop.COM>\n" + "Subject: Test 02\n" 
            + "To: mooch@owatagu.siam.edu\n" + "Message-Id: <B27397-0100000@Blurdybloop.COM>\n" + "MIME-Version: 1.0\n"
            + "Content-Type: TEXT/PLAIN; CHARSET=US-ASCII\n" + "\n" + "Test\n" + "\n.";
    SharedInputStream content = new SharedByteArrayInputStream(messageTemplate.getBytes());
    PropertyBuilder propBuilder = new PropertyBuilder();
    private final static int fakeMailboxId = 42;
    
    private static PropertyBuilder getPropBuilder() {
        PropertyBuilder propBuilder = new PropertyBuilder();
        propBuilder.setProperty("gsoc", "prop", "value");
        propBuilder.setMediaType("text");
        propBuilder.setSubType("html");
        propBuilder.setTextualLineCount(2L);
        return propBuilder;
    }
    
    public SimpleMessageMock(int id) {
        this(new Date(), messageTemplate.getBytes().length, messageTemplate.getBytes().length - 20, 
                new SharedByteArrayInputStream(messageTemplate.getBytes()), new Flags(Flags.Flag.RECENT), getPropBuilder(), fakeMailboxId);
        this.setUid(id);
    }
    
    public SimpleMessageMock(Date internalDate, int size, int bodyStartOctet, SharedInputStream content, Flags flags, PropertyBuilder propertyBuilder, Integer mailboxId) {
        super(internalDate, size, bodyStartOctet, content, flags, propertyBuilder, mailboxId);
    }

}
