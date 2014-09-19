package org.apache.james.mailbox.store.search;

import java.util.Random;

import org.apache.james.mailbox.model.MailboxACL;
import org.apache.james.mailbox.model.SimpleMailboxACL;
import org.apache.james.mailbox.store.mail.model.Mailbox;

public final class SimpleMailboxMock implements Mailbox<Long> {
    private long id;

    public SimpleMailboxMock() {
        this.id = new Random().nextLong();
    }
    
    public SimpleMailboxMock(long id) {
        this.id = id;
    }

    public Long getMailboxId() {
        return id;
    }

    public String getNamespace() {
        throw new UnsupportedOperationException("Not supported");
    }

    public void setNamespace(String namespace) {
        throw new UnsupportedOperationException("Not supported");
    }

    public String getUser() {
        throw new UnsupportedOperationException("Not supported");
    }

    public void setUser(String user) {
        throw new UnsupportedOperationException("Not supported");
    }

    public String getName() {
        return Long.toString(id);
    }

    public void setName(String name) {
        throw new UnsupportedOperationException("Not supported");

    }

    public long getUidValidity() {
        return 0;
    }

    /* (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.model.Mailbox#getACL()
     */
    @Override
    public MailboxACL getACL() {
        return SimpleMailboxACL.OWNER_FULL_ACL;
    }

    /* (non-Javadoc)
     * @see org.apache.james.mailbox.store.mail.model.Mailbox#setACL(org.apache.james.mailbox.MailboxACL)
     */
    @Override
    public void setACL(MailboxACL acl) {
        throw new UnsupportedOperationException("Not supported");
    }
}

