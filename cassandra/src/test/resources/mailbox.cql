CREATE TABLE mailbox (
	id UUID PRIMARY KEY,
	name text, namespace text,
	uidvalidity bigint,
	user text,
	path text
);
CREATE INDEX ON mailbox(path);

CREATE TABLE mailboxCounters (
	mailboxId UUID PRIMARY KEY,
	count counter, 
	unseen counter,
	nextUid counter,
	nextModSeq counter
);

CREATE TABLE message (
	mailboxId UUID, 
	uid bigint,
	internalDate timestamp, 
	bodyStartOctet int,
	content blob,
	modSeq bigint,
	mediaType text,
	subType text,
	fullContentOctets int,
	bodyOctets int,
	textualLineCount int,
	bodyContent blob,
	headerContent blob,
	flagAnswered boolean,
	flagDeleted boolean,
	flagDraft boolean,
	flagRecent boolean,
	flagSeen boolean,
	flagFlagged boolean,
	flagUser boolean,
	PRIMARY KEY (mailboxId, uid)
	);
