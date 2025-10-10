CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS "user" (
    id UUID PRIMARY KEY,
    email TEXT NOT NULL,
    roles TEXT, -- edn
    joined_at TIMESTAMP,
    digest_days TEXT, -- edn
    send_digest_at TIMESTAMP,
    timezone TEXT,
    digest_last_sent TIMESTAMP,
    from_the_sample BOOLEAN,
    use_original_links BOOLEAN,
    suppressed_at TIMESTAMP,
    email_username TEXT,
    customer_id TEXT,
    plan TEXT,
    cancel_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS sub (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    created_at TIMESTAMP NOT NULL,
    pinned_at TIMESTAMP,
    kind TEXT NOT NULL, -- 'feed' or 'email'
    -- Feed-specific:
    feed_id UUID,
    -- Email-specific:
    email_from TEXT,
    unsubscribed_at TIMESTAMP
);

CREATE TABLE IF NOT EXISTS item (
    id UUID PRIMARY KEY,
    ingested_at TIMESTAMP NOT NULL,
    title TEXT,
    url TEXT,
    redirect_urls TEXT, -- edn
    content TEXT,
    content_key UUID,
    published_at TIMESTAMP,
    excerpt TEXT,
    author_name TEXT,
    author_url TEXT,
    feed_url TEXT,
    lang TEXT,
    site_name TEXT,
    byline TEXT,
    length INTEGER,
    image_url TEXT,
    paywalled BOOLEAN,
    kind TEXT NOT NULL, -- 'feed', 'email', 'direct'
    -- Feed-specific:
    feed_id UUID,
    guid TEXT,
    -- Email-specific:
    email_sub_id UUID,
    raw_content_key UUID,
    list_unsubscribe TEXT,
    list_unsubscribe_post TEXT,
    reply_to TEXT,
    maybe_confirmation BOOLEAN,
    -- Direct-specific:
    candidate_status TEXT
);

CREATE TABLE IF NOT EXISTS feed (
    id UUID PRIMARY KEY,
    url TEXT NOT NULL,
    synced_at TIMESTAMP,
    title TEXT,
    description TEXT,
    image_url TEXT,
    etag TEXT,
    last_modified TEXT,
    failed_syncs INTEGER,
    moderation TEXT
);

CREATE TABLE IF NOT EXISTS user_item (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    item_id UUID NOT NULL,
    viewed_at TIMESTAMP,
    skipped_at TIMESTAMP,
    bookmarked_at TIMESTAMP,
    favorited_at TIMESTAMP,
    disliked_at TIMESTAMP,
    reported_at TIMESTAMP,
    report_reason TEXT
);

CREATE TABLE IF NOT EXISTS ad (
    id UUID PRIMARY KEY,
    "user" UUID NOT NULL,
    approve_state TEXT NOT NULL,
    updated_at TIMESTAMP NOT NULL,
    balance INTEGER NOT NULL,
    recent_cost INTEGER,
    bid INTEGER,
    budget INTEGER,
    url TEXT,
    title TEXT,
    description TEXT,
    image_url TEXT,
    paused BOOLEAN,
    payment_failed BOOLEAN,
    customer_id TEXT,
    session_id TEXT,
    payment_method TEXT,
    card_details TEXT -- edn
);

CREATE TABLE IF NOT EXISTS ad_click (
    id UUID PRIMARY KEY,
    user_id UUID NOT NULL,
    ad_id UUID NOT NULL,
    created_at TIMESTAMP NOT NULL,
    cost INTEGER NOT NULL,
    source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ad_credit (
    id UUID PRIMARY KEY,
    ad_id UUID NOT NULL,
    source TEXT NOT NULL,
    amount INTEGER NOT NULL,
    created_at TIMESTAMP NOT NULL,
    charge_status TEXT
);

CREATE INDEX IF NOT EXISTS idx_user_email ON "user"(email);
CREATE INDEX IF NOT EXISTS idx_sub_user_id ON sub(user_id);
CREATE INDEX IF NOT EXISTS idx_sub_feed_id ON sub(feed_id);
CREATE INDEX IF NOT EXISTS idx_item_feed_id ON item(feed_id);
