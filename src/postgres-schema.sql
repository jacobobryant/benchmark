CREATE TABLE IF NOT EXISTS user (
    id INTEGER PRIMARY KEY,
    email TEXT NOT NULL,
    roles TEXT, -- edn
    joined_at INTEGER,
    digest_days TEXT, -- edn
    send_digest_at TEXT,
    timezone TEXT,
    digest_last_sent INTEGER,
    from_the_sample INTEGER,
    use_original_links INTEGER,
    suppressed_at INTEGER,
    email_username TEXT,
    customer_id TEXT,
    plan TEXT,
    cancel_at INTEGER
);

CREATE TABLE IF NOT EXISTS sub (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    pinned_at INTEGER,
    kind TEXT NOT NULL, -- 'feed' or 'email'
    -- Feed-specific:
    feed_id INTEGER,
    -- Email-specific:
    email_from TEXT,
    unsubscribed_at INTEGER
);

CREATE TABLE IF NOT EXISTS item (
    id INTEGER PRIMARY KEY,
    ingested_at INTEGER NOT NULL,
    title TEXT,
    url TEXT,
    redirect_urls TEXT, -- edn
    content TEXT,
    content_key BLOB,
    published_at INTEGER,
    excerpt TEXT,
    author_name TEXT,
    author_url TEXT,
    feed_url TEXT,
    lang TEXT,
    site_name TEXT,
    byline TEXT,
    length INTEGER,
    image_url TEXT,
    paywalled INTEGER,
    kind TEXT NOT NULL, -- 'feed', 'email', 'direct'
    -- Feed-specific:
    feed_id INTEGER,
    guid TEXT,
    -- Email-specific:
    email_sub_id INTEGER,
    raw_content_key BLOB,
    list_unsubscribe TEXT,
    list_unsubscribe_post TEXT,
    reply_to TEXT,
    maybe_confirmation INTEGER,
    -- Direct-specific:
    candidate_status TEXT
);

CREATE TABLE IF NOT EXISTS feed (
    id INTEGER PRIMARY KEY,
    url TEXT NOT NULL,
    synced_at INTEGER,
    title TEXT,
    description TEXT,
    image_url TEXT,
    etag TEXT,
    last_modified TEXT,
    failed_syncs INTEGER,
    moderation TEXT
);

CREATE TABLE IF NOT EXISTS user_item (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    item_id INTEGER NOT NULL,
    viewed_at INTEGER,
    skipped_at INTEGER,
    bookmarked_at INTEGER,
    favorited_at INTEGER,
    disliked_at INTEGER,
    reported_at INTEGER,
    report_reason TEXT
);

CREATE TABLE IF NOT EXISTS ad (
    id INTEGER PRIMARY KEY,
    user INTEGER NOT NULL,
    approve_state TEXT NOT NULL,
    updated_at INTEGER NOT NULL,
    balance INTEGER NOT NULL,
    recent_cost INTEGER,
    bid INTEGER,
    budget INTEGER,
    url TEXT,
    title TEXT,
    description TEXT,
    image_url TEXT,
    paused INTEGER,
    payment_failed INTEGER,
    customer_id TEXT,
    session_id TEXT,
    payment_method TEXT,
    card_details TEXT -- edn
);

CREATE TABLE IF NOT EXISTS ad_click (
    id INTEGER PRIMARY KEY,
    user_id INTEGER NOT NULL,
    ad_id INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    cost INTEGER NOT NULL,
    source TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS ad_credit (
    id INTEGER PRIMARY KEY,
    ad_id INTEGER NOT NULL,
    source TEXT NOT NULL,
    amount INTEGER NOT NULL,
    created_at INTEGER NOT NULL,
    charge_status TEXT
);

CREATE INDEX IF NOT EXISTS idx_user_email ON user(email);

CREATE INDEX IF NOT EXISTS idx_sub_user_id ON sub(user_id);
CREATE INDEX IF NOT EXISTS idx_sub_feed_id ON sub(feed_id);

CREATE INDEX IF NOT EXISTS idx_item_feed_id ON item(feed_id);
