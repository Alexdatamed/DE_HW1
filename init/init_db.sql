-- 1. Таблиця рекламодавців
CREATE TABLE advertisers (
    advertiser_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100) UNIQUE
);

-- 2. Таблиця кампаній
CREATE TABLE campaigns (
    campaign_id INT AUTO_INCREMENT PRIMARY KEY,
    name VARCHAR(100),
    advertiser_id INT,
    start_date DATE,
    end_date DATE,
    targeting_criteria TEXT,
    ad_slot_size VARCHAR(20),
    budget FLOAT,
    remaining_budget FLOAT,
    FOREIGN KEY (advertiser_id) REFERENCES advertisers(advertiser_id)
);

-- 3. Таблиця користувачів
CREATE TABLE users (
    user_id BIGINT PRIMARY KEY,
    age INT,
    gender VARCHAR(20),
    location VARCHAR(100),
    signup_date DATE
);

-- 4. Таблиця інтересів користувачів (many-to-many)
CREATE TABLE user_interests (
    user_id BIGINT,
    interest VARCHAR(50),
    PRIMARY KEY (user_id, interest),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 5. Таблиця показів реклами (ad events)
CREATE TABLE ad_events (
    event_id CHAR(36) PRIMARY KEY,
    campaign_id INT,
    user_id BIGINT,
    timestamp DATETIME,
    ad_slot_size VARCHAR(20),
    device VARCHAR(20),
    location VARCHAR(100),
    bid_amount FLOAT,
    ad_cost FLOAT,
    FOREIGN KEY (campaign_id) REFERENCES campaigns(campaign_id),
    FOREIGN KEY (user_id) REFERENCES users(user_id)
);

-- 6. Таблиця кліків (лише якщо був клік)
CREATE TABLE clicks (
    event_id CHAR(36) PRIMARY KEY,
    click_timestamp DATETIME,
    ad_revenue FLOAT,
    FOREIGN KEY (event_id) REFERENCES ad_events(event_id)
);