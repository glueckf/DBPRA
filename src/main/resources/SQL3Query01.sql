-- Task 1:
--
-- Points:
-- 0.25
--
CREATE TABLE CustomerContactData
(
    CustKey INT PRIMARY KEY NOT NULL,
    TwitterId VARCHAR(35), -- Twitter ID of the customer
    GoogleId BIGINT,
    FacebookId BIGINT,
    InstagramId VARCHAR(26),
    GithubId VARCHAR(34),
    TelegramName VARCHAR(38),
    ZipCode VARCHAR(5),
    Phone VARCHAR(40)
)