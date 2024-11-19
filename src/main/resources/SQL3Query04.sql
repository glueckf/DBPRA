-- Task 4:
--
-- Points:
-- 0.5
--
ALTER TABLE CustomerContactData
    ADD CONSTRAINT AtLeastOne_CONSTRAINT
    CHECK(
    TwitterId IS NOT NULL
    OR GoogleId IS NOT NULL
    OR FacebookId IS NOT NULL
    OR InstagramId IS NOT NULL
    OR GithubId IS NOT NULL
    OR TelegramName IS NOT NULL
    OR ZipCode IS NOT NULL
    OR Phone IS NOT NULL
        );