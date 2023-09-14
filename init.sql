CREATE EXTENSION IF NOT EXISTS moreint;
INSERT INTO log.seen_click_fav_sum (seen, click, fav) SELECT 240, 3, 1 WHERE NOT EXISTS (SELECT 1 FROM log.seen_click_fav_sum);
