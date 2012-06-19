DELETE FROM clicks;

ALTER TABLE clicks DROP COLUMN uuid;
ALTER TABLE clicks DROP COLUMN button_id;

ALTER TABLE clicks ADD COLUMN publisher_user_id integer NOT NULL;
ALTER TABLE clicks ADD COLUMN content_id integer;

CREATE INDEX index_clicks_on_publisher_user_id ON clicks (publisher_user_id, content_id);
