DELETE FROM clicks;

ALTER TABLE clicks DROP COLUMN publisher_user_id;
ALTER TABLE clicks DROP COLUMN content_id;

ALTER TABLE clicks ADD COLUMN uuid character varying(255) NOT NULL;
ALTER TABLE clicks ADD COLUMN button_id integer NOT NULL;

CREATE INDEX index_clicks_on_button_id ON clicks (button_id);
CREATE UNIQUE INDEX index_clicks_on_uuid ON clicks (lower(uuid));
