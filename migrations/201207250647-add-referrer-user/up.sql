ALTER TABLE clicks ADD COLUMN referrer_user_id integer;

CREATE INDEX index_clicks_on_referrer_user_id ON clicks (referrer_user_id);
