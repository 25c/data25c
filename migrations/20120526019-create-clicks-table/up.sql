CREATE TABLE clicks (
    id bigserial PRIMARY KEY,
    user_id integer NOT NULL,
    publisher_user_id integer NOT NULL,
		content_id integer,
    ip_address character varying(255),
    user_agent text,
    referrer text,
		state integer NOT NULL DEFAULT 0,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
CREATE INDEX index_clicks_on_publisher_user_id ON clicks USING btree (publisher_user_id);
CREATE INDEX index_clicks_on_user_id ON clicks USING btree (user_id);
