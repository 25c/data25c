step(
  """
CREATE TABLE clicks (
    id bigserial PRIMARY KEY,
    uuid character varying(255) NOT NULL,
    user_id integer NOT NULL,
    referrer_user_id integer,
    button_id integer NOT NULL,
    ip_address character varying(255),
    user_agent text,
    referrer text,
		state integer NOT NULL DEFAULT 0,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
CREATE INDEX index_clicks_on_user_id ON clicks USING btree (user_id);
CREATE INDEX index_clicks_on_button_id ON clicks (button_id);
CREATE UNIQUE INDEX index_clicks_on_uuid ON clicks (lower(uuid));
CREATE INDEX index_clicks_on_referrer_user_id ON clicks (referrer_user_id);
""",
  "DROP TABLE clicks;",
  ignore_errors='apply'
)
