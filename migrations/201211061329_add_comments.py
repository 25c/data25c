step(
  """
DROP INDEX index_urls_on_uuid;
CREATE UNIQUE INDEX index_urls_on_uuid ON urls (uuid);
  
CREATE TABLE comments (
    id bigserial PRIMARY KEY,
    uuid character varying(255) NOT NULL,
    user_id INTEGER NOT NULL,
    button_id INTEGER NOT NULL,
    url_id BIGINT NOT NULL,
    content text NOT NULL,
    total_amount BIGINT NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
CREATE UNIQUE INDEX index_comments_on_uuid ON comments (uuid);
CREATE INDEX index_comments_on_user_id_and_button_id_and_url_id_and_total_amount ON comments (user_id, button_id, url_id, total_amount);

ALTER TABLE clicks ADD comment_id BIGINT;
CREATE INDEX index_clicks_on_comment_id ON clicks (comment_id);
""",
  """
DROP TABLE comments;
ALTER TABLE clicks DROP comment_id;
"""
)
