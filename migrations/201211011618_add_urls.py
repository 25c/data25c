step(
  """
CREATE EXTENSION "uuid-ossp";
CREATE TABLE urls (
    id bigserial PRIMARY KEY,
    uuid character varying(255) NOT NULL,
    url character varying(255) NOT NULL,
    title character varying(255),
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
CREATE UNIQUE INDEX index_urls_on_uuid ON urls (url);
CREATE UNIQUE INDEX index_urls_on_url ON urls (url);

INSERT INTO urls (uuid, url, title, created_at, updated_at) SELECT REPLACE(uuid_generate_v4()::TEXT, '-', ''), url, title, created_at, updated_at FROM titles;
DROP TABLE titles;

ALTER TABLE clicks ADD url_id BIGINT;
DROP INDEX index_clicks_on_button_id;
CREATE INDEX index_clicks_on_button_id_and_url_id ON clicks (button_id, url_id);
""",
  """
DROP EXTENSION "uuid-ossp";
DROP TABLE urls;
CREATE TABLE titles (
    url character varying(255) PRIMARY KEY,
    title character varying(255),
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
ALTER TABLE clicks DROP url_id;
CREATE INDEX index_clicks_on_button_id ON clicks (button_id);
"""
)
