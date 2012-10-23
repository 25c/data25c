step(
  """
CREATE TABLE titles (
    url character varying(255) PRIMARY KEY,
    title character varying(255),
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
""",
  "DROP TABLE titles;",
)
