step(
  """
ALTER TABLE clicks ALTER amount TYPE BIGINT USING amount::BIGINT*1000000;
ALTER TABLE clicks ADD parent_click_id BIGINT;
ALTER TABLE clicks ADD receiver_user_id INTEGER;
CREATE UNIQUE INDEX index_clicks_on_parent_and_receiver ON clicks (parent_click_id, receiver_user_id);
CREATE INDEX index_clicks_on_receiver_user_id ON clicks (receiver_user_id);
""",
  """
ALTER TABLE clicks ALTER amount TYPE INTEGER USING CAST(amount/1000000 AS INTEGER);
ALTER TABLE clicks DROP parent_click_id;
ALTER TABLE clicks DROP receiver_user_id;
"""
)
