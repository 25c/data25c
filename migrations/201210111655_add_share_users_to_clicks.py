step(
  """
ALTER TABLE clicks ADD share_users VARCHAR(255);
DROP INDEX index_clicks_on_user_id;
CREATE INDEX index_clicks_on_user_id_and_parent_click_id ON clicks USING btree (user_id, parent_click_id);
""",
  """
ALTER TABLE clicks DROP share_users;
CREATE INDEX index_clicks_on_user_id ON clicks USING btree (user_id);
DROP INDEX index_clicks_on_user_id_and_parent_click_id;
"""
)
