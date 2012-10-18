step(
  """
ALTER TABLE clicks ADD fb_action_id VARCHAR(255);
""",
  """
ALTER TABLE clicks DROP fb_action_id;
"""
)
