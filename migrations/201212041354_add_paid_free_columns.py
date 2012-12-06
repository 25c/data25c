step(
  """
ALTER TABLE clicks ALTER amount TYPE NUMERIC USING (amount/1000000)::INTEGER;
ALTER TABLE clicks ALTER amount DROP DEFAULT;
ALTER TABLE clicks ADD amount_free NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE clicks ALTER amount_free DROP DEFAULT;
ALTER TABLE clicks ADD amount_paid NUMERIC NOT NULL DEFAULT 0;
ALTER TABLE clicks ALTER amount_paid DROP DEFAULT;
""",
  """
ALTER TABLE clicks ALTER amount TYPE BIGINT USING amount::BIGINT*1000000;
ALTER TABLE clicks DROP amount_free;
ALTER TABLE clicks DROP amount_paid;
"""
)
