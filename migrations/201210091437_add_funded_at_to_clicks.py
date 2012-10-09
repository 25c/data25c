step(
  "ALTER TABLE clicks ADD COLUMN funded_at TIMESTAMP WITHOUT TIME ZONE; UPDATE clicks SET funded_at=updated_at WHERE state IN (2,3,4);",
  "ALTER TABLE clicks DROP COLUMN funded_at;"
)
