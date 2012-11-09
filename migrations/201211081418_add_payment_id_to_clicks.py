step(
  "ALTER TABLE clicks ADD payment_id INTEGER; CREATE INDEX index_clicks_on_payment_id ON clicks (payment_id);",
  "ALTER TABLE clicks DROP payment_id;"
)
