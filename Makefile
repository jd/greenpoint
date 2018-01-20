sql:
	psql < sql/tables.sql

clean-sql:
	psql < sql/delete.sql

.PHONY: sql clean-sql
