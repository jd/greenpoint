sql:
	psql greenpoint < sql/tables.sql
	pgloader sql/exchanges.pgloader

clean-sql:
	psql greenpoint < sql/delete.sql

.PHONY: sql clean-sql
