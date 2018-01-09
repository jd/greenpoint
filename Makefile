sql:
	psql greenpoint < sql/tables.sql
	pgloader sql/exchanges.pgloader

.PHONY: sql
