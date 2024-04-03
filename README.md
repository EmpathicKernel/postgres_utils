# Postgres_Utils

Set of Utilities created to make administration of Postgres Easier

## Analyze

Created to analyze Postgres tables across all databases within a Postgres Cluster. Specifically designed for RDS although should be usable on any PostgreSQL installation.

Once connected, all databases (aside from excluded) within the Postgres cluster will be connected to and all tables analyzed. Tables are analyzed in parallel on each database. Databases are connected to in series.

INFO logging provides updates on every 100 tables analyzed.

[Password file](https://www.postgresql.org/docs/current/libpq-pgpass.html) required!

Usage:

```bash
python3 ./analyze/main.py -d <DB-to-connect-to> --username <Username> --host <DB-Host> -p <#-of-parallel-processes>
```

## Extension_Update

Created to update extensions on all tables across all databases within a Postgres Cluster.

**Please understand what you are doing before running this!**

[Password file](https://www.postgresql.org/docs/current/libpq-pgpass.html) required!

Usage:

```bash
python3 ./extension_update/main.py -d <DB-to-connect-to> --username <Username> --host <DB-Host>
```
