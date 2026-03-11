# Project: SQL Commander

## Goal
A SQL command-line tool for Oracle and PostgreSQL (and other SQL RDBMS in the future) written in Python. It is capable of running Lua scripts to provide vendor- and version-agnostic system maintenance, with the capability to extend functionality based on the target RDBMS.

While this tool accepts standard SQL commands, its primary purpose is to create, edit, and extend Lua scripts. The Lua implementation must follow the current language standard (Lua 5.4).

## Technical Requirements
- **Runtime:** Python 3.10+
- **Lua Bridge:** `lupa` (recommended for LuaJIT performance and Python integration)
- **CLI Framework:** `cmd2`
- **Output Formatting:** `prettytable` (for displaying SQL query results)
- **Database Drivers:**
  - `oracledb` (Thick mode support)
  - `psycopg` (Version 3+)
- **Project manager:** poetry

## directory structure
- src/ - the sourcecode of the project, structured as a python project

## Connection & Security
The database connection is performed using the CLI command `CONNECT`.

### Syntax
`CONNECT <connection_string>`

### Authentication Strategy
To ensure security and avoid password handling within the tool:
- **Oracle:** 
  - The `<connection_string>` must be a **TNS Alias**.
  - **Requirement:** A configured External Password Store (EPS) Wallet and a valid `sqlnet.ora` profile.
  - **Environment:** `TNS_ADMIN` should point to the directory containing `tnsnames.ora` and `sqlnet.ora`.
- **PostgreSQL:** 
  - The `<connection_string>` must be a **Service Name**.
  - **Requirement:** A service entry in `pg_service.conf` and credentials stored in `pgpass.conf`.
  - **Environment:** `PGSERVICEFILE` and `PGPASSFILE` (or default OS locations) must be correctly configured.
