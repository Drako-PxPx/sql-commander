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
## Lua Scripting Runtime Behavior

### Isolated Execution Environment
To ensure script reliability and prevent side effects, every execution of a Lua script (via the `RUN` command) must operate within a **clean symbol table**.
- **Memory Isolation:** Variables, functions, and tables defined in one execution must not persist or be accessible in subsequent executions, even if it is the same script being run multiple times.
- **Orchestrator Integrity:** This isolation is critical for the `db_` pattern orchestrator. The `__newindex` metamethod used to inject the `go()` method only triggers when a key is newly created in the global scope. A dirty symbol table prevents this mechanism from working on script re-runs.
- **Future-proofing:** While a future "include" feature may allow sharing definitions via specific library loading mechanisms, the default state for any script entry point remains a fully isolated environment.

### Runtime Variables
The tool provides a global `runtime` table to assist with environmental awareness:
- `runtime.cwd`: Contains the absolute path to the directory of the currently executing script. This should be used for relative file operations (e.g., `io.open(runtime.cwd .. "/data.csv")`) instead of relying on the process's current working directory.
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
