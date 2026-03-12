# Scripting Model

This document specifies the integration between the Lua scripting environment and the database management system (DBMS).

## 1. SQL Integration in Lua

A custom `SQL` instruction must be implemented within the Lua scripting context to execute database commands.

### Syntax
`SQL <SQL_STATEMENT> [SAVE OUTPUT IN :<variable_name>]`

### Variable Substitution Rules
The tool uses a pre-processor to replace Lua variables (prefixed with `$` or `&`) into the SQL statement before execution.

- **Scalar Values (`$var`):** Numbers, strings, and booleans are substituted using standard SQL literals or bind variables. These are used for values in `WHERE`, `VALUES`, `SET`, etc.
- **Literal Substitution (`&var`):** Injects the variable value directly into the SQL statement without binding. This is essential for identifiers like schema names, table names, or column names (e.g., `GRANT SELECT ON &owner.&table TO role`).
- **Table/Array Expansion (SQL `IN` Support):** If a Lua table is substituted (e.g., `$my_list`), the pre-processor automatically expands it into a comma-separated list enclosed in parentheses.
  - **Example:** If `roles = {"admin", "user"}`, the statement `SELECT * FROM <USERS> WHERE role IN $roles` becomes `SELECT * FROM DBA_USERS WHERE role IN ('admin', 'user')`.
- **Global and Local Scope:** The pre-processor searches both local variables and the Lua global table (`_G`) for the variable name following the `$` or `&`.

---

## 2. RDBMS-Specific Logic (The `db_` Dispatch Pattern)

To implement vendor-specific operations, use the `db_` class pattern. This enables unified scripts to behave differently based on the active connection.

### Dispatcher Logic
- **Naming:** All specific logic classes must start with the `db_` prefix.
- **Methods:** Each class must define a method named after the RDBMS (e.g., `:ORACLE`, `:POSTGRESQL`).
- **Fallback (`any`):** If a method for the active vendor is not found, the orchestrator looks for a method named `any` or `ANY`. This is useful for generic SQL that works across all supported databases.
- **Internal State:** Each method must set a `self.command` string attribute containing the actual SQL to be executed.
- **Execution (`go()`):** The tool's Lua runtime provides a non-declared function `go(...)` as an orchestrator:
  1. It detects the active DBMS type (e.g., `ORACLE`).
  2. It invokes the matching method (e.g., `:ORACLE` or fallback `:any`) passing any provided arguments.
  3. It automatically executes the resulting `self.command`.

### sql_exists() function
Used to check if a SQL statement returns any rows.
- **Return:** `true` if the SQL statement returns any rows, `false` otherwise.
- **Error:** Script execution halts if the RDBMS returns an error.
- **Example:** `sql_exists("SELECT * FROM <USERS> WHERE username = $username")`

### rdbms() function
Used for "Side-effect" SQL commands (e.g., `ALTER SESSION`, `EXPLAIN PLAN`, `BEGIN...END`).
- **Return:** None.
- **Error:** Script execution halts if the RDBMS returns an error.
- **Example:** `rdbms("BEGIN GATHER_STATS('HR'); END;")`

### Agnostic Cursors (SQL Macro Syntax)
Agnostic cursors use `[name(args)]` within a `SQL` instruction. The engine uses a **Discovery Rule** to locate and execute the correct logic at runtime.

#### Discovery Rule
1. When the engine encounters `[view_name(args)]` in a `SQL` statement:
2. It scans all Lua tables starting with `db_` in the global scope.
3. It searches for a method named `vw_<view_name>_<active_vendor>` (e.g., `vw_usage_oracle`).
4. If not found, it searches for `vw_<view_name>_any` or `vw_<view_name>_ANY`.
5. It executes that method, passing the provided arguments.
6. The method is expected to set `self.command`.
7. The engine replaces the `[...]` block with the value of `self.command` before executing the full query.

### Examples

#### Basic Dispatch
```lua
db_CreateUser = {command = ""}

function db_CreateUser:ORACLE(user)
  self.command = string.format("CREATE USER %s IDENTIFIED EXTERNALLY", user)
end

function db_CreateUser:POSTGRESQL(user)
  self.command = string.format("CREATE USER %s", user)
end

db_CreateUser:go("nicko")
```

#### Agnostic View with Procedural Pre-step
```lua
db_Performance = {command = ""}

function db_Performance:vw_execplan_oracle(sqlstmt)
  -- Side effect: populate plan_table
  rdbms(string.format("explain plan for %s", sqlstmt))
  self.command = "SELECT plan_table_output as plan FROM TABLE(DBMS_XPLAN.DISPLAY)"
end

function db_Performance:vw_execplan_postgresql(sqlstmt)
  self.command = "EXPLAIN " .. sqlstmt
end

-- Agnostic usage in SQL
SQL SELECT * FROM [execplan("select * from dual")]
```

---

## 3. Pseudo System Views

The tool provides vendor-agnostic view names (enclosed in `< >`) that are mapped to actual system views at execution time.

| Pseudo View | Description | Oracle Mapping | PostgreSQL Mapping |
| :--- | :--- | :--- | :--- |
| `<USERS>` | Database Users/Roles | `DBA_USERS` | `PG_ROLES` |
| `<TABLES>` | Database Tables | `DBA_TABLES` | `PG_TABLES` |
| `<ROLES>` | Database Roles | `DBA_ROLES` | `PG_ROLES` |

### Column Mapping Specification
| Pseudo View | Data Type | Unified Column | Oracle Column or expression | PostgreSQL Column or expression |
| :--- | :--- | :--- | :--- |
| `<USERS>` | String | `username` | `USERNAME` | `ROLNAME` |
| `<USERS>` | boolean | `can_login` | `ACCOUNT_STATUS IN ('OPEN','EXPIRED(GRACE)')` | `rolcanlogin` |
| `<TABLES>` | String | `owner` | `OWNER` | `schemaname` |
| `<TABLES>` | String | `table_name` | `TABLE_NAME` | `tablename` |
| `<ROLES>` | String | `role` | `ROLE` | `rolname` |


---

## 4. Cursor Handling

When a `SELECT` statement is executed, the results must be mapped into a Lua table structure for iteration and processing:
- Rows should be indexed by number (array part).
- Columns should be accessible by name within each row (dictionary part).

Example access: `print(result[1].username)`

## 5. runtime variables

The `runtime` table is a global table that is populated with the runtime variables that are set in the Lua script.

### Available runtime variables

| Variable Name | Description |
| :--- | :--- |
| cwd | current working dir, it should be the same as the current running script |