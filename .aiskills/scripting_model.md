# Scripting Model

This document specifies the integration between the Lua scripting environment and the database management system (DBMS).

## 1. SQL Integration in Lua

A custom `SQL` instruction must be implemented within the Lua scripting context to execute database commands.

### Syntax
`SQL <SQL_STATEMENT> [SAVE OUTPUT IN :<variable_name>]`

### Variable Substitution Rules
The tool uses a pre-processor to replace Lua variables (prefixed with `$`) into the SQL statement before execution.

- **Scalar Values:** Numbers, strings, and booleans are substituted using standard SQL literals or bind variables.
- **Table/Array Expansion (SQL `IN` Support):** If a Lua table is substituted (e.g., `$my_list`), the pre-processor automatically expands it into a comma-separated list enclosed in parentheses.
  - **Example:** If `roles = {"admin", "user"}`, the statement `SELECT * FROM <USERS> WHERE role IN $roles` becomes `SELECT * FROM DBA_USERS WHERE role IN ('admin', 'user')`.
- **Global Scope:** The pre-processor searches the Lua global table (`_G`) for the variable name following the `$`.

---

## 2. RDBMS-Specific Logic (The `db_` Dispatch Pattern)

To implement vendor-specific operations, use the `db_` class pattern. This enables unified scripts to behave differently based on the active connection.

### Dispatcher Logic
- **Naming:** All specific logic classes must start with the `db_` prefix.
- **Methods:** Each class must define a method named after the RDBMS (e.g., `:ORACLE`, `:POSTGRESQL`).
- **Internal State:** Each method must set a `self.command` string attribute containing the actual SQL to be executed.
- **Execution (`go()`):** The tool's Lua runtime provides a non-declared function `go()` that acts as an orchestrator. When `db_Example:go(...)` is called:
  1. It detects the active DBMS type.
  2. It invokes the matching method (e.g., `:ORACLE`) passing any provided arguments.
  3. It automatically executes the resulting `self.command`.

### Example
```lua
db_CreateUser = {command = ""}

function db_CreateUser:ORACLE(user)
  self.command = string.format("CREATE USER %s IDENTIFIED EXTERNALLY", user)
end

function db_CreateUser:POSTGRESQL(user)
  self.command = string.format("CREATE USER %s", user)
end

-- Call using the automatic orchestrator
db_CreateUser:go("nicko")
```

---

## 3. Pseudo System Views

The tool provides vendor-agnostic view names (enclosed in `< >`) that are mapped to actual system views at execution time.

| Pseudo View | Description | Oracle Mapping | PostgreSQL Mapping |
| :--- | :--- | :--- | :--- |
| `<USERS>` | Database Users/Roles | `DBA_USERS` | `PG_ROLES` |

### Column Mapping Specification
| Pseudo View | Data Type | Unified Column | Oracle Column or expression | PostgreSQL Column or expression |
| :--- | :--- | :--- | :--- |
| `<USERS>` | String | `username` | `USERNAME` | `ROLNAME` |
| `<USERS>` | boolean | `can_login` | `ACCOUNT_STATUS IN ('OPEN','EXPIRED(GRACE)')` | `rolcanlogin` |

---

## 4. Cursor Handling

When a `SELECT` statement is executed, the results must be mapped into a Lua table structure for iteration and processing:
- Rows should be indexed by number (array part).
- Columns should be accessible by name within each row (dictionary part).

Example access: `print(result[1].username)`
