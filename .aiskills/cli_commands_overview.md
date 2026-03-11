# CLI Commands Overview

This document specifies the commands available for this CLI project.

## run
Executes a Lua 5.4 script with support for dynamic, typed parameter injection.

### Syntax
`run <script_path> [key1=val1 key2=val2 ...]`

### Parameter Injection Logic
Parameters are passed as `key=value` pairs and are automatically injected into the Lua global scope (`_G`) before the script executes. The tool infers data types based on the value format:

- **Numbers:** e.g., `limit=50` (Injected as a Lua number)
- **Booleans:** e.g., `debug=true` or `verbose=false` (Injected as Lua booleans)
- **Arrays (JSON Syntax):** e.g., `ids=[1,2,3]` or `roles=["admin","user"]` (Injected as standard 1-indexed Lua tables)
- **Strings:** e.g., `schema=HR` (Default type if no other match is found)

### Examples
- **Basic:** `sql-cmd> run health_check.lua`
- **With Parameters:** `sql-cmd> run cleanup.lua schema=audit_logs days=30 verbose=true`
- **With Arrays:** `sql-cmd> run user_report.lua roles=["manager","clerk"] ids=[101,102]`

### Error Handling
- **File Not Found:** `Error: Script '<filename>' not found.`
- **Invalid Parameter Format:** `Error: Invalid parameter syntax for '<pair>'. Expected key=value.`
- **Runtime Error:** Displays the Lua stack trace if the script fails.

---

## doc
Prints the documentation on screen (first multi-line comment) of the Lua script

### Syntax
`doc <script_path>`

## Supported SQL Commands
The CLI natively captures and executes the following standard SQL commands, passing them directly to the underlying database engine:
- `SELECT` (Result grids are formatted using `prettytable`)
- `INSERT`
- `UPDATE`
- `DELETE`
- `MERGE`

For Data Manipulation Language (DML) commands, the interface displays the number of affected rows.
