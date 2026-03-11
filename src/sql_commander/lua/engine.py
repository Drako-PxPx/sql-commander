import lupa
import re
import json
from typing import Optional, Any, Dict
from sql_commander.db import DBConnection, PseudoViews
from sql_commander.lua.preprocessor import LuaPreprocessor

class LuaEngine:
    def __init__(self, db: DBConnection):
        self.db = db
        # Set unpack_returned_tuples to true to make function returns cleaner
        self.lua = lupa.LuaRuntime(unpack_returned_tuples=True)
        self._register_globals()

    def _register_globals(self):
        # Register __sql_execute
        self.lua.globals()['__sql_execute'] = self.__sql_execute
        self.lua.globals()['__get_vendor'] = lambda: self.db.vendor
        
        # Inject the db_ pattern orchestrator code snippet
        orchestrator_lua = """
        function _init_orchestrator()
            local function add_go(t)
                if not t.go then
                    t.go = function(self, ...)
                        local vendor = __get_vendor()
                        if vendor == nil then
                            error("Not connected to a database")
                        end
                        local method = self[vendor]
                        if method then
                            method(self, ...)
                            if self.command and self.command ~= "" then
                                return __sql_execute(self.command, nil, false)
                            end
                        else
                            error("Method for vendor " .. vendor .. " not found in this db_ table")
                        end
                    end
                end
            end
            
            local mt = getmetatable(_G) or {}
            local old_newindex = mt.__newindex
            mt.__newindex = function(t, k, v)
                if type(k) == "string" and string.sub(k, 1, 3) == "db_" and type(v) == "table" then
                    add_go(v)
                end
                if type(old_newindex) == "function" then
                    old_newindex(t, k, v)
                elseif type(old_newindex) == "table" then
                    old_newindex[k] = v
                else
                    rawset(t, k, v)
                end
            end
            setmetatable(_G, mt)
        end
        _init_orchestrator()
        """
        self.lua.execute(orchestrator_lua)

    def _infer_type(self, val_str: str) -> Any:
        # Booleans
        if val_str.lower() == 'true': return True
        if val_str.lower() == 'false': return False
        
        # Numbers (Int and Float)
        try:
            if '.' in val_str:
                return float(val_str)
            return int(val_str)
        except ValueError:
            pass
            
        # JSON Arrays (Lists)
        if val_str.startswith('[') and val_str.endswith(']'):
            try:
                return json.loads(val_str)
            except json.JSONDecodeError:
                pass
                
        # String as fallback
        return val_str

    def __sql_execute(self, sql_statement: str, args_table: Optional[Any] = None, is_single_value_output: bool = False):
        if not self.db.vendor:
            raise Exception("Cannot execute SQL: Not connected to a database.")
            
        params = {}
        if args_table:
            # Extract items from Lua table wrapper
            for k in args_table:
                params[k] = args_table[k]
                
        # 1. Rewrite pseudo views (e.g. <USERS>)
        vendor_sql = PseudoViews.rewrite(sql_statement, self.db.vendor)

        # 2. Variable expansion for SQL "IN" clauses
        # Scan for $variable_name and check if it's a table/list
        for var_name in list(params.keys()):
            val = params[var_name]
            
            # Check for list-like objects (Python list or lupa table wrapper)
            is_list = isinstance(val, (list, tuple))
            if not is_list and hasattr(val, 'values') and callable(val.values):
                # Probably a lupa table
                is_list = True
                
            if is_list:
                # Format as SQL list: (item1, item2, ...)
                items = []
                # If it's a lupa table, we can iterate over values
                val_iterator = val.values() if hasattr(val, 'values') and callable(val.values) else val
                
                for item in val_iterator:
                    if isinstance(item, str):
                        items.append(f"'{item}'")
                    elif isinstance(item, bool):
                        items.append('1' if item else '0')
                    else:
                        items.append(str(item))
                
                if not items:
                    formatted_list = "(NULL)" # SQL safety for empty lists
                else:
                    formatted_list = "(" + ", ".join(items) + ")"
                
                # Replace $var in SQL with literal list and remove from bind params
                vendor_sql = vendor_sql.replace(f"${var_name}", formatted_list)
                del params[var_name]
        
        # 3. Rewrite remaining script bind variables ($var) to driver-specific placeholders
        if self.db.vendor == "ORACLE":
            vendor_sql = re.sub(r'\$([a-zA-Z_]\w*)', r':\1', vendor_sql)
        elif self.db.vendor == "POSTGRESQL":
            vendor_sql = re.sub(r'\$([a-zA-Z_]\w*)', r'%(\1)s', vendor_sql)
            
        # 4. Execute
        results = self.db.execute_query(vendor_sql, params)
        
        # 5. Handle Save Output
        if is_single_value_output:
            if not results or isinstance(results, int):
                return None
            first_row = results[0]
            if not first_row:
                return None
            # Return first column of first row
            return list(first_row.values())[0]
            
        # 6. Handle cursor return (List of Dicts mapped to Lua Tables)
        # Return row count integer for DML/DDL operations
        if isinstance(results, int):
            return results

        # Deep map to Lua tables so `ipairs` and typical Lua patterns work correctly
        lua_table = self.lua.eval("{}")
        for i, row in enumerate(results, start=1):
            row_table = self.lua.eval("{}")
            for k, v in row.items():
                row_table[k] = v
            lua_table[i] = row_table
            
        return lua_table

    def execute_script(self, script_content: str, params: Optional[Dict[str, str]] = None):
        # Inject parameters if provided
        if params:
            for k, v in params.items():
                val = self._infer_type(v)
                if isinstance(val, list):
                    # Convert to native Lua table
                    self.lua.globals()[k] = self.lua.table_from(val)
                else:
                    self.lua.globals()[k] = val
                
        processed = LuaPreprocessor.process(script_content)
        try:
            self.lua.execute(processed)
        except lupa.LuaError as e:
            raise Exception(f"Lua Runtime Error in script:\n{e}")
