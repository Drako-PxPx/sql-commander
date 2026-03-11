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
        # Register core bridges
        self.lua.globals()['__sql_execute'] = self.__sql_execute
        self.lua.globals()['__get_vendor'] = lambda: self.db.vendor.lower()
        
        # rdbms() function for non-returning SQL
        self.lua.globals()['rdbms'] = self.__rdbms_execute
        
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

    def __rdbms_execute(self, sql: str):
        if not self.db.conn:
            raise Exception("Not connected to a database")
        try:
            self.db.execute_query(sql)
        except Exception as e:
            # Re-raise to halt Lua execution
            raise Exception(f"RDBMS Error: {e}")

    def __sql_execute(self, sql_statement: str, args_table: Optional[Any] = None, is_single_value_output: bool = False):
        if not self.db.vendor:
            raise Exception("Cannot execute SQL: Not connected to a database.")
            
        params = {}
        if args_table:
            for k in args_table:
                params[k] = args_table[k]
                
        # 1. Agnostic Cursor Replacement ([view(args)])
        # Pattern matches [view_name(optional_args)]
        vendor_sql = sql_statement
        agnostic_matches = re.findall(r'\[(\w+)\((.*?)\)\]', vendor_sql)
        
        for view_name, raw_args in agnostic_matches:
            target_method = f"vw_{view_name}_{self.db.vendor.lower()}"
            found_command = None
            
            # Search db_ objects in Lua global scope
            for key in self.lua.globals():
                if isinstance(key, str) and key.startswith("db_"):
                    lua_table = self.lua.globals()[key]
                    if target_method in lua_table:
                        # Parse args into Lua values using a temporary table/expression
                        try:
                            # Use a function call bridge to pass arguments safely
                            lua_args = self.lua.eval(f"function(...) return ... end")({raw_args}) # Simplified for now
                            # More robust: use eval with a table wrapper
                            if raw_args.strip() == "":
                                method_args = []
                            else:
                                method_args = self.lua.eval(f"{{ {raw_args} }}")
                                # method_args is a lupa table, convert to list if it's an array part
                                if hasattr(method_args, 'values') and callable(method_args.values):
                                    method_args = list(method_args.values())
                                else:
                                    method_args = [method_args]
                            
                            # Execute the method
                            lua_table[target_method](lua_table, *method_args)
                            found_command = lua_table["command"]
                            break
                        except Exception as e:
                            raise Exception(f"Error executing agnostic view {view_name}: {e}")
            
            if found_command:
                vendor_sql = vendor_sql.replace(f"[{view_name}({raw_args})]", found_command)
            else:
                raise Exception(f"Agnostic view method '{target_method}' not found in any db_ object.")

        # 2. Rewrite pseudo views (e.g. <USERS>)
        vendor_sql = PseudoViews.rewrite(vendor_sql, self.db.vendor)

        # 3. Variable expansion for SQL "IN" clauses
        for var_name in list(params.keys()):
            val = params[var_name]
            is_list = isinstance(val, (list, tuple))
            if not is_list and hasattr(val, 'values') and callable(val.values):
                is_list = True
                
            if is_list:
                items = []
                val_iterator = val.values() if hasattr(val, 'values') and callable(val.values) else val
                for item in val_iterator:
                    if isinstance(item, str):
                        items.append(f"'{item}'")
                    elif isinstance(item, bool):
                        items.append('1' if item else '0')
                    else:
                        items.append(str(item))
                
                formatted_list = "(" + (", ".join(items) if items else "NULL") + ")"
                vendor_sql = vendor_sql.replace(f"${var_name}", formatted_list)
                del params[var_name]
        
        # 4. Driver-specific placeholders
        if self.db.vendor == "oracle":
            vendor_sql = re.sub(r'\$([a-zA-Z_]\w*)', r':\1', vendor_sql)
        elif self.db.vendor == "postgresql":
            vendor_sql = re.sub(r'\$([a-zA-Z_]\w*)', r'%(\1)s', vendor_sql)
            
        # 5. Execute
        results = self.db.execute_query(vendor_sql, params)
        
        # 6. Handle Save Output
        if is_single_value_output:
            if not results or isinstance(results, int):
                return None
            first_row = results[0]
            return list(first_row.values())[0] if first_row else None
            
        # 7. Cursor return (List of Dicts mapped to Lua Tables)
        if isinstance(results, int):
            return results

        lua_table = self.lua.eval("{}")
        for i, row in enumerate(results, start=1):
            row_table = self.lua.eval("{}")
            for k, v in row.items():
                row_table[k] = v
            lua_table[i] = row_table
            
        return lua_table

    def execute_script(self, script_content: str, params: Optional[Dict[str, str]] = None):
        if params:
            for k, v in params.items():
                val = self._infer_type(v)
                if isinstance(val, list):
                    self.lua.globals()[k] = self.lua.table_from(val)
                else:
                    self.lua.globals()[k] = val
                
        processed = LuaPreprocessor.process(script_content)
        try:
            self.lua.execute(processed)
        except lupa.LuaError as e:
            raise Exception(f"Lua Runtime Error in script:\n{e}")
