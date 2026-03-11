import pytest
from sql_commander.lua.engine import LuaEngine
from typing import Optional, Any, List, Dict

class MockDB:
    def __init__(self, vendor):
        self.vendor = vendor
        self.queries = []
        self.conn = True # Simulating connected

    def execute_query(self, sql: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        self.queries.append((sql, params))
        if "SELECT" in sql.upper():
            return [{"id": 1, "username": "admin"}]
        return 1

def test_run_parameters_injection():
    db = MockDB("ORACLE")
    engine = LuaEngine(db)
    
    # Parameters as strings from CLI
    params = {
        "limit": "50",
        "debug": "true",
        "schema": "HR",
        "ids": "[101, 102, 103]"
    }
    
    # This script will verify the types in Lua
    script = """
    assert(type(limit) == "number")
    assert(limit == 50)
    assert(type(debug) == "boolean")
    assert(debug == true)
    assert(type(schema) == "string")
    assert(schema == "HR")
    assert(type(ids) == "table")
    assert(#ids == 3)
    assert(ids[1] == 101)
    """
    
    engine.execute_script(script, params)

def test_sql_in_expansion():
    db = MockDB("POSTGRESQL")
    engine = LuaEngine(db)
    
    params = {
        "roles": '["admin", "manager"]'
    }
    
    script = """
    SQL SELECT * FROM <USERS> WHERE role IN $roles
    """
    
    engine.execute_script(script, params)
    
    assert len(db.queries) == 1
    sql, params_out = db.queries[0]
    
    # Verify that $roles was expanded to literal list and removed from params
    assert "role IN ('admin', 'manager')" in sql
    assert "roles" not in params_out

def test_sql_mixed_params():
    db = MockDB("ORACLE")
    engine = LuaEngine(db)
    
    params = {
        "ids": "[1, 2]",
        "status": "active"
    }
    
    script = """
    SQL UPDATE users SET status = $status WHERE id IN $ids
    """
    
    engine.execute_script(script, params)
    
    assert len(db.queries) == 1
    sql, params_out = db.queries[0]
    
    # status should be a bind variable, ids should be expanded
    assert "SET status = :status" in sql
    assert "WHERE id IN (1, 2)" in sql
    assert params_out["status"] == "active"
    assert "ids" not in params_out
