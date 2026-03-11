import pytest
from sql_commander.lua.engine import LuaEngine
from typing import Optional, Any, List, Dict

class MockDB:
    def __init__(self, vendor):
        self.vendor = vendor
        self.queries = []
        self.conn = True

    def execute_query(self, sql: str, params: Optional[Dict] = None) -> List[Dict[str, Any]]:
        self.queries.append((sql, params))
        if "SELECT" in sql.upper():
            return [{"plan": "some plan output"}]
        return 1

def test_rdbms_function():
    db = MockDB("ORACLE")
    engine = LuaEngine(db)
    
    script = """
    rdbms("ALTER SESSION SET CURRENT_SCHEMA = HR")
    """
    engine.execute_script(script)
    
    assert len(db.queries) == 1
    assert db.queries[0][0] == "ALTER SESSION SET CURRENT_SCHEMA = HR"

def test_agnostic_cursor_discovery():
    db = MockDB("ORACLE")
    engine = LuaEngine(db)
    
    script = """
    db_Test = { command = "" }
    function db_Test:vw_myview_oracle(arg1)
        rdbms("PRE-STEP " .. arg1)
        self.command = "SELECT * FROM oracle_table"
    end
    
    SQL [myview('hello')]
    """
    engine.execute_script(script)
    
    # 1. rdbms call, 2. the SQL call
    assert len(db.queries) == 2
    assert db.queries[0][0] == "PRE-STEP hello"
    assert db.queries[1][0] == "SELECT * FROM oracle_table"

def test_agnostic_cursor_multiple_args():
    db = MockDB("POSTGRESQL")
    engine = LuaEngine(db)
    
    script = """
    db_Test = { command = "" }
    function db_Test:vw_complex_postgresql(a, b)
        self.command = "SELECT " .. a .. " + " .. b
    end
    
    SQL [complex(1, 2)]
    """
    engine.execute_script(script)
    
    assert len(db.queries) == 1
    assert db.queries[0][0] == "SELECT 1 + 2"

def test_agnostic_cursor_missing_method_error():
    db = MockDB("POSTGRESQL")
    engine = LuaEngine(db)
    
    script = """
    db_Test = {} -- No vw_ method
    SQL SELECT * FROM [missing()]
    """
    with pytest.raises(Exception) as excinfo:
        engine.execute_script(script)
    assert "not found" in str(excinfo.value)
