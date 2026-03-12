from sql_commander.lua.engine import LuaEngine
from sql_commander.db import DBConnection
import pytest

class MockDB(DBConnection):
    def __init__(self, vendor="ORACLE"):
        super().__init__()
        self.vendor = vendor
        self.queries = []
        self.conn = True
        
    def execute_query(self, sql, params=None):
        self.queries.append((sql, params))
        if "SELECT" in sql.upper():
            return [{"col": "val"}]
        return 1

def test_any_vendor_orchestrator():
    db = MockDB("ORACLE")
    engine = LuaEngine(db)
    
    script = """
    db_Generic = { command = "" }
    function db_Generic:any(name)
        self.command = "CREATE USER " .. name
    end
    db_Generic:go("BOB")
    """
    engine.execute_script(script)
    
    assert len(db.queries) == 1
    assert db.queries[0][0] == "CREATE USER BOB"

def test_any_vendor_agnostic_cursor():
    db = MockDB("POSTGRESQL")
    engine = LuaEngine(db)
    
    script = """
    db_View = { command = "" }
    function db_View:vw_test_any()
        self.command = "SELECT 'any_view'"
    end
    SQL [test()]
    """
    engine.execute_script(script)
    
    assert len(db.queries) == 1
    assert db.queries[0][0] == "SELECT 'any_view'"

def test_literal_substitution_ampersand():
    db = MockDB("ORACLE")
    engine = LuaEngine(db)
    
    script = """
    local schema = "MY_SCHEMA"
    local table = "MY_TABLE"
    SQL GRANT SELECT ON &schema.&table TO ROLE
    """
    engine.execute_script(script)
    
    assert len(db.queries) == 1
    sql, params = db.queries[0]
    # &schema and &table should be replaced by their values
    assert sql == "GRANT SELECT ON MY_SCHEMA.MY_TABLE TO ROLE"
    # They should NOT be in params as bind variables if only & was used
    assert "schema" not in params
    assert "table" not in params

def test_mixed_literal_and_bind():
    db = MockDB("ORACLE")
    engine = LuaEngine(db)
    
    script = """
    local col = "username"
    local val = "admin"
    SQL SELECT &col FROM users WHERE &col = $val
    """
    engine.execute_script(script)
    
    assert len(db.queries) == 1
    sql, params = db.queries[0]
    assert sql == "SELECT username FROM users WHERE username = :val"
    assert params["val"] == "admin"
    assert "col" not in params

def test_sql_exists_with_literal():
    db = MockDB("ORACLE")
    engine = LuaEngine(db)
    
    script = """
    local tbl = "MY_TABLE"
    local exists = sql_exists("SELECT 1 FROM &tbl")
    is_ok = exists
    """
    engine.execute_script(script)
    
    assert len(db.queries) == 1
    assert db.queries[0][0] == "SELECT 1 FROM MY_TABLE"
    assert engine.lua.globals().is_ok == True
