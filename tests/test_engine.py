from sql_commander.lua.engine import LuaEngine
from sql_commander.db import DBConnection
import pytest

class MockDB(DBConnection):
    def __init__(self, vendor="ORACLE"):
        super().__init__()
        self.vendor = vendor
        self.queries = []
        
    def execute_query(self, sql, params=None):
        self.queries.append((sql, params))
        # Return mock rows
        return [{"id": 1, "username": "mockuser"}]

def test_engine_sql_execute_oracle():
    db = MockDB("ORACLE")
    engine = LuaEngine(db)
    
    script = """
    local x = 100
    SQL SELECT * FROM system WHERE id = $x SAVE OUTPUT IN :sys_id
    """
    engine.execute_script(script)
    
    assert len(db.queries) == 1
    sql, params = db.queries[0]
    assert sql == "SELECT * FROM system WHERE id = :x"
    assert params["x"] == 100
    # user_id should be 1 since mockuser returns id=1 first
    assert engine.lua.globals().sys_id == 1

def test_engine_sql_execute_postgres():
    db = MockDB("POSTGRESQL")
    engine = LuaEngine(db)
    
    script = """
    local my_user = 'admin'
    SQL SELECT can_login FROM <USERS> WHERE username = $my_user SAVE OUTPUT IN :can_login
    """
    engine.execute_script(script)
    
    assert len(db.queries) == 1
    sql, params = db.queries[0]
    # Check pseudo view replacement and postgres placeholder
    assert sql == "SELECT can_login FROM (SELECT rolname as username, CASE WHEN rolcanlogin THEN 1 ELSE 0 END as can_login FROM PG_ROLES) WHERE username = %(my_user)s"
    assert params["my_user"] == "admin"
    assert engine.lua.globals().can_login == 1

def test_engine_return_table():
    db = MockDB("ORACLE")
    engine = LuaEngine(db)
    
    script = """
    SQL SELECT * FROM users
    -- The execute implicitly returns if we wrap it, but the preprocessor
    -- currently just executes it natively if no SAVE OUTPUT IN is provided.
    -- Let's change this: Wait, if there's no SAVE OUTPUT, Lua preprocessor does:
    -- __sql_execute(..., {args}, false)
    -- So the result is discarded unless assigned. But Lua script can't assign it since it's an SQL command without SAVE OUTPUT.
    -- The requirement: "Large Results: For standard SELECT statements, results are returned as a Lua table."
    -- Ah, wait! The requirement says "The SAVE OUTPUT clause is only valid for SELECT statements that return a single value. The result must be stored in the specified Lua global variable... For standard SELECT statements, results are returned as a Lua table."
    -- It didn't specify HOW they are accessed if not using SAVE OUTPUT.
    -- "The result must be mapped into a Lua table structure for iteration and processing".
    -- "Example access: print(result[1].username)"
    -- This means if we do `SQL SELECT * FROM users`, it auto-saves into a variable called `result`!
    """

def test_db_pattern_orchestration():
    db = MockDB("POSTGRESQL")
    engine = LuaEngine(db)
    
    script = """
    db_Test = { command = "" }
    function db_Test:ORACLE()
        self.command = "SELECT 1 FROM DUAL"
    end
    function db_Test:POSTGRESQL()
        self.command = "SELECT 1"
    end
    
    db_Test:go()
    """
    
    engine.execute_script(script)
    
    assert len(db.queries) == 1
    sql, params = db.queries[0]
    assert sql == "SELECT 1"
