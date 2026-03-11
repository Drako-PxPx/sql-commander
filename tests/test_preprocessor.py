from sql_commander.lua.preprocessor import LuaPreprocessor

def test_basic_sql():
    script = "SQL SELECT * FROM system"
    result = LuaPreprocessor.process(script)
    assert result == 'result = __sql_execute("SELECT * FROM system", {}, false)'

def test_sql_with_save_output():
    script = "SQL SELECT count(*) FROM dual SAVE OUTPUT IN :my_count"
    result = LuaPreprocessor.process(script)
    assert result == 'my_count = __sql_execute("SELECT count(*) FROM dual", {}, true)'

def test_sql_with_variables():
    script = "SQL SELECT * FROM users WHERE id = $user_id"
    result = LuaPreprocessor.process(script)
    assert result == 'result = __sql_execute("SELECT * FROM users WHERE id = $user_id", {user_id = user_id}, false)'

def test_sql_with_variables_and_save():
    script = "SQL SELECT can_login FROM <USERS> WHERE username = $username SAVE OUTPUT IN :login_status"
    result = LuaPreprocessor.process(script)
    assert result == 'login_status = __sql_execute("SELECT can_login FROM <USERS> WHERE username = $username", {username = username}, true)'

def test_multiple_lines():
    script = """
    local x = 1
    SQL INSERT INTO logs (msg) VALUES ($msg)
    print(x)
    """
    result = LuaPreprocessor.process(script)
    expected = """
    local x = 1
    result = __sql_execute("INSERT INTO logs (msg) VALUES ($msg)", {msg = msg}, false)
    print(x)
    """
    assert result.strip() == expected.strip()

def test_string_escaping():
    script = 'SQL SELECT * FROM sys WHERE name = "test"'
    result = LuaPreprocessor.process(script)
    assert result == 'result = __sql_execute("SELECT * FROM sys WHERE name = \\"test\\"", {}, false)'
