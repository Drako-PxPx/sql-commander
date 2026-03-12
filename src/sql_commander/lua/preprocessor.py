import re

class LuaPreprocessor:
    """
    Pre-processes custom Lua scripts to transpile custom SQL constructs into valid Lua code.
    """
    
    # Matches: SQL <query> [SAVE OUTPUT IN :var]
    # We use a permissive regex to capture everything between SQL and optionally SAVE OUTPUT IN
    SQL_LINE_RE = re.compile(
        r'^\s*SQL\s+(.*?)(?:\s+SAVE\s+OUTPUT\s+IN\s+:([a-zA-Z_]\w*))?\s*$', 
        re.IGNORECASE
    )
    
    # Matches: $variable_name or &variable_name
    VAR_RE = re.compile(r'[\$&]([a-zA-Z_]\w*)')

    @classmethod
    def process(cls, script_content: str) -> str:
        lines = script_content.split('\n')
        processed_lines = []
        
        for line in lines:
            # We only transpile single-line SQL commands as implied by the syntax
            # If the line starts with SQL (ignoring whitespace), transpile it
            if line.lstrip().upper().startswith('SQL '):
                match = cls.SQL_LINE_RE.match(line)
                if match:
                    sql_statement = match.group(1).strip()
                    save_var = match.group(2)
                    
                    # Find bind variables
                    extracted_vars = cls.VAR_RE.findall(sql_statement)
                    # Deduplicate variables while preserving order
                    unique_vars = list(dict.fromkeys(extracted_vars))
                    
                    # Construct the Lua table for arguments
                    # Example: { username = username, age = age }
                    args_table = "{" + ", ".join(f"{v} = {v}" for v in unique_vars) + "}"
                    
                    # Escape quotes in the SQL statement
                    # We can use Lua long brackets [[ ]] but we must ensure we don't clash
                    # Alternatively, just escape double quotes and use double quotes.
                    escaped_sql = sql_statement.replace('"', '\\"')
                    
                    if save_var:
                        new_line = f'{save_var} = __sql_execute("{escaped_sql}", {args_table}, true)'
                    else:
                        new_line = f'result = __sql_execute("{escaped_sql}", {args_table}, false)'
                        
                    # Preserve leading whitespace
                    leading_ws = line[:len(line) - len(line.lstrip())]
                    processed_lines.append(leading_ws + new_line)
                else:
                    processed_lines.append(line)
            else:
                # Transpile sql_exists calls if they use literal strings
                new_line = line
                # Regex for sql_exists("...") or sql_exists('...')
                # Using [^"']* to avoid greediness over multiple arguments
                sql_exists_matches = list(re.finditer(r'sql_exists\s*\(\s*(["\'])(.*?)\1\s*\)', new_line))
                for match in reversed(sql_exists_matches):
                    quote = match.group(1)
                    sql_statement = match.group(2)
                    
                    # Find variables ($ or &)
                    extracted_vars = cls.VAR_RE.findall(sql_statement)
                    unique_vars = list(dict.fromkeys(extracted_vars))
                    args_table = "{" + ", ".join(f"{v} = {v}" for v in unique_vars) + "}"
                    
                    # Reconstruct the call with the args_table
                    # We use double quotes for the new call
                    replacement = f'sql_exists("{sql_statement}", {args_table})'
                    new_line = new_line[:match.start()] + replacement + new_line[match.end():]
                processed_lines.append(new_line)
                
        return '\n'.join(processed_lines)
