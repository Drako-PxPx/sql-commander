import cmd2
import sys
import os
from sql_commander.db import DBConnection
from sql_commander.lua import LuaEngine

class SQLCommanderApp(cmd2.Cmd):
    def __init__(self):
        super().__init__()
        self.prompt = 'sql-cmd> '
        self.intro = 'Welcome to SQL Commander. Type help or ? to list commands.'
        self.db = DBConnection()
        self.lua_engine = LuaEngine(self.db)
        
        # Add uppercase aliases
        self.aliases['CONNECT'] = 'connect'
        self.aliases['DISCONNECT'] = 'disconnect'
        self.aliases['RUN'] = 'run'
        self.aliases['DOC'] = 'doc'

    def do_connect(self, args: cmd2.Statement):
        """Connect to a database using: CONNECT <connection_string>
        Example: 
          CONNECT oracle:MY_TNS_ALIAS
          CONNECT postgresql:MY_SERVICE_NAME
        """
        connection_string = args.args.strip()
        if not connection_string:
            self.poutput("Error: Please provide a connection string.")
            return

        self.poutput(f"Attempting to connect to '{connection_string}'...")
        try:
            success = self.db.connect(connection_string)
            if success:
                self.poutput(f"Successfully connected to {self.db.vendor}.")
                self.prompt = f'sql-cmd ({self.db.vendor})> '
            else:
                self.perror("Failed to connect: Unrecognized connection string format or vendor.")
        except Exception as e:
            self.perror(f"Connection error: {e}")

    def do_disconnect(self, args: cmd2.Statement):
        """Disconnect from the current database."""
        if self.db.conn:
            self.db.disconnect()
            self.poutput("Disconnected.")
            self.prompt = 'sql-cmd> '
        else:
            self.poutput("Not connected.")

    def do_run(self, args: cmd2.Statement):
        """Run a Lua script: RUN <script_file.lua> [key=val ...]"""
        # cmd2 provides arg_list which splits by spaces while respecting quotes
        arg_list = args.arg_list
        if not arg_list:
            self.poutput("Error: Please provide a script file path.")
            return

        file_path = arg_list[0]
        params_raw = arg_list[1:]

        if not os.path.exists(file_path):
            self.poutput(f"Error: Script file not found: {file_path}")
            return

        params = {}
        for p in params_raw:
            if '=' in p:
                k, v = p.split('=', 1)
                params[k.strip()] = v.strip()
            else:
                self.poutput(f"Warning: Ignoring invalid parameter format '{p}'. Expected key=val")

        if not self.db.conn:
            self.poutput("Warning: Not connected to any database. Script database commands will fail.")

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            self.poutput(f"Executing script: {file_path}...")
            self.lua_engine.execute_script(content, params, script_path=file_path)
            self.poutput("Execution completed.")
        except Exception as e:
            self.perror(f"Error executing script:\n{e}")

    def do_doc(self, args: cmd2.Statement):
        """Prints the documentation on screen (first multi-line comment) of the Lua script: DOC <script_file.lua>"""
        arg_list = args.arg_list
        if not arg_list:
            self.poutput("Error: Please provide a script file path.")
            return

        file_path = arg_list[0]

        if not os.path.exists(file_path):
            self.poutput(f"Error: Script file not found: {file_path}")
            return

        try:
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()

            import re
            match = re.search(r'--\[(=*)\[(.*?)\]\1\]', content, re.DOTALL)
            if match:
                self.poutput(match.group(2).strip())
            else:
                self.poutput("No documentation found.")
        except Exception as e:
            self.perror(f"Error reading documentation:\\n{e}")

    def default(self, statement: cmd2.Statement):
        """Execute unrecognised commands as standard SQL queries."""
        import prettytable

        command_upper = statement.command.upper()
        sql_commands = ["SELECT", "INSERT", "DELETE", "UPDATE", "MERGE"]
        
        if command_upper in sql_commands:
            if not self.db.conn:
                self.perror("Error: Not connected to any database. Try CONNECT first.")
                return
            
            try:
                full_query = statement.raw.strip()
                if full_query.endswith(';'):
                    full_query = full_query[:-1] # Remove trailing semicolon if any
                
                result = self.db.execute_query(full_query)
                
                if command_upper == "SELECT":
                    if not result:
                        self.poutput("No rows selected.")
                    else:
                        table = prettytable.PrettyTable()
                        table.field_names = result[0].keys()
                        for row in result:
                            table.add_row(row.values())
                        self.poutput(table)
                else:
                    self.poutput(f"{result} row(s) affected.")
            except Exception as e:
                self.perror(f"SQL Error:\\n{e}")
        else:
            self.poutput(f"{statement.command} is not a recognized command, alias, or macro.")


def main():
    app = SQLCommanderApp()
    sys.exit(app.cmdloop())

if __name__ == '__main__':
    main()
