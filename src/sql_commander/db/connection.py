import os
import oracledb
import psycopg
from typing import Optional, Any, List, Dict

class DBConnection:
    def __init__(self):
        self.conn = None
        self.vendor: Optional[str] = None

    def connect(self, connection_string: str) -> bool:
        """
        Attempts to connect using External Password Store (Oracle) or Service Name (PostgreSQL).
        """
        self.disconnect()
        
        # Determine vendor based on prefix if provided
        target_vendor = None
        if connection_string.startswith("postgresql:"):
            target_vendor = "POSTGRESQL"
            connection_string = connection_string.split(":", 1)[1]
        elif connection_string.startswith("oracle:"):
            target_vendor = "ORACLE"
            connection_string = connection_string.split(":", 1)[1]
            
        if target_vendor == "ORACLE":
            return self._connect_oracle(connection_string)
        elif target_vendor == "POSTGRESQL":
            return self._connect_postgres(connection_string)
        else:
            # Try Oracle first, then PostgreSQL
            try:
                if self._connect_oracle(connection_string):
                    return True
            except Exception:
                pass
                
            try:
                if self._connect_postgres(connection_string):
                    return True
            except Exception:
                pass
                
            return False

    def _connect_oracle(self, tns_alias: str) -> bool:
        try:
            # For EPS, we just need to provide the dsn (TNS alias), and rely on sqlnet.ora/wallet
            # oracledb thick mode is often required for wallet connections depending on setup,
            # but thin mode supports it in recent versions if config_dir is passed or TNS_ADMIN is set.
            # Using externalauth=True to leverage the external password store.
            
            # Thick mode might be required if the user has a complex wallet setup
            # We initialize thick mode, optionally passing config_dir.
            tns_admin = os.environ.get("TNS_ADMIN")
            try:
                if tns_admin:
                    oracledb.init_oracle_client(config_dir=tns_admin)
                else:
                    oracledb.init_oracle_client()
            except oracledb.ProgrammingError:
                # Client might already be initialized
                pass
                
            self.conn = oracledb.connect(dsn=tns_alias, externalauth=True)
            self.vendor = "ORACLE"
            return True
        except Exception as e:
            self.conn = None
            self.vendor = None
            raise e

    def _connect_postgres(self, service_name: str) -> bool:
        try:
            # For PostgreSQL, using the service name. psycopg 3 supports it via the dsn parameter:
            # 'service=my_service'
            self.conn = psycopg.connect(f"service={service_name}")
            self.vendor = "POSTGRESQL"
            return True
        except Exception as e:
            self.conn = None
            self.vendor = None
            raise e

    def disconnect(self):
        if self.conn:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None
            self.vendor = None

    def execute_query(self, sql: str, params: Optional[Dict|List] = None) -> List[Dict[str, Any]]:
        """
        Executes a SQL query and returns the results as a list of dictionaries.
        """
        if not self.conn:
            raise Exception("Not connected to a database")
            
        params = params or {}
        
        # Mapping Oracle and Postgres cursor results to list of dicts
        target_sql = sql
        
        with self.conn.cursor() as cursor:
            if self.vendor == "ORACLE":
                cursor.execute(target_sql, params)
                if cursor.description:
                    # Oracle column names are in cursor.description
                    columns = [col[0].lower() for col in cursor.description]
                    results = []
                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))
                    return results
                else:
                    self.conn.commit()
                    return cursor.rowcount
                
            elif self.vendor == "POSTGRESQL":
                # Psycopg 3 syntax
                cursor.execute(target_sql, params)
                if cursor.description:
                    columns = [col.name for col in cursor.description]
                    results = []
                    for row in cursor.fetchall():
                        results.append(dict(zip(columns, row)))
                    return results
                else:
                    self.conn.commit()
                    return cursor.rowcount
