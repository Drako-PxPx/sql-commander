class PseudoViews:
    VIEWS = {
        "ORACLE": {
            "<USERS>": "(SELECT USERNAME as username, CASE WHEN ACCOUNT_STATUS IN ('OPEN', 'EXPIRED(GRACE)') THEN 1 ELSE 0 END as can_login FROM DBA_USERS)",
            "<TABLES>": "(SELECT OWNER as owner, TABLE_NAME as table_name FROM DBA_TABLES)",
            "<ROLES>": "(SELECT ROLE as role FROM DBA_ROLES)"
        },
        "POSTGRESQL": {
            "<USERS>": "(SELECT rolname as username, CASE WHEN rolcanlogin THEN 1 ELSE 0 END as can_login FROM PG_ROLES)",
            "<TABLES>": "(SELECT schemaname as owner, tablename as table_name FROM PG_TABLES)",
            "<ROLES>": "(SELECT rolname as role FROM PG_ROLES)"
        }
    }

    @classmethod
    def rewrite(cls, sql: str, vendor: str) -> str:
        vendor_key = vendor.upper()
        if vendor_key not in cls.VIEWS:
            return sql
            
        rewritten = sql
        for pseudo_name, vendor_sql in cls.VIEWS[vendor_key].items():
            # simple string replacement. In a robust system, this might need 
            # word boundary matching to avoid partial replacements.
            rewritten = rewritten.replace(pseudo_name, vendor_sql)
            
        return rewritten
