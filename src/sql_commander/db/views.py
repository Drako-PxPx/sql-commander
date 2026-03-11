class PseudoViews:
    VIEWS = {
        "ORACLE": {
            "<USERS>": "(SELECT username as username, CASE WHEN account_status IN ('OPEN', 'EXPIRED(GRACE)') THEN 1 ELSE 0 END as can_login FROM DBA_USERS)"
        },
        "POSTGRESQL": {
            "<USERS>": "(SELECT rolname as username, CASE WHEN rolcanlogin THEN 1 ELSE 0 END as can_login FROM PG_ROLES)"
        }
    }

    @classmethod
    def rewrite(cls, sql: str, vendor: str) -> str:
        if vendor not in cls.VIEWS:
            return sql
            
        rewritten = sql
        for pseudo_name, vendor_sql in cls.VIEWS[vendor].items():
            # simple string replacement. In a robust system, this might need 
            # word boundary matching to avoid partial replacements.
            rewritten = rewritten.replace(pseudo_name, vendor_sql)
            
        return rewritten
