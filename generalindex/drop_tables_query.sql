DO
$do$
BEGIN
   RAISE NOTICE '%', (
-- EXECUTE (
   SELECT 'DROP TABLE ' || string_agg(format('%I.%I', schemaname, tablename), ', ')
   --  || ' CASCADE' -- optional
   FROM   pg_catalog.pg_tables t
   WHERE  schemaname NOT LIKE 'pg\_%'     -- exclude system schemas
   AND    tablename LIKE 'doc_meta_' || '%'  -- your table name prefix
   );
END
$do$;
