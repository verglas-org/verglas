SELECT 'CREATE DATABASE verglas_scheduler OWNER verglas'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'verglas_scheduler')\gexec

SELECT 'CREATE DATABASE verglas_permissions OWNER verglas'
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'verglas_permissions')\gexec
