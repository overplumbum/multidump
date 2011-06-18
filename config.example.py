DSN = {
    'SALES: dev vs test': (
        'sftp://user:pass@dev.host/postgres:pgpasswd@localhost/salesdb',
        'sftp://user:pass@test.host/postgres:pgpasswd@localhost/salesdb', 
    ),
}

IGNORE_SHEMAS = '_slonycluster_|test|pgunit|test|local|temp'
