SET 'table.dynamic-table-options.enabled' = 'true';

CREATE CATALOG jon WITH (
    'type' = 'imap',
    'host' = 'greenmail',
    'port' = '3143',
    'user' = 'jon',
    'password' = 'jon'
);

CREATE CATALOG jane WITH (
    'type' = 'imap',
    'host' = 'greenmail',
    'port' = '3143',
    'user' = 'jane',
    'password' = 'jane'
);
