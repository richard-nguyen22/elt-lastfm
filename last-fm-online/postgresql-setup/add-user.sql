/* Create a new user for online lastfm database:
	- username: api1
	- passwd: 'Open API@1'
	- database: openapi.lastfm
*/
CREATE USER api1 WITH PASSWORD 'Open API@1' LOGIN NOSUPERUSER NOCREATEDB NOINHERIT;
-- Grant ALL PRIVILEGES on openapi database to api1
GRANT CREATE, CONNECT ON DATABASE openapi TO api1;
-- Grant ALL PRIVILEGES on 'lastfm' schema to api1
GRANT ALL ON SCHEMA lastfm TO api1;
--DROP OWNED BY api1; -- Drop all privileges owned by api1 
--DROP USER api1 -- Then can drop api1

-- Show all users
SELECT *
FROM pg_user
