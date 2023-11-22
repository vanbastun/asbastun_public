/* Creating role and call special function to grant all provoleges on all schemas in the database.
	For database developers.
	!!!! Check database name !!!!*/

------create role: commented it because direct IF NOT EXISTS doesn't supported with roles
--CREATE ROLE db_developer_group;
------grant
SELECT * FROM func_grant_all_privileges_on_database_to_role('db_developer_group', 'DATABASENAME');