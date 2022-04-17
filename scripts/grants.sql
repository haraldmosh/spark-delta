--Roles
CREATE ROLE readwrite;
GRANT CREATE, SELECT, INSERT, UPDATE, ALTER ON `metastore`.* TO readwrite;

--Apply Roles
REVOKE ALL PRIVILEGES ON `metastore`.* FROM `dataeng`@`%`; GRANT readwrite TO `dataeng`@`%`;
FLUSH PRIVILEGES;

--VIEW Grants
mysql> SHOW GRANTS FOR `dataeng`@`%`; 
/*
+------------------------------------------------------+ 
| Grants for dataeng@% | 
+------------------------------------------------------+ 
| GRANT USAGE ON *.* TO `dataeng`@`%` | 
| GRANT ALL PRIVILEGES ON `default`.* TO `dataeng`@`%` | 
| GRANT `readwrite`@`%` TO `dataeng`@`%` | 
+------------------------------------------------------+
*/