CREATE KEYSPACE IF NOT EXISTS testspace
WITH REPLICATION = {
'class' : 'SimpleStrategy',
'replication_factor' : 1
};

DROP TABLE IF EXISTS testspace.user;
CREATE TABLE testspace.user (
id varchar,
emailverified boolean,
pwd varchar,
address map<varchar, varchar>,
PRIMARY KEY (id)
);

INSERT INTO testspace.user (id, emailverified, pwd, address) VALUES ('daf9d9fac2c5ed8217839', false, '76866435ca96e4', {'street' : 'Bendel Close', 'city': 'Lagos'});
INSERT INTO testspace.user (id, emailverified, pwd, address) VALUES ('daf9d9fac2c5ed82130', true, '76a866435ca9e4', {'street' : 'Bendel Close', 'city': 'Lagos'});
INSERT INTO testspace.user (id, emailverified, pwd, address) VALUES ('dftf9d9fac2c5ed8217849', false, '76a86tttt435ca96e4', {'street' : 'Bendel Close', 'city': 'Lagos'});
INSERT INTO testspace.user (id, emailverified, pwd, address) VALUES ('daf9d9fac2sssed8217859', true, '76a866435soo96e4', {'street' : 'Bendel Close', 'city': 'Lagos'});
INSERT INTO testspace.user (id, emailverified, pwd, address) VALUES ('daf9d9fac217837', false, '76a866435ca96psojdjdjse4', {'street' : 'Bendel Close', 'city': 'Lagos'});

DROP TABLE IF EXISTS testspace.phonebook;
CREATE TABLE testspace.phonebook (
userid varchar,
phone varchar,
createdat timestamp,
PRIMARY KEY (userid)
);

INSERT INTO testspace.phonebook (userid, phone, createdat) VALUES ('daf9d9fac2c5ed8217839', '080238383771', '2019-01-11 11:12:14');
INSERT INTO testspace.phonebook (userid, phone, createdat) VALUES ('daf9d9fac2c5ed82130', '080238383791', '2019-01-12 11:12:14');
INSERT INTO testspace.phonebook (userid, phone, createdat) VALUES ('dftf9d9fac2c5ed8217849', '08113456789', '2019-01-13 11:12:14');
INSERT INTO testspace.phonebook (userid, phone, createdat) VALUES ('daf9d9fac2sssed8217859', '08023888991', '2019-01-01 11:12:14');
INSERT INTO testspace.phonebook (userid, phone, createdat) VALUES ('daf9d9fac217837', '080238383771', '2019-01-03 11:12:14');


SELECT COUNT(*) as total_users FROM testspace.user;
SELECT COUNT(*) as total_phonebook FROM testspace.phonebook;