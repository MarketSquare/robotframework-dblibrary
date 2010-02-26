This sample demonstrates the usage of the Robot Framework Database Library.
As a basis for this to run you need a Windows PC and a MySql database running 
(Sure you can convert this to other environments with some experience in using the
Robot Framework.)

For the MySql database you can use the following commands to setup the database
and user used in the example (your path to mysql might of course differ from
the example below):


C:\xampp\mysql\bin>mysql -u root -p
mysql> create database databaselibrarydemo;
mysql> create user 'dblib'@'localhost' identified by 'dblib';
mysql> grant all privileges on databaselibrarydemo.* to 'dblib';


Note: In case you are using a different database you need to change the database connection
string given as one parameter to the "Connect to Database" keyword during Suite Setup. 
