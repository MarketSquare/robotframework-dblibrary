



C:\xampp\mysql\bin>mysql -u root -p
mysql> create database databaselibrarydemo;
mysql> create user 'dblib'@'localhost' identified by 'dblib';
mysql> grant all privileges on databaselibrarydemo.* to 'dblib';
