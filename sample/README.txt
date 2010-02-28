This sample demonstrates the usage of the Robot Framework Database Library.
The example as such is platform independent, but the provided start scripts of cause do
depend on the corresponding platform.

Currently the following start scripts are provided:

Windows : startDBLibraryDemo_MySql.bat
Mac OS X: startDBLibraryDemo_MySql.sh

Of course the start-script for Mac OS X can be easily adapted for other Unix
operating systems.

As the port where you are running the MySql database might differ from the default
port this value is set as a variable in the start script. Please change it if needed.

For the MySql database you can use the following commands to setup the database
and user required for the example (your path to mysql might of course differ from
the example below):

C:\xampp\mysql\bin>mysql -u root -p
mysql> create database databaselibrarydemo;
mysql> create user 'dblib'@'localhost' identified by 'dblib';
mysql> grant all privileges on databaselibrarydemo.* to 'dblib';

For Mac OS X the easiest way to setup a MySql database is for sure using MAMP:
http://www.mamp.info/de/index.html


NOTE:
In case you are using a different database you need to change the database connection
string given as one parameter to the "Connect to Database" keyword during Suite Setup. In 
addition you need to edit the CLASSPATH in the start script to include the proper Driver-JAR. 

