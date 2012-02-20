
set %CLASSPATH% = %CLASSPATH%;..\robotframework-dblibrary\dblibrary\src\main\java

jython -Dpython.path=C:\Python26\Lib\site-packages libdoc.py -f HTML -V v2.0 -T "DatabaseLibrary_-_Documentation" -o DatabaseLibrary_v20.html ..\robotframework-dblibrary\dblibrary\src\main\java\org\robot\database\keywords\DatabaseLibrary.java 