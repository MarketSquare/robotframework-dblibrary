# Database library for Robot Framework
Introduction
------------
The Robot Framework Database Library is a library which provides common functionality for testing database contents.

* More information about this library can be found in the
  [Keyword Documentation](https://repo1.maven.org/maven2/com/github/marketsquare/robotframework-dblibrary/3.3.0/robotframework-dblibrary-3.3.0.html).
* For keyword completion in RIDE you can download this
  [Library Specs](https://repo1.maven.org/maven2/com/github/marketsquare/robotframework-dblibrary/3.3.0/robotframework-dblibrary-3.3.0.xml)
  and place it in your PYTHONPATH.
Usage
-----
If you are using the [robotframework-maven-plugin](http://robotframework.org/MavenPlugin/) you can
use this library by adding the following dependency to
your pom.xml:

    <dependency>
        <groupId>com.github.marketsquare</groupId>
        <artifactId>robotframework-dblibrary</artifactId>
        <version>3.3.0</version>
    </dependency>

With Gradle, library can be use by importing it as a dependency in build.gradle:

    runtime('com.github.marketsquare:robotframework-dblibrary:3.3.0')

Library import in Robot tests can be done with:

|                    |                                 |
| ----------------   | ------------------------------- |
| *** Settings ***   |                                 |
| Library            | DatabaseLibrary                 |

Please note that at the library itself there's no JDBC drivers included, so also DB-specific driver needs to be added as a dependency.
Test build e.g. uses H2 driver, and it's inclusion can be seen from [pom-file](https://github.com/MarketSquare/robotframework-dblibrary/blob/master/pom.xml).

Example
-------
Usage examples can be found at [Tests-folder](/src/test/robotframework/acceptance) for local usage. Remote usage is similar, just importing library with Remote library.
