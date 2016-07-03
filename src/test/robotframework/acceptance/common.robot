*** Settings ***
Library           DatabaseLibrary
Library           Process
Library           DateTime

*** Keywords ***
Start H2 server
    Start Process    java    -cp    ${maven.test.classpath}    org.h2.tools.Server    -tcp    -tcpAllowOthers
    Connect To Database    org.h2.Driver    jdbc:h2:tcp://127.0.0.1:9092/~/robotTest;DB_CLOSE_DELAY=-1    sa    ${EMPTY}

Stop H2 server
    Disconnect From Database
    Terminate Process
    ${stdout}    ${stderr}    Get Process Result    stdout=yes    stderr=yes
    Log    ${stdout}    DEBUG
    Log    ${stderr}    DEBUG
