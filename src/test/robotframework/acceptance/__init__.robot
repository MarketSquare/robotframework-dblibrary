*** Settings ***
Suite Setup    Setup suite
Suite Teardown    Teardown suite
Library    Process

*** Variables ***
${PORT}              62022

*** Keywords ***
Setup suite
    ${process}    Start Process    java    -cp    ${maven.test.classpath}:${maven.runtime.classpath}    org.robotframework.remoteserver.RemoteServer
    ...    --library    DatabaseLibrary:/
    ...    --port    ${PORT}
    Sleep    3s

Teardown suite
    Run keyword and ignore error    Stop Remote Server
    Terminate Process
    Sleep    3s
    Process Should Be Stopped