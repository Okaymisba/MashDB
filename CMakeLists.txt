cmake_minimum_required(VERSION 3.10)

project(MashDB LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

add_executable(MashDB
        src/main.cpp
        src/Creation/createDatabase.cpp
        src/Operations/Insertion/insert.cpp
        src/Operations/CurrentDB/currentDB.cpp
        src/Operations/ChangeDB/changeDB.cpp
        src/Parser/parser.cpp
        src/Creation/createTable.cpp
)