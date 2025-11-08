@echo off
start cmd /k "py -3.13 gateway.py"

start cmd /k "py -3.13 indexServer.py 0"
start cmd /k "py -3.13 indexServer.py 1"
start cmd /k "py -3.13 indexServer.py 2"

start cmd /k "py -3.13 robot.py"
start cmd /k "py -3.13 robot.py"

start cmd /k "py -3.13 client.py"