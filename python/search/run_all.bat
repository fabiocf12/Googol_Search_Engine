@echo off
start cmd /k "py -3.12 gateway.py"

start cmd /k "py -3.12 indexServer.py 0"
start cmd /k "py -3.12 indexServer.py 1"
start cmd /k "py -3.12 indexServer.py 2"

start cmd /k "py -3.12 robot.py"
start cmd /k "py -3.12 robot.py"

start cmd /k "py -3.12 client.py"