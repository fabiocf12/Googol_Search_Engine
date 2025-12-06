@echo off
start cmd /k "py gateway.py"

start cmd /k "py indexServer.py 0"
start cmd /k "py indexServer.py 1"
start cmd /k "py indexServer.py 2"

start cmd /k "py robot.py"
start cmd /k "py robot.py"

start cmd /k "py client.py"