@echo off
start cmd /k "py gateway.py"

start cmd /k "py indexServer.py --port 8081"
start cmd /k "py indexServer.py --port 8082"
start cmd /k "py indexServer.py --port 8083"

start cmd /k "py robot.py"
start cmd /k "py robot.py"

start cmd /k "py client.py"