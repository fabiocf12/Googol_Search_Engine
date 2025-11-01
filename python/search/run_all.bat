@echo off
start cmd /k "py -3.13 gateway.py"
start cmd /k "py -3.13 indexServer.py --port 8081"
start cmd /k "py -3.13 indexServer.py --port 8082"
start cmd /k "py -3.13 indexServer.py --port 8083"

start cmd /k "py -3.13 robot.py"
start cmd /k "py -3.13 robot.py"
