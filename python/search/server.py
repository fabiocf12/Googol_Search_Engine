from fastapi import FastAPI
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
import os

import grpc
from google.protobuf import empty_pb2
import index_pb2 as index_pb2
import index_pb2_grpc as index_pb2_grpc
import requests
import json

app = FastAPI()

static_dir = os.path.join(os.path.dirname(__file__), "static")

app.mount("/static", StaticFiles(directory=static_dir), name="static")

# grpc setup --

with open("config.json") as f:
    config = json.load(f)

gateway_host = config["gateway"]["host"]
gateway_port = config["gateway"]["port"]

# Create a gRPC channel and client
channel = grpc.insecure_channel(f"{gateway_host}:{gateway_port}")
stub = index_pb2_grpc.GatewayStub(channel)

print(f"Connected to Gateway at {gateway_host}:{gateway_port}")

#--

@app.get("/") # home route
def read_index():
    return FileResponse(os.path.join(static_dir, "index.html"))

@app.get("/index")
def button1(value: str):
    return PlainTextResponse(f"You clicked index with input: {value}")

@app.get("/search")
def button1(value: str):
    return PlainTextResponse(f"You clicked search with input: {value}")

@app.get("/page")
def button1(value: str):
    return PlainTextResponse(f"You clicked page with input: {value}")
