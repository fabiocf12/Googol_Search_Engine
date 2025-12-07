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
try:
    channel = grpc.insecure_channel(f"{gateway_host}:{gateway_port}")
    stub = index_pb2_grpc.GatewayStub(channel)
except Exception as e:
    print(e)

print(f"Connected to Gateway at {gateway_host}:{gateway_port}")

#--

@app.get("/") # home route
def read_index():
    return FileResponse(os.path.join(static_dir, "index.html"))

@app.get("/index")
def index_func(value: str):
    try:
        stub.putNew(index_pb2.PutNewRequest(url=value))
    except:
        pass
    return PlainTextResponse(f"Submitted URL: {value} to Gateway")

@app.get("/search")
def search_func(value: str):
    words = value.lower()
    words_list = words.split(" ")
    
    try:
        result = stub.searchWord(index_pb2.SearchWordRequest(words=words_list))
        results = result.results

        send_back = ""
        if not results:
            send_back = "Nothing found!"
        else:
            i = 0 # for test only, needs a way to send more results as needed, possibly use session storage
            group = results[i:i+10]
            send_back = send_back + f"\n--- Results {i+1} to {i+len(group)}---"
            
            for r in group:
                send_back = send_back + f"• Title - {r.title}\n  URL - {r.url}\n  Snippet - {r.snippet}\n"
            
            if i + 10 < len(results):
                send_back = send_back + "Click enter to see more..."

    except Exception as e: # sends back error, for debug
        send_back = str(e)

    return PlainTextResponse(send_back)

@app.get("/page")
def page_func(value: str):
    send_back = ""
    try:
        result = stub.searchPage(index_pb2.SearchPageRequest(url=value))
        send_back = f"got result: \n{result}"
    except Exception as e:
        send_back = str(e)
    return PlainTextResponse(send_back)
