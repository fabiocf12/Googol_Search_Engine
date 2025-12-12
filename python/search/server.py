import os, json, asyncio, threading, requests
from concurrent.futures import ThreadPoolExecutor
from concurrent import futures
from typing import Set

from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request
from fastapi import Form, WebSocket
from starlette.websockets import WebSocketDisconnect

from dotenv import load_dotenv
from bs4 import BeautifulSoup
from groq import Groq

import grpc
from google.protobuf import empty_pb2
import index_pb2, index_pb2_grpc

#----------------------------
# Initial Config
#----------------------------
load_dotenv()
executor = ThreadPoolExecutor(max_workers=1)

with open("config.json") as f:
    config = json.load(f)

gateway_host = config["gateway"]["host"]
gateway_port = config["gateway"]["port"]
host = config["server"]["host"]
port = config["server"]["port"]
FRONTEND_HOST = config["frontend"]["host"]
FRONTEND_PORT = config["frontend"]["port"]


#Config Groq client for IA analysis
groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))


# Create a gRPC channel and client to gateway
try:
    channel = grpc.insecure_channel(f"{gateway_host}:{gateway_port}")
    stub = index_pb2_grpc.GatewayStub(channel)
    print(f"Connected to Gateway at {gateway_host}:{gateway_port}")
except Exception as e:
    print(e)


loop = asyncio.get_event_loop()  # main FastAPI loop

#-----------------
# FastAPI() Setup
#-----------------
app = FastAPI()
templates = Jinja2Templates(directory="templates")
static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")

#------------------------------
# gRPC Server (ServerServicer)
#------------------------------
class ServerServicer(index_pb2_grpc.ServerServicer):
    def pushSystemStats(self, request, context): #schedule async broadcast on main loop
        print("Server : got stats")
        loop.call_soon_threadsafe(asyncio.create_task, broadcast(request))
        return empty_pb2.Empty()

def start_grpc_server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    index_pb2_grpc.add_ServerServicer_to_server(ServerServicer(), server)
    server.add_insecure_port(f"{host}:{port}")
    server.start()
    server.wait_for_termination()

threading.Thread(target=start_grpc_server, daemon=True).start()

# ----------------------------
# WebSocket management
# ----------------------------
connected_clients: Set[WebSocket] = set()

last_stats = None

async def broadcast(result):
    
    global last_stats
    last_stats = result
    
    barrels_dict = [
        {"port": b.port, "num_entries": b.num_entries, "avg_search_time": b.avg_search_time}
            for b in result.barrels if b.num_entries != -1]
    top_searches = list(result.top_searches)

    msg = json.dumps({"barrels": barrels_dict, "top_searches": top_searches})
    
    for client in connected_clients.copy():
        try:
            print("Server : sending stats")
            await client.send_text(msg)
        except:
            connected_clients.discard(client)
            print("failed to send to client",client)


@app.websocket("/my-websocket")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    connected_clients.add(ws)
    
    if last_stats:
        print(f"vou enviar laststats ao client {ws}")
        await ws.send_text(json.dumps({
            "barrels": [{"port": b.port, "num_entries": b.num_entries, "avg_search_time": b.avg_search_time} for b in last_stats.barrels if b.num_entries != -1],
            "top_searches": list(last_stats.top_searches)
        }))
        
    try:
        while True:
            await asyncio.sleep(1)  # keep loop alive for broadcast
    except WebSocketDisconnect:
        pass
    finally:
        connected_clients.discard(ws)
        print("client disconnected", ws)
    
    
# ----------------------------
# HTTP routes
# ----------------------------
@app.get("/",response_class=HTMLResponse) # home route
def read_index(request: Request):
    return templates.TemplateResponse("index.html",{"request":request,"ws_host": FRONTEND_HOST,"ws_port": FRONTEND_PORT})


@app.get("/index",response_class=HTMLResponse)
def index_func(request : Request, value: str):
    
    try:
        stub.putNew(index_pb2.PutNewRequest(url=value))
        message = f"Submitted URL: {value} to Gateway"
        
    except Exception as e:
        message = str(e)
        return templates.TemplateResponse("error.html", {"request": request, "message": message,"ws_host": FRONTEND_HOST,"ws_port": FRONTEND_PORT})

    return templates.TemplateResponse("message.html", {"request": request, "message": message,"ws_host": FRONTEND_HOST,"ws_port": FRONTEND_PORT})


@app.get("/search",response_class=HTMLResponse)
def search_func(request: Request , value: str , page: int = 1):
    words = value.lower()
    words_list = words.split(" ")
    error_message = None
    
    try:
        result = stub.searchWord(index_pb2.SearchWordRequest(words=words_list))
        results = result.results
        
        if not results:  #check if barrels are offline or the actual search didnt lead omse
        
            stats = stub.getSystemStats(empty_pb2.Empty())
            if all(b.num_entries == -1 for b in stats.barrels):
                return templates.TemplateResponse("error.html", {"request": request, "error": "System unavailable","ws_host": FRONTEND_HOST,"ws_port": FRONTEND_PORT})    
            
    except Exception as e: # sends back error, for debug
        error_message = str(e)
        return templates.TemplateResponse("error.html",{"request": request, "error": error_message,"ws_host": FRONTEND_HOST,"ws_port": FRONTEND_PORT})
    
    # Page by page
    per_page = 10
    start = (page - 1) * per_page
    end = start + per_page
    paginated_results = results[start:end]
    
    if len(results) % per_page > 0:
        total_pages = len(results) // per_page + 1
    else:
        total_pages = len(results)// per_page
    
    analysis = generate_analysis(value, results)
    
    return templates.TemplateResponse("results.html",{
        "request": request,
        "results": paginated_results,
        "query": value,
        "error": error_message, 
        "page": page,
        "total_pages": total_pages,
        "analysis": analysis,
        "ws_host": FRONTEND_HOST,
        "ws_port": FRONTEND_PORT
    })


@app.get("/page",response_class=HTMLResponse)
def page_func(request: Request, value: str):
    
    try:
        result = stub.searchPage(index_pb2.SearchPageRequest(url=value))
        urls = result.urls
        
        if not urls:  #check if barrels are offline or the actual search didnt lead omse
            stats = stub.getSystemStats(empty_pb2.Empty())
            if all(b.num_entries == -1 for b in stats.barrels):
                return templates.TemplateResponse("error.html", {"request": request, "error": "System unavailable","ws_host": FRONTEND_HOST,"ws_port": FRONTEND_PORT})   
            
    except Exception as e:
        result = str(e)
        return templates.TemplateResponse("error.html",{"request": request, "error": result,"ws_host": FRONTEND_HOST,"ws_port": FRONTEND_PORT})
        
    return templates.TemplateResponse("page.html",{"request": request, "results": result.urls, "query":value,"ws_host": FRONTEND_HOST,"ws_port": FRONTEND_PORT})


@app.post("/hackernews_index")
def hackernews_index(request: Request, query: str = Form(...)):
    
    try:
        top_ids = requests.get("https://hacker-news.firebaseio.com/v0/topstories.json").json()
    except Exception as e:
        return templates.TemplateResponse("message.html",{"request": request, "message": str(e),"ws_host": FRONTEND_HOST,"ws_port": FRONTEND_PORT})

    threading.Thread(target=index_hackernews_stories, args=(top_ids, query,)).start()

    msg = f"{len(top_ids)} stories from Hacker News retrieved!"

    return templates.TemplateResponse("message.html",{"request": request, "message": msg,"ws_host": FRONTEND_HOST,"ws_port": FRONTEND_PORT})

# ----------------------------
# Aux functions
# ----------------------------
def generate_analysis(query: str, results):

    result_str = str([result.snippet + "\n\n" for result in results[:4]])
    prompt = f"O utilizador pesquisou por : {query}.\n\n Os resultados são {result_str}. Escreve uma análise contextualizada, resumindo as principais ideias!"
    
    try:
        response = groq_client.chat.completions.create(model="llama-3.1-8b-instant", messages = [{"role": "user", "content":prompt}])
    except:
        return "AI overview couldn't be reachedª"

    return response.choices[0].message.content


def index_hackernews_stories(top_ids, query):
    
    words = query.lower().split()

    for story_id in top_ids[:50]: 
        
        story = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json").json()
        url = story.get("url")
        
        if url:
            try:
                page = requests.get(url, timeout=5)
                soup = BeautifulSoup(page.text, "html.parser")
                text = soup.get_text().lower().split()
                
                found_all = all(word in text for word in words)
                
                if found_all:
                    print(f"{query} IS IN the link {url} and the id is {story_id}")
                    try:
                        stub.putNew(index_pb2.PutNewRequest(url=url))
                    except Exception as e:
                        print(e)
                else:
                    print(f"{query} IS NOT IN the link {url} ")
                        
            except Exception:
                continue