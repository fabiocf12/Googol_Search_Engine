from fastapi import FastAPI
from fastapi.responses import FileResponse, PlainTextResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from fastapi import Request
from fastapi import Form

import os
from groq import Groq

import grpc
from google.protobuf import empty_pb2
import index_pb2 as index_pb2
import index_pb2_grpc as index_pb2_grpc
import requests
import json

from dotenv import load_dotenv

from bs4 import BeautifulSoup

app = FastAPI()

templates = Jinja2Templates(directory="templates")

static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/static", StaticFiles(directory=static_dir), name="static")

# Load variables from .env
load_dotenv()

#Config Groq client
groq_client = Groq(api_key=os.getenv("GROQ_API_KEY"))

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



@app.get("/",response_class=HTMLResponse) # home route
def read_index(request: Request):
    return templates.TemplateResponse("index.html",{"request":request})

@app.get("/index",response_class=HTMLResponse)
def index_func(request : Request, value: str):
    
    try:
        stub.putNew(index_pb2.PutNewRequest(url=value))
        message = f"Submitted URL: {value} to Gateway"
    except Exception as e:
        message = str(e)
        
    return templates.TemplateResponse("message.html", {"request": request, "message": message})

@app.get("/search",response_class=HTMLResponse)
def search_func(request: Request , value: str , page: int = 1):
    words = value.lower()
    words_list = words.split(" ")
    error_message = None
    
    try:
        result = stub.searchWord(index_pb2.SearchWordRequest(words=words_list))
        results = result.results
            
    except Exception as e: # sends back error, for debug
        error_message = str(e)
        
    # Page by page
    per_page = 10
    start = (page - 1) * per_page
    end = start + per_page
    paginated_results = results[start:end]
    
    if len(results) % per_page > 0:
        total_pages = len(results) // per_page + 1
    else:
        total_pages = len(results)// per_page
    
    analysis = generate_analysis(value)
    
    return templates.TemplateResponse("results.html",{"request": request, "results": paginated_results, "query": value,"error": error_message,  "page": page, "total_pages": total_pages,"analysis": analysis})

@app.get("/page",response_class=HTMLResponse)
def page_func(request: Request, value: str):
    
    try:
        result = stub.searchPage(index_pb2.SearchPageRequest(url=value))
    except Exception as e:
        result = str(e)
        
    return templates.TemplateResponse("page.html",{"request": request, "results": result.urls, "query":value})

def generate_analysis(query: str):
    
    prompt = f"O utilizador pesquisou por : {query}.\n\n Escreve uma análise contextualizada, resumindo as principais ideias!"
    response = groq_client.chat.completions.create(model="llama-3.1-8b-instant", messages = [{"role": "user", "content":prompt}])
    
    return response.choices[0].message.content


@app.post("/hackernews_index")
def hackernews_index(request: Request, query: str = Form(...)):
    
    try:
        top_ids = requests.get("https://hacker-news.firebaseio.com/v0/topstories.json").json()
        added = 0

        for story_id in top_ids[:50]: 
            
            story = requests.get(f"https://hacker-news.firebaseio.com/v0/item/{story_id}.json").json()
            url = story.get("url")
            
            if url:
                try:
                    page = requests.get(url, timeout=5)
                    soup = BeautifulSoup(page.text, "html.parser")
                    text = soup.get_text().lower()
                    if query.lower() in text:
                        
                        try:
                            stub.putNew(index_pb2.PutNewRequest(url=url))
                        except Exception as e:
                            print(e)
                            
                        added += 1
                except Exception:
                    continue

        msg = f"{added} stories from Hacker News added to the indexation!"
    except Exception as e:
        msg = f"Error integrating Hacker News: {str(e)}"

    return templates.TemplateResponse("message.html",{"request": request, "message": msg})
