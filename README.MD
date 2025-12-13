# Googol Search Engine

A distributed search engine built with **FastAPI**, **gRPC**, **WebSockets**, and a simple crawler system.

---

## Features

- Distributed indexing across multiple barrels (index servers)  
- Real-time system statistics via WebSockets  
- Search for words or pages  
- Automatic crawling and indexing of Hacker News URLs  
- AI-generated analysis of search results using **Groq**

---

## Architecture

**Components:**

- **Gateway**: Receives URLs, distributes them to barrels, tracks popular searches, and gathers stats.  
- **Barrels**: Index servers storing content and serving search queries.  
- **Server**: FastAPI application exposing HTTP routes and WebSocket endpoints for live stats.  
- **Robot**: Simple crawler fetching URLs, extracting text, and sending them to barrels.  
- **WebSockets**: Deliver live updates of system stats (active barrels, average search time, top searches).  
- **gRPC**: Handles communication between Gateway, Barrels, and Server for stats and indexing.

---

## Clone repository

1. **Clone the repository**

git clone https://github.com/yourusername/googol-search.git
cd googol-search

--- 

## Installation of dependencies

pip install -r requirements.txt

--- 

## Set environment variables

GROQ_API_KEY=your_api_key_here

---

## Start the System

On Windows, run the provided batch script:

./run_all.bat 

This will start:

    Gateway
    Multiple barrel instances
    Crawler robots
    FastAPI server

---

## Usage 

 Access the website provided on the windows terminal (that opened up) from server.py 
