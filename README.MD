Googol Search Engine

A distributed search engine project built with FastAPI, gRPC, WebSockets, and a simple crawling mechanism.

Features

Distributed indexing across multiple barrels (index servers)

Real-time system statistics via WebSockets

Search for words or pages

Automatic crawling and indexing of URLs from Hacker News

AI-generated analysis of search results using Groq

Architecture

Gateway: Central coordinator; receives URLs, distributes them to barrels, tracks popular searches, and gathers stats.

Barrels: Index servers storing page content and serving search requests.

Server: FastAPI application exposing HTTP routes and WebSocket endpoints for real-time statistics.

Robot: Simple crawler fetching URLs, extracting text, and sending them to barrels.

WebSockets: Provide live updates of system stats (active barrels, average search time, top searches).

gRPC: Handles communication between Gateway, Barrels, and Server for stats and indexing.

Setup

Clone the repository:

git clone <repo_url>
cd <repo_folder>


Install dependencies:

pip install -r requirements.txt


Create a .env file with your Groq API key:

GROQ_API_KEY=your_api_key_here


Start the system:

On Windows, run the batch script:

start_system.bat


This will open multiple command windows and run:

gateway.py (distributes URLs)

indexServer.py instances (barrels)

robot.py instances (crawlers)

FastAPI server with Uvicorn (server:app)

Access the site:

http://localhost:8000

Usage

Index a URL: Use the form on the home page or the /index route.

Search: Use the /search route with a query.

Page lookup: Use the /page route with a URL.

Hacker News indexing: Submit a query via /hackernews_index to index relevant stories.

Real-time stats: View active barrels, average search times, and top searches via WebSockets (displayed on all pages).

Testing

Crawlers (robot.py) fetch URLs and push to barrels.

Barrels can be started/stopped individually for testing resilience.

WebSockets can be observed for live updates in the browser console.

AI-generated analysis is shown on the search results page.