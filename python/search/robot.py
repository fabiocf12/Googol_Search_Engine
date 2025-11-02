import grpc
from google.protobuf import empty_pb2
import index_pb2 as index_pb2
import index_pb2_grpc as index_pb2_grpc
import requests
from bs4 import BeautifulSoup as jsoup
import json

from urllib.parse import urljoin, urlparse
import os

def add_callback(future, link, url, barrel, attempts):
    future.add_done_callback(lambda fut: on_ack(fut, link, url, barrel, attempts))


def on_ack(fut, link, url, barrel, attempts):
    if attempts >= 6:
        return
    try:
        result = fut.result()  # raises if RPC failed
        #print(f"ACK received for link={link} from url={url}")
        # cancel timer, mark success, etc.
    except grpc.RpcError as e:
        print(f"RPC failed for link={link} from url={url}. Attempts: {attempts}")
        # retry or handle error

        future = barrel.addToIndexPage.future(
            index_pb2.AddToIndexRequestPage(url_pointed=link, url_that_points=url),
            timeout=5.0  # seconds
        )
        add_callback(future, link, url, barrel, attempts + 1)

    
def run():
    
    with open("config.json") as f:
        config = json.load(f)

    gateway_host = config["gateway"]["host"]
    gateway_port = config["gateway"]["port"]
    
    # Create a gRPC channel and client to gateway
    channel_gateway = grpc.insecure_channel(f"{gateway_host}:{gateway_port}")
    stub_gateway = index_pb2_grpc.GatewayStub(channel_gateway)

    barrels  = []
    for b in config["barrels"]: # and one for communicating with the barrels
        channel_barrel = grpc.insecure_channel(f"{b['host']}:{b['port']}")
        stub_barrel = index_pb2_grpc.IndexStub(channel_barrel)
        barrels.append(stub_barrel)
    
    while True:
        try:
            try:
                response = stub_gateway.takeNext(empty_pb2.Empty())
                url = response.url
                print(f"Received URL: {url}")
                
                try:
                    # Fetch webpage using requests and parse with BeautifulSoup
                    response = requests.get(url)
                    response.raise_for_status()  # Raise an exception for bad status codes
                    soup = jsoup(response.text, 'html.parser')

                    link_list = soup.select("a[href]")
                    abs_links = []
                    for link in link_list:
                        link = urljoin(url, link["href"])
                        #print("LINK:", link)

                        path = urlparse(link).path.lower()
                        if os.path.splitext(path)[1] in {".zip", ".tar", ".gz", ".xz", ".pdf", ".jpg", ".jpeg", ".png", ".gif", ".mp4", ".exe", ".iso", ".sign", ".bz2"}:
                            #print("Skipping file type:", link)
                            continue
                        
                        for stub_barrel in barrels:
                            future = stub_barrel.addToIndexPage.future(
                                index_pb2.AddToIndexRequestPage(url_pointed=link, url_that_points=url),
                                timeout=5.0  # seconds
                            )
                            attempts = 1
                            future.add_done_callback(lambda fut, l=link, u=url, b=stub_barrel, a=attempts:
                                                                            on_ack(fut, l, u, b, a))
                        abs_links.append(link)
                        
                    
                    page_title = soup.title.string if soup.title else "No title"
                    page_text = soup.get_text()
                    page_text_tokenized = page_text.split()
                    page_text_tokenized = [word.lower() for word in page_text_tokenized]    
                    page_snippet = " ".join(page_text_tokenized[:40])     

                    for link in abs_links:
                        stub_gateway.putNew(index_pb2.PutNewRequest(url=link))

                    for stub_barrel in barrels:
                        future = stub_barrel.addToIndex.future(
                            index_pb2.AddToIndexRequest(url=url,words=page_text_tokenized,
                                                        title=page_title,
                                                        snippet=page_snippet),timeout=5.0  # seconds
                                                        )
                        attempts = 1
                        future.add_done_callback(lambda fut, l=link, u=url, b=stub_barrel, a=attempts:
                                                on_ack(fut, l, u, b, a))
                except requests.RequestException as e:
                    print(f"Error fetching webpage: {e}")
                
            except grpc.RpcError as e:
                print(f"RPC failed: {e.code()}")
                print(f"RPC error details: {e.details()}")
                raise
                    
        except KeyboardInterrupt:
            print("\nStopping the robot...")
            break
        
if __name__ == '__main__':
    print("I am a robot")
    run()
