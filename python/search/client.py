import grpc
from google.protobuf import empty_pb2
import index_pb2 as index_pb2
import index_pb2_grpc as index_pb2_grpc
import requests

def run():
    # Create a gRPC channel
    channel = grpc.insecure_channel('localhost:8185')
    
    # Create a stub (client)
    stub = index_pb2_grpc.GatewayStub(channel)

    try:
        while True:
            link_to_idx = input("Link to index (or q to exit):")
            if link_to_idx == 'q':
                break
            else:
                stub.putNew(index_pb2.PutNewRequest(url=link_to_idx))
                print(f"Submitted URL: {link_to_idx} to Gateway")
            
                
    except KeyboardInterrupt:
        print("\nStopping the robot...")


if __name__ == '__main__':
    run()
