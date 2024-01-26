#local web socket to stream data in chunks from review dataset
import json
import socket
import time
import pandas as pd



def send_data_over_socket(file_path, host='127.0.0.1', port=9999, chunk_size=2):
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((host, port))
    s.listen(1)
    print(f"Listening on port {host}:{port}...")


    
    last_sent_idx = 0
    stream_interval = 5
    while True:
        #Fixed reconnection bug by moving connection establishment to the loop
        connection, address = s.accept()
        print(f"Connection from {address} has been established!")
        try:
            with open(file_path, 'r') as f:
                for _ in range(last_sent_idx):
                    next(f)
                records = []
                for line in f:
                    #send until chunk size is reached
                    records.append(json.loads(line))
                    if len(records) == chunk_size:
                        current_chunk = pd.DataFrame(records)
                        print(current_chunk)
                        #send data over socket once full chunk
                        for record in current_chunk.to_dict(orient='records'):
                            serialized_record = json.dumps(record).encode('utf-8')
                            #somehow need add new line RIP so server knows need to send to client
                            connection.send(serialized_record + b'\n')
                            time.sleep(stream_interval)
                            last_sent_idx += 1
                        records = []
        except (BrokenPipeError, ConnectionResetError) as e:
            print("Connection was closed by the client!")
        finally:
            connection.close()
            print("Connection closed!")


if __name__ == '__main__':
    send_data_over_socket('datasets/yelp_academic_dataset_review.json')