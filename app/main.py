import socket


def main():    
    print("Logs from your program will appear here!")

    pong = "+PONG\r\n"
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, addr = server_socket.accept()

    while True:
        try:
            data = conn.recv(1024)
            conn.send(pong.encode())
        except Exception as e:
            print(f"Got error: {e}")        
    
    # conn.close()
    # server_socket.close()

if __name__ == "__main__":
    main()
