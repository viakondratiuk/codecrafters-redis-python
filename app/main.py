import socket


def main():    
    print("Logs from your program will appear here!")

    pong = "+PONG\r\n"
    
    server_socket = socket.create_server(("localhost", 6379), reuse_port=True)
    conn, addr = server_socket.accept()

    with conn:
        conn.recv(1024)
        conn.send(pong.encode())
    


if __name__ == "__main__":
    main()
