import socket
import logging

logging.basicConfig(level=logging.DEBUG)

def test_kafka_connection():
    host = 'kafka'
    port = 29092
    
    try:
        # Try DNS resolution
        ip = socket.gethostbyname(host)
        logging.info(f"Successfully resolved {host} to {ip}")
        
        # Try socket connection
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(5)
        result = sock.connect_ex((ip, port))
        if result == 0:
            logging.info(f"Successfully connected to {host}:{port}")
        else:
            logging.error(f"Failed to connect to {host}:{port}, error code: {result}")
        sock.close()
        
    except Exception as e:
        logging.error(f"Error testing connection: {str(e)}")

if __name__ == "__main__":
    test_kafka_connection() 