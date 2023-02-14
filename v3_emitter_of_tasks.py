import pika
import sys
import webbrowser
import csv
import socket
import time

def offer_rabbitmq_admin_site():
    """Offer to open the RabbitMQ Admin website"""
    ans = input("Would you like to monitor RabbitMQ queues? y or n ")
    print()
    if ans.lower() == "y":
        webbrowser.open_new("http://localhost:15672/#/queues")
        print()

def send_message(host: str, queue_name: str, message: str):
    """
    Creates and sends a message to the queue each execution.
    This process runs and finishes.

    Parameters:
        host (str): the host name or IP address of the RabbitMQ server
        queue_name (str): the name of the queue
        message (str): the message to be sent to the queue
    """

    host = "localhost"
    port = 9999
    address_tuple = (host, port)

    socket_family = socket.AF_INET 

    socket_type = socket.SOCK_DGRAM 

    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) 

    use_file = open("tasks.csv", "r")

    reader = csv.reader(use_file, delimiter=',')

    for row in reader:
        input_file.read

        fstring_use = f"{row}"

        rabbit = fstring_use.encode()

        sock.sendto(rabbit, address_tuple)
        
        time.sleep(5)

        try:
            conn = pika.BlockingConnection(pika.ConnectionParameters(host))
            ch = conn.channel()
            ch.queue_declare(queue=queue_name, durable=True)
            ch.basic_publish(exchange="", routing_key=queue_name, body=MESSAGE)
            print(f" [x] Sent {MESSAGE}")
        except pika.exceptions.AMQPConnectionError as e:
            print(f"Error: Connection to RabbitMQ server failed: {e}")
            sys.exit(1)
        finally:
            conn.close()

if __name__ == "__main__":  
    show_offer = False
    offer_rabbitmq_admin_site(show_offer)
    message = " ".join(sys.argv[1:])  or "{message}"
    send_message("localhost","task_queue3",message)

