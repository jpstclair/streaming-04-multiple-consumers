import pika
import sys
import time

def callback(ch, method, properties, body):
    """ Define behavior on getting a message."""
    print(f" [x] Received {body.decode()}")
    time.sleep(body.count(b".")
    print(" [x] Done.")
    ch.basic_ack(delivery_tag=method.delivery_tag)


def main(hn: str = "localhost", qn: str = "task_queue3"):
    """ Continuously listen for task messages on a named queue."""

    try:
        connection = pika.BlockingConnection(pika.ConnectionParameters(host=hn))


    except Exception as e:
        print()
        print("ERROR: Connection to RabbitMQ Server failed.")
        print(f"Verify the server is running on host={hn}.")
        print(f"The error says: {e}")
        print()
        sys.exit(1)

    try:
        channel = connection.channel()

        channel.queue_declare(queue=qn, durable=True)

        channel.basic_qos(prefetch_count=1) 

        channel.basic_consume( queue=qn, on_message_callback=callback)

      
        print(" [*] Ready for work. To exit press CTRL+C")

        channel.start_consuming()
  
    except Exception as e:
        print()
        print("ERROR: something went wrong.")
        print(f"The error says: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print()
        print(" User interrupted continuous listening process.")
        sys.exit(0)
    finally:
        print("\nClosing connection. Goodbye.\n")
        connection.close()



if __name__ == "__main__":
    main("localhost", "task_queue3")