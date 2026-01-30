import asyncio
import websockets
import sys

async def listen():
    url = "ws://127.0.0.1:8000/ws"
    async with websockets.connect(url) as websocket:
        print(f"Connected to {url}")
        print("Type your JSON message and press Enter.")
        
        # Task to read messages from the server
        async def receive_messages():
            try:
                while True:
                    message = await websocket.recv()
                    print(f"\n< Received: {message}")
            except websockets.exceptions.ConnectionClosed:
                print("Connection closed by server")

        # Start the receiver task
        receiver = asyncio.create_task(receive_messages())

        # Main loop to read input from you and send to server
        loop = asyncio.get_event_loop()
        while True:
            # We use a non-blocking input method
            msg = await loop.run_in_executor(None, sys.stdin.readline)
            if not msg:
                break
            await websocket.send(msg.strip())

        receiver.cancel()

if __name__ == "__main__":
    try:
        asyncio.run(listen())
    except KeyboardInterrupt:
        print("\nExiting...")