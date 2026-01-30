import asyncio
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
import uuid

from fastapi import FastAPI, HTTPException, WebSocket, WebSocketDisconnect, status
from pydantic import BaseModel, Field

MAX_HISTORY = 100  # Max messages to keep per topic for replay
MAX_SUBSCRIBER_QUEUE = 50  # Max messages per subscriber queue for backpressure


class MessagePayload(BaseModel):
  order_id: Optional[str] = None
  amount: Optional[str] = None
  currency: Optional[str] = None
  
  # Correct way to allow extra fields in Pydantic v2
  model_config = {"extra": "allow"}


class Message(BaseModel):
  id: str
  payload: Dict[str, Any]


class ClientMessage(BaseModel):
  type: str
  topic: Optional[str] = None
  message: Optional[Message] = None
  client_id: Optional[str] = None
  last_n: Optional[int] = 0
  request_id: Optional[str] = None


class Topic:

  def __init__(self, name: str):
    self.name = name
    self.subscribers: Set[WebSocket] = set()
    self.message_history: deque[Dict[str, Any]] = deque(maxlen=MAX_HISTORY)
    self.lock = asyncio.Lock()
    self.subscriber_queues: Dict[WebSocket, asyncio.Queue] = {}
    self.message_count = 0

  async def add_subscriber(self, websocket: WebSocket, last_n: int = 0):
    async with self.lock:
      if websocket not in self.subscribers:
        self.subscribers.add(websocket)
        self.subscriber_queues[websocket] = asyncio.Queue(
            maxsize=MAX_SUBSCRIBER_QUEUE
        )
        # Start a task to send messages from the queue to the websocket
        asyncio.create_task(self._send_messages_to_subscriber(websocket))

        if last_n > 0:
          history = list(self.message_history)
          num_to_replay = min(last_n, len(history))
          for i in range(len(history) - num_to_replay, len(history)):
            await self._enqueue_message(websocket, history[i])

  async def remove_subscriber(self, websocket: WebSocket):
    async with self.lock:
      self.subscribers.discard(websocket)
      queue = self.subscriber_queues.pop(websocket, None)
      if queue:
        # Clear the queue to release any blocked puts
        while not queue.empty():
          queue.get_nowait()
          queue.task_done()

  async def publish_message(self, message: Dict[str, Any]):
    async with self.lock:
      self.message_history.append(message)
      self.message_count += 1
      for ws in list(self.subscribers):  # Iterate over a copy
        await self._enqueue_message(ws, message)

  async def _enqueue_message(
      self, websocket: WebSocket, message: Dict[str, Any]
  ):
    queue = self.subscriber_queues.get(websocket)
    if not queue:
      return

    try:
      if queue.full():
        # Backpressure: Drop the oldest message
        try:
          queue.get_nowait()
          queue.task_done()
          print(
              "Warning: SLOW_CONSUMER - Dropped oldest message for client on"
              f" topic {self.name}"
          )
          # Optionally send SLOW_CONSUMER error to client
          error_msg = {
              "type": "error",
              "topic": self.name,
              "error": {
                  "code": "SLOW_CONSUMER",
                  "message": (
                      "Subscriber queue overflow, oldest message dropped."
                  ),
              },
              "ts": datetime.now(timezone.utc).isoformat(),
          }
          # Try sending error without blocking, or skip if ws is busy
          try:
            await asyncio.wait_for(websocket.send_json(error_msg), timeout=0.1)
          except asyncio.TimeoutError:
            print(
                "Warning: Could not send SLOW_CONSUMER error to client on"
                f" topic {self.name} without blocking."
            )
          except WebSocketDisconnect:
            await self.remove_subscriber(websocket)
          except Exception as e:
            print(f"Error sending SLOW_CONSUMER message: {e}")

        except asyncio.QueueEmpty:
          pass  # Should not happen if full

      queue.put_nowait(message)
    except asyncio.QueueFull:
      # This should ideally not be reached due to the check above
      print(f"Error: Queue full even after dropping for {self.name}")
    except Exception as e:
      print(f"Error enqueuing message for {self.name}: {e}")
      await self.remove_subscriber(websocket)

  async def _send_messages_to_subscriber(self, websocket: WebSocket):
    queue = self.subscriber_queues.get(websocket)
    if not queue:
      return

    try:
      while websocket in self.subscribers:
        message = await queue.get()
        try:
          await websocket.send_json(message)
          queue.task_done()
        except WebSocketDisconnect:
          break
        except Exception as e:
          print(
              f"Error sending message to subscriber on topic {self.name}: {e}"
          )
          # Potentially break or remove subscriber if send fails repeatedly
          break
    except asyncio.CancelledError:
      print(f"Sender task for {self.name} cancelled.")
    finally:
      await self.remove_subscriber(websocket)

  def get_stats(self):
    return {
        "messages": self.message_count,
        "subscribers": len(self.subscribers),
    }


topics: Dict[str, Topic] = {}
topics_lock = asyncio.Lock()


async def get_topic(topic_name: str) -> Optional[Topic]:
  async with topics_lock:
    return topics.get(topic_name)


async def create_topic(topic_name: str) -> Topic:
  async with topics_lock:
    if topic_name in topics:
      raise HTTPException(
          status_code=status.HTTP_409_CONFLICT,
          detail=f"Topic '{topic_name}' already exists",
      )
    topic = Topic(topic_name)
    topics[topic_name] = topic
    return topic


async def delete_topic(topic_name: str):
  async with topics_lock:
    topic = topics.pop(topic_name, None)
    if not topic:
      raise HTTPException(
          status_code=status.HTTP_404_NOT_FOUND,
          detail=f"Topic '{topic_name}' not found",
      )

    # Notify subscribers and close connections
    async with topic.lock:
      for ws in list(topic.subscribers):
        info_msg = {
            "type": "info",
            "topic": topic_name,
            "msg": "topic_deleted",
            "ts": datetime.now(timezone.utc).isoformat(),
        }
        try:
          await ws.send_json(info_msg)
          await ws.close()
        except Exception:
          pass  # Ignore errors on close
      topic.subscribers.clear()
      for queue in topic.subscriber_queues.values():
        while not queue.empty():
          queue.get_nowait()
          queue.task_done()
      topic.subscriber_queues.clear()


# --- Heartbeat Task ---
async def heartbeat():
  while True:
    try:
      await asyncio.sleep(30)  # Send heartbeat every 30 seconds
      async with topics_lock:
        all_subscribers = set()
        for topic in topics.values():
          async with topic.lock:
            all_subscribers.update(topic.subscribers)

      info_msg = {
          "type": "info",
          "msg": "ping",
          "ts": datetime.now(timezone.utc).isoformat(),
      }
      for ws in list(all_subscribers):  # Iterate over a copy
        try:
          await asyncio.wait_for(ws.send_json(info_msg), timeout=1.0)
        except WebSocketDisconnect:
          # The main loop will handle removal
          pass
        except asyncio.TimeoutError:
          print("Heartbeat send timeout")
        except Exception as e:
          print(f"Error sending heartbeat: {e}")
    except asyncio.CancelledError:
      print("Heartbeat task cancelled.")
      break


@asynccontextmanager
async def lifespan(app: FastAPI):
  # Startup
  print("Starting up...")
  heartbeat_task = asyncio.create_task(heartbeat())
  yield
  # Shutdown
  print("Shutting down...")
  heartbeat_task.cancel()
  try:
    await heartbeat_task
  except asyncio.CancelledError:
    pass
  async with topics_lock:
    for topic_name in list(topics.keys()):
      await delete_topic(topic_name)
  print("Shutdown complete.")


app = FastAPI(lifespan=lifespan)


# --- WebSocket Endpoint ---
@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket):
  await websocket.accept()
  client_subscriptions: Set[str] = set()

  try:
    while True:
      try:
        data = await websocket.receive_json()
        msg = ClientMessage(**data)
      except Exception as e:
        await send_error(websocket, f"Invalid JSON format: {e}", "BAD_REQUEST")
        continue

      request_id = msg.request_id
      topic_name = msg.topic

      if msg.type == "ping":
        await websocket.send_json({
            "type": "pong",
            "request_id": request_id,
            "ts": datetime.now(timezone.utc).isoformat(),
        })
        continue

      if not topic_name and msg.type in ["subscribe", "unsubscribe", "publish"]:
        await send_error(
            websocket, "Missing 'topic'", "BAD_REQUEST", request_id
        )
        continue

      topic = await get_topic(topic_name)

      if not topic and msg.type in ["subscribe", "publish"]:
        await send_error(
            websocket,
            f"Topic '{topic_name}' not found",
            "TOPIC_NOT_FOUND",
            request_id,
            topic=topic_name,
        )
        continue

      try:
        if msg.type == "subscribe":
          if not msg.client_id:
            await send_error(
                websocket,
                "Missing 'client_id' for subscribe",
                "BAD_REQUEST",
                request_id,
                topic=topic_name,
            )
            continue
          await topic.add_subscriber(websocket, msg.last_n)
          client_subscriptions.add(topic_name)
          await send_ack(websocket, request_id, topic_name, "ok")

        elif msg.type == "unsubscribe":
          if not msg.client_id:
            await send_error(
                websocket,
                "Missing 'client_id' for unsubscribe",
                "BAD_REQUEST",
                request_id,
                topic=topic_name,
            )
            continue
          if topic:
            await topic.remove_subscriber(websocket)
          client_subscriptions.discard(topic_name)
          await send_ack(websocket, request_id, topic_name, "ok")

        elif msg.type == "publish":
          if not msg.message:
            await send_error(
                websocket,
                "Missing 'message' for publish",
                "BAD_REQUEST",
                request_id,
                topic=topic_name,
            )
            continue
          try:
            # Validate message ID
            uuid.UUID(msg.message.id)
          except ValueError:
            await send_error(
                websocket,
                "message.id must be a valid UUID",
                "BAD_REQUEST",
                request_id,
                topic=topic_name,
            )
            continue

          event = {
              "type": "event",
              "topic": topic_name,
              "message": msg.message.model_dump(),
              "ts": datetime.now(timezone.utc).isoformat(),
          }
          await topic.publish_message(event)
          await send_ack(websocket, request_id, topic_name, "ok")

        else:
          await send_error(
              websocket,
              f"Unknown message type '{msg.type}'",
              "BAD_REQUEST",
              request_id,
          )

      except HTTPException as e:  # Catch errors from topic operations
        await send_error(
            websocket, e.detail, "BAD_REQUEST", request_id, topic=topic_name
        )

  except WebSocketDisconnect:
    print("Client disconnected")
  except Exception as e:
    print(f"Error in WebSocket handler: {e}")
    await send_error(websocket, "Internal server error", "INTERNAL")
  finally:
    # Cleanup subscriptions on disconnect
    for topic_name in client_subscriptions:
      topic = await get_topic(topic_name)
      if topic:
        await topic.remove_subscriber(websocket)
    print("Cleaned up client subscriptions")


async def send_error(
    websocket: WebSocket,
    message: str,
    code: str,
    request_id: Optional[str] = None,
    topic: Optional[str] = None,
):
  error_msg = {
      "type": "error",
      "request_id": request_id,
      "error": {"code": code, "message": message},
      "ts": datetime.now(timezone.utc).isoformat(),
  }
  if topic:
    error_msg["topic"] = topic
  try:
    await websocket.send_json(error_msg)
  except Exception:
    pass  # Ignore send errors if client disconnected


async def send_ack(
    websocket: WebSocket, request_id: Optional[str], topic: str, status_msg: str
):
  ack_msg = {
      "type": "ack",
      "request_id": request_id,
      "topic": topic,
      "status": status_msg,
      "ts": datetime.now(timezone.utc).isoformat(),
  }
  try:
    await websocket.send_json(ack_msg)
  except Exception:
    pass


# --- REST Endpoints ---
class CreateTopicRequest(BaseModel):
  name: str


@app.post("/topics", status_code=status.HTTP_201_CREATED)
async def http_create_topic(request: CreateTopicRequest):
  topic_name = request.name
  if not topic_name:
    raise HTTPException(
        status_code=status.HTTP_400_BAD_REQUEST,
        detail="Topic name cannot be empty",
    )
  await create_topic(topic_name)
  return {"status": "created", "topic": topic_name}


@app.delete("/topics/{name}", status_code=status.HTTP_200_OK)
async def http_delete_topic(name: str):
  await delete_topic(name)
  return {"status": "deleted", "topic": name}


@app.get("/topics")
async def http_list_topics():
  async with topics_lock:
    topic_list = [
        {"name": name, "subscribers": len(topic.subscribers)}
        for name, topic in topics.items()
    ]
  return {"topics": topic_list}


start_time = datetime.now(timezone.utc)


@app.get("/health")
async def http_health():
  uptime = (datetime.now(timezone.utc) - start_time).total_seconds()
  async with topics_lock:
    num_topics = len(topics)
    num_subscribers = sum(len(topic.subscribers) for topic in topics.values())
  return {
      "uptime_sec": int(uptime),
      "topics": num_topics,
      "subscribers": num_subscribers,
  }


@app.get("/stats")
async def http_stats():
  async with topics_lock:
    stats_data = {name: topic.get_stats() for name, topic in topics.items()}
  return {"topics": stats_data}


if __name__ == "__main__":
  import uvicorn

  uvicorn.run(app, host="0.0.0.0", port=8000)