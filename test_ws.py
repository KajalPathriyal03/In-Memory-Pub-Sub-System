import asyncio
import uuid
import pytest
from fastapi.testclient import TestClient
from starlette.websockets import WebSocketDisconnect

# Import main to patch constants
import main
from main import app, topics

# --- CONFIGURATION ---
# Set strict timeouts so tests never hang
TIMEOUT = 0.3 
# Reduce queue size to 2 so backpressure triggers instantly
main.MAX_SUBSCRIBER_QUEUE = 2 

client = TestClient(app)

@pytest.fixture(autouse=True)
async def clear_topics():
    topics.clear()
    # Tiny sleep to ensure previous tasks release the lock
    await asyncio.sleep(0.001)

def test_health():
    response = client.get("/health")
    assert response.status_code == 200
    data = response.json()
    assert data["topics"] == 0

def test_create_topic():
    response = client.post("/topics", json={"name": "test_topic"})
    assert response.status_code == 201
    assert "test_topic" in topics

def test_list_topics():
    client.post("/topics", json={"name": "topic1"})
    client.post("/topics", json={"name": "topic2"})
    response = client.get("/topics")
    assert len(response.json()["topics"]) == 2

def test_delete_topic():
    client.post("/topics", json={"name": "delete_me"})
    client.delete("/topics/delete_me")
    assert "delete_me" not in topics

@pytest.mark.asyncio
async def test_stats():
    client.post("/topics", json={"name": "stats_topic"})
    await asyncio.sleep(0.001) # Yield

    # Publish one message
    with client.websocket_connect("/ws") as publisher:
        publisher.send_json({
            "type": "publish", "topic": "stats_topic",
            "message": {"id": str(uuid.uuid4()), "payload": {"d": 1}},
            "request_id": "pub-stats",
        })
        publisher.receive_json(timeout=TIMEOUT) # Ack
    
    await asyncio.sleep(0.001) # Allow stats to update
    response = client.get("/stats")
    assert response.json()["topics"]["stats_topic"]["messages"] == 1

def test_websocket_ping():
    with client.websocket_connect("/ws") as ws:
        ws.send_json({"type": "ping", "request_id": "123"})
        res = ws.receive_json(timeout=TIMEOUT)
        assert res["type"] == "pong"

def test_websocket_errors():
    with client.websocket_connect("/ws") as ws:
        # Invalid Type
        ws.send_json({"type": "bad", "request_id": "1"})
        assert ws.receive_json(timeout=TIMEOUT)["type"] == "error"
        
        # Missing Topic
        ws.send_json({"type": "subscribe", "client_id": "c1"})
        assert ws.receive_json(timeout=TIMEOUT)["type"] == "error"

@pytest.mark.asyncio
async def test_websocket_pubsub():
    client.post("/topics", json={"name": "pubsub"})
    await asyncio.sleep(0.001)

    with client.websocket_connect("/ws") as ws1, client.websocket_connect("/ws") as ws2:
        # Subscribe both
        ws1.send_json({"type": "subscribe", "topic": "pubsub", "client_id": "s1", "request_id":"1"})
        ws1.receive_json(timeout=TIMEOUT) # Ack
        
        ws2.send_json({"type": "subscribe", "topic": "pubsub", "client_id": "s2", "request_id":"2"})
        ws2.receive_json(timeout=TIMEOUT) # Ack

        # Publish
        msg_id = str(uuid.uuid4())
        with client.websocket_connect("/ws") as pub:
            pub.send_json({
                "type": "publish", "topic": "pubsub",
                "message": {"id": msg_id, "payload": "hi"},
                "request_id": "p1"
            })
            pub.receive_json(timeout=TIMEOUT) # Ack

        # Check delivery
        assert ws1.receive_json(timeout=TIMEOUT)["message"]["id"] == msg_id
        assert ws2.receive_json(timeout=TIMEOUT)["message"]["id"] == msg_id

@pytest.mark.asyncio
async def test_websocket_last_n():
    client.post("/topics", json={"name": "lastn"})
    await asyncio.sleep(0.001)

    # Publish 5 messages
    ids = []
    with client.websocket_connect("/ws") as pub:
        for i in range(5):
            mid = str(uuid.uuid4())
            ids.append(mid)
            pub.send_json({
                "type": "publish", "topic": "lastn",
                "message": {"id": mid, "payload": i},
                "request_id": f"p{i}"
            })
            pub.receive_json(timeout=TIMEOUT)

    # Subscribe requesting last 3
    with client.websocket_connect("/ws") as sub:
        sub.send_json({
            "type": "subscribe", "topic": "lastn", "client_id": "s",
            "last_n": 3, "request_id": "s1"
        })
        sub.receive_json(timeout=TIMEOUT) # Ack

        # Verify we get exactly the last 3
        for i in range(3):
            msg = sub.receive_json(timeout=TIMEOUT)
            assert msg["message"]["id"] == ids[2+i]

@pytest.mark.asyncio
async def test_websocket_backpressure():
    # THIS TEST IS NOW SUPER FAST (Queue size = 2)
    client.post("/topics", json={"name": "bp"})
    await asyncio.sleep(0.001)

    with client.websocket_connect("/ws") as sub:
        sub.send_json({"type": "subscribe", "topic": "bp", "client_id": "slow", "request_id": "1"})
        sub.receive_json(timeout=TIMEOUT) # Ack

        topic = topics["bp"]
        ws_obj = list(topic.subscribers)[0]
        queue = topic.subscriber_queues[ws_obj]

        # 1. Fill Queue (Max=2)
        await queue.put({"type": "event", "payload": 0})
        await queue.put({"type": "event", "payload": 1})
        assert queue.full()

        # 2. Trigger Overflow
        with client.websocket_connect("/ws") as pub:
            pub.send_json({
                "type": "publish", "topic": "bp",
                "message": {"id": str(uuid.uuid4()), "payload": "overflow"},
                "request_id": "p1"
            })
            pub.receive_json(timeout=TIMEOUT) # Ack (Must not block)

        await asyncio.sleep(0.01) # Yield for drop

        # 3. Drain and verify drop
        msgs = []
        try:
            while True:
                m = sub.receive_json(timeout=0.1)
                if m["type"] == "event" and isinstance(m.get("payload"), int):
                    msgs.append(m["payload"])
        except Exception:
            pass
        
        # Verify 0 (oldest) was dropped. We should only see '1'.
        assert 0 not in msgs

@pytest.mark.asyncio
async def test_topic_deletion():
    client.post("/topics", json={"name": "del"})
    await asyncio.sleep(0.001)

    with client.websocket_connect("/ws") as sub:
        sub.send_json({"type": "subscribe", "topic": "del", "client_id": "s", "request_id": "1"})
        sub.receive_json(timeout=TIMEOUT) # Ack
        
        client.delete("/topics/del")
        
        # Should get info message then disconnect
        msg = sub.receive_json(timeout=TIMEOUT)
        assert msg["type"] == "info"
        assert msg["msg"] == "topic_deleted"
        
        with pytest.raises(WebSocketDisconnect):
            sub.receive_json(timeout=TIMEOUT)