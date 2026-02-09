import asyncio
import json
import os

from dotenv import load_dotenv
import websockets

load_dotenv()  # reads .env if present

rooms: dict[str, dict[str, websockets.WebSocketServerProtocol]] = {}


async def broadcast(room_id: str, message: dict, exclude: str | None = None):
    if room_id not in rooms:
        return

    dead_users = []
    text = json.dumps(message)

    for uid, ws in rooms[room_id].items():
        if exclude is not None and uid == exclude:
            continue
        try:
            await ws.send(text)
        except Exception:
            dead_users.append(uid)

    for uid in dead_users:
        rooms[room_id].pop(uid, None)
    if not rooms[room_id]:
        rooms.pop(room_id, None)


async def handle_client(websocket: websockets.WebSocketServerProtocol):
    room_id = None
    user_id = None

    try:
        async for raw in websocket:
            data = json.loads(raw)
            action = data.get("action")

            if action == "join":
                room_id = data.get("roomId")
                user_id = data.get("userId")
                if not room_id or not user_id:
                    continue

                rooms.setdefault(room_id, {})
                rooms[room_id][user_id] = websocket

                await websocket.send(json.dumps({
                    "type": "joined",
                    "roomId": room_id,
                    "userId": user_id,
                    "participants": len(rooms[room_id]),
                }))

                await broadcast(room_id, {
                    "type": "user_joined",
                    "userId": user_id,
                    "participants": len(rooms[room_id]),
                }, exclude=user_id)

            elif action in ("play", "pause", "seek", "timeupdate"):
                if not room_id or not user_id:
                    continue
                await broadcast(room_id, {
                    "type": action,
                    "currentTime": data.get("currentTime"),
                    "userId": user_id,
                }, exclude=user_id)

    finally:
        if room_id and user_id and room_id in rooms:
            rooms[room_id].pop(user_id, None)
            if rooms[room_id]:
                await broadcast(room_id, {
                    "type": "user_left",
                    "userId": user_id,
                    "participants": len(rooms[room_id]),
                })
            else:
                rooms.pop(room_id, None)


async def main():
    # Railway will inject PORT, default 8765 locally [web:82][web:94]
    port = int(os.environ.get("PORT", "8765"))
    print(f"WebSocket server on 0.0.0.0:{port}")
    async with websockets.serve(handle_client, "0.0.0.0", port):
        await asyncio.Future()


if __name__ == "__main__":
    asyncio.run(main())
