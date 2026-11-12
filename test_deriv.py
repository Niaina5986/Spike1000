import json
import time
import websocket

URL = "wss://api.derivws.com/trading/v1/options/ws/public"

print("Connexion à Deriv...")

try:
    ws = websocket.create_connection(
        URL,
        timeout=30
    )

    print("Connexion réussie.")

    ws.send(json.dumps({
        "ticks": "BOOM1000",
        "subscribe": 1,
        "req_id": 2001
    }))

    print("Abonnement envoyé.")

    for i in range(10):
        try:
            message = ws.recv()
            data = json.loads(message)

            print("MESSAGE", i + 1, ":", data)

        except websocket.WebSocketTimeoutException:
            print("Timeout :", i + 1)

    ws.close()

except Exception as e:
    print("ERREUR :", repr(e))