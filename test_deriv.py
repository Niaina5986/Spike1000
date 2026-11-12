import json
import time
import websocket

ENDPOINTS = [
    (
        "NOUVELLE API",
        "wss://api.derivws.com/trading/v1/options/ws/public"
    ),
    (
        "ANCIENNE API",
        "wss://ws.derivws.com/websockets/v3?app_id=1089"
    )
]

def test_connection(name, url):
    print(f"\n--- TEST {name} ---")
    ws = None

    try:
        ws = websocket.create_connection(
            url,
            timeout=20,
            enable_multithread=True
        )

        print("CONNEXION : OK")

        request = {
            "ticks_history": "BOOM1000",
            "count": 5,
            "end": "latest",
            "style": "ticks",
            "req_id": 1
        }

        ws.send(json.dumps(request))
        response = json.loads(ws.recv())

        print("REPONSE DERIV :")
        print(json.dumps(response, indent=2))

        if "error" in response:
            print("RESULTAT : CONNECTE, MAIS ERREUR API")
        else:
            print("RESULTAT : SUCCES")

    except Exception as e:
        print("RESULTAT : ECHEC")
        print(type(e).__name__, str(e))

    finally:
        if ws:
            ws.close()

for name, url in ENDPOINTS:
    test_connection(name, url)
    time.sleep(3)

print("\nTESTS TERMINES")
