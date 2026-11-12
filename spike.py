#!/usr/bin/env python3
# ============================================================
# SPIKE1000
# Collecte Deriv + Analyse statistique + Détection pré-spike
# Stockage intégral dans db.json
#
# Aucun trade, paiement ou retrait.
# ============================================================

import os
import sys
import json
import time
import math
import statistics
from pathlib import Path
from datetime import datetime, timezone

try:
    import websocket
except ImportError:
    print("ERREUR : websocket-client manquant")
    sys.exit(1)


# ============================================================
# CONFIGURATION
# ============================================================

APP_ID = os.getenv("DERIV_APP_ID", "1089")
TOKEN = os.getenv("DERIV_API_TOKEN", "").strip()

SYMBOL = os.getenv("DERIV_SYMBOL", "BOOM1000")

HISTORY_COUNT = int(os.getenv("HISTORY_COUNT", "5000"))
COLLECT_SECONDS = int(os.getenv("COLLECT_SECONDS", "180"))
FUTURE_TICKS = int(os.getenv("FUTURE_TICKS", "20"))

MANUAL_THRESHOLD = os.getenv("SPIKE_THRESHOLD", "").strip()

BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "db.json"

WS_URL = (
    f"wss://ws.derivws.com/websockets/v3"
    f"?app_id={APP_ID}"
)


# ============================================================
# UTILITAIRES
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def load_db():

    if not DB_FILE.exists():

        return {
            "version": 1,
            "symbol": SYMBOL,
            "created_at": utc_now(),
            "updated_at": None,
            "ticks": [],
            "spikes": [],
            "analysis": {},
            "prediction": {},
            "errors": []
        }

    try:

        with open(DB_FILE, "r", encoding="utf-8") as f:
            db = json.load(f)

        db.setdefault("ticks", [])
        db.setdefault("spikes", [])
        db.setdefault("analysis", {})
        db.setdefault("prediction", {})
        db.setdefault("errors", [])

        return db

    except Exception as e:

        print("Erreur lecture db.json :", e)
        raise


def save_db(db):

    db["updated_at"] = utc_now()

    temporary = DB_FILE.with_suffix(".tmp")

    with open(temporary, "w", encoding="utf-8") as f:

        json.dump(
            db,
            f,
            indent=2,
            ensure_ascii=False
        )

    temporary.replace(DB_FILE)


def record_error(db, message):

    db["errors"].append({
        "time": utc_now(),
        "message": str(message)
    })

    db["errors"] = db["errors"][-100:]

    save_db(db)


# ============================================================
# CONNEXION DERIV
# ============================================================

def connect_deriv():

    print("Connexion à Deriv...")

    ws = websocket.create_connection(
        WS_URL,
        timeout=20,
        enable_multithread=True
    )

    ws.settimeout(5)

    print("WebSocket connecté.")

    if TOKEN:

        print("Authentification Deriv...")

        ws.send(json.dumps({
            "authorize": TOKEN
        }))

        response = receive_response(ws, 20)

        if not response or "authorize" not in response:

            ws.close()

            raise RuntimeError(
                "Authentification Deriv refusée."
            )

        account = response["authorize"]

        print(
            "Authentifié :",
            account.get("loginid", "Compte autorisé")
        )

        print("Mode : lecture uniquement.")

    else:

        print("Token absent : accès public.")

    return ws


def receive_response(ws, timeout=20):

    deadline = time.time() + timeout

    while time.time() < deadline:

        try:

            message = ws.recv()

            if not message:
                continue

            data = json.loads(message)

            if "error" in data:

                error = data["error"].get(
                    "message",
                    "Erreur Deriv"
                )

                raise RuntimeError(error)

            return data

        except websocket.WebSocketTimeoutException:
            continue

    return None


# ============================================================
# COLLECTE HISTORIQUE
# ============================================================

def collect_history(ws):

    print("Demande historique :", HISTORY_COUNT)

    ws.send(json.dumps({

        "ticks_history": SYMBOL,
        "count": HISTORY_COUNT,
        "end": "latest",
        "style": "ticks"

    }))

    deadline = time.time() + 40

    while time.time() < deadline:

        data = receive_response(ws, 5)

        if not data:
            continue

        history = data.get("history")

        if history:

            prices = history.get("prices", [])
            times = history.get("times", [])

            rows = []

            for timestamp, price in zip(times, prices):

                rows.append({

                    "epoch": int(timestamp),
                    "price": float(price),
                    "source": "history"

                })

            print(
                "Historique récupéré :",
                len(rows)
            )

            return rows

    raise TimeoutError(
        "Impossible de récupérer l'historique."
    )


# ============================================================
# COLLECTE EN DIRECT
# ============================================================

def collect_live(ws):

    print("Démarrage collecte en direct...")

    ws.send(json.dumps({

        "ticks": SYMBOL,
        "subscribe": 1

    }))

    deadline = time.time() + COLLECT_SECONDS

    rows = []

    while time.time() < deadline:

        data = receive_response(ws, 5)

        if not data:
            continue

        tick = data.get("tick")

        if not tick:
            continue

        rows.append({

            "epoch": int(tick["epoch"]),
            "price": float(tick["quote"]),
            "source": "live"

        })

    print(
        "Ticks en direct récupérés :",
        len(rows)
    )

    return rows


# ============================================================
# STOCKAGE CENTRAL DANS db.json
# ============================================================

def merge_ticks(db, new_rows):

    existing = {}

    for row in db["ticks"]:

        key = (
            int(row["epoch"]),
            float(row["price"])
        )

        existing[key] = row

    for row in new_rows:

        price = float(row["price"])

        if price <= 0:
            continue

        key = (
            int(row["epoch"]),
            price
        )

        if key not in existing:

            existing[key] = {

                "epoch": int(row["epoch"]),
                "price": price,
                "source": row.get("source", "unknown")

            }

    ordered = sorted(
        existing.values(),
        key=lambda x: x["epoch"]
    )

    # Limite de sécurité pour ne pas faire grossir
    # indéfiniment le fichier GitHub.

    db["ticks"] = ordered[-100000:]

    print(
        "Total ticks dans db.json :",
        len(db["ticks"])
    )


# ============================================================
# STATISTIQUES
# ============================================================

def percentile(values, q):

    if not values:
        return 0.0

    values = sorted(values)

    position = (len(values) - 1) * q

    lower = int(position)

    upper = min(
        lower + 1,
        len(values) - 1
    )

    fraction = position - lower

    return (
        values[lower]
        + fraction * (
            values[upper] - values[lower]
        )
    )


def calculate_returns(prices):

    returns = []

    for i in range(1, len(prices)):

        previous = prices[i - 1]

        current = prices[i]

        if previous <= 0:
            continue

        returns.append(
            (current - previous) / previous
        )

    return returns


def get_features(prices, index):

    if index < 30:
        return None

    returns = calculate_returns(
        prices[:index + 1]
    )

    if len(returns) < 30:
        return None

    last10 = returns[-10:]
    last30 = returns[-30:]

    negative = sum(
        1 for x in last10 if x < 0
    )

    average_movement = statistics.mean(
        abs(x) for x in last10
    )

    volatility = statistics.pstdev(last30)

    momentum10 = sum(last10)
    momentum30 = sum(last30)

    return (

        min(4, negative // 2),

        min(4, int(average_movement * 100000)),

        min(4, int(volatility * 100000)),

        -1 if momentum10 < -0.0002
        else 1 if momentum10 > 0.0002
        else 0,

        -1 if momentum30 < -0.0005
        else 1 if momentum30 > 0.0005
        else 0

    )


# ============================================================
# IDENTIFICATION DES SPIKES
# ============================================================

def detect_spikes(prices):

    returns = calculate_returns(prices)

    positive = [
        x for x in returns if x > 0
    ]

    if MANUAL_THRESHOLD:

        threshold = float(MANUAL_THRESHOLD)

    else:

        threshold = percentile(
            positive,
            0.995
        )

    threshold = max(
        threshold,
        0.00000001
    )

    spikes = []

    for i in range(1, len(prices)):

        previous = prices[i - 1]
        current = prices[i]

        if previous <= 0:
            continue

        variation = (
            current - previous
        ) / previous

        if variation >= threshold:

            spikes.append({

                "index": i,
                "epoch": i,
                "variation": variation,
                "price_before": previous,
                "price_after": current

            })

    return threshold, spikes


# ============================================================
# ANALYSE DES CONFIGURATIONS AVANT SPIKE
# ============================================================

def analyze_data(db):

    prices = [
        float(x["price"])
        for x in db["ticks"]
    ]

    if len(prices) < 100:

        db["analysis"] = {

            "status": "collecting",
            "ticks": len(prices),
            "message": "Données insuffisantes."

        }

        return

    print("Analyse des données...")

    threshold, spikes = detect_spikes(prices)

    spike_indices = {
        x["index"] for x in spikes
    }

    labeled = []

    for i in range(
        30,
        len(prices) - FUTURE_TICKS
    ):

        features = get_features(
            prices,
            i
        )

        if features is None:
            continue

        target = any(

            i < spike <= i + FUTURE_TICKS

            for spike in spike_indices

        )

        labeled.append({

            "index": i,
            "features": features,
            "target": target

        })

    # Séparation chronologique 70 / 30

    split = int(len(labeled) * 0.70)

    training = labeled[:split]
    testing = labeled[split:]

    patterns = {}

    for item in training:

        key = str(item["features"])

        if key not in patterns:

            patterns[key] = {
                "positive": 0,
                "total": 0
            }

        patterns[key]["total"] += 1

        if item["target"]:
            patterns[key]["positive"] += 1

    TP = FP = FN = TN = 0

    for item in testing:

        key = str(item["features"])

        pattern = patterns.get(
            key,
            {"positive": 0, "total": 0}
        )

        total = pattern["total"]

        rate = (
            pattern["positive"] / total
            if total else 0
        )

        prediction = (
            total >= 20
            and rate >= 0.10
        )

        actual = item["target"]

        if prediction and actual:
            TP += 1

        elif prediction and not actual:
            FP += 1

        elif not prediction and actual:
            FN += 1

        else:
            TN += 1

    precision = (
        TP / (TP + FP)
        if TP + FP else 0
    )

    recall = (
        TP / (TP + FN)
        if TP + FN else 0
    )

    false_positive_rate = (
        FP / (FP + TN)
        if FP + TN else 0
    )

    # Recherche des configurations les plus fréquentes

    best_patterns = []

    for key, value in patterns.items():

        total = value["total"]

        if total < 20:
            continue

        probability = (
            value["positive"] / total
        )

        best_patterns.append({

            "configuration": json.loads(
                key.replace("(", "[").replace(")", "]")
            ) if False else key,

            "observations": total,

            "spikes": value["positive"],

            "probability": round(
                probability,
                6
            )

        })

    best_patterns.sort(
        key=lambda x: (
            x["probability"],
            x["observations"]
        ),
        reverse=True
    )

    db["spikes"] = [

        {
            "tick_index": x["index"],
            "variation": x["variation"],
            "price_before": x["price_before"],
            "price_after": x["price_after"]
        }

        for x in spikes[-5000:]

    ]

    db["analysis"] = {

        "status": "completed",

        "updated_at": utc_now(),

        "symbol": SYMBOL,

        "total_ticks": len(prices),

        "total_spikes": len(spikes),

        "spike_threshold": threshold,

        "future_ticks": FUTURE_TICKS,

        "training_samples": len(training),

        "testing_samples": len(testing),

        "true_positive": TP,

        "false_positive": FP,

        "false_negative": FN,

        "true_negative": TN,

        "precision": round(precision, 6),

        "recall": round(recall, 6),

        "false_positive_rate": round(
            false_positive_rate,
            6
        ),

        "top_patterns": best_patterns[:30]

    }

    # Dernière configuration connue pour surveillance

    latest = get_features(
        prices,
        len(prices) - 1
    )

    prediction = None

    if latest is not None:

        pattern = patterns.get(
            str(latest),
            {"positive": 0, "total": 0}
        )

        total = pattern["total"]

        probability = (
            pattern["positive"] / total
            if total else 0
        )

        if total >= 20:

            prediction = {

                "configuration": str(latest),

                "observations": total,

                "historical_probability": round(
                    probability,
                    6
                ),

                "signal": (
                    "CONFIGURATION A SURVEILLER"
                    if probability >= 0.10
                    else "AUCUN SIGNAL FORT"
                )

            }

        else:

            prediction = {

                "configuration": str(latest),

                "observations": total,

                "historical_probability": None,

                "signal": "DONNEES INSUFFISANTES"

            }

    db["prediction"] = {

        "updated_at": utc_now(),

        "result": prediction,

        "warning": (
            "Résultat statistique expérimental. "
            "Aucune garantie de prédiction."
        )

    }

    print("Analyse terminée.")
    print("Spikes :", len(spikes))
    print("Précision :", round(precision * 100, 2), "%")
    print("Rappel :", round(recall * 100, 2), "%")


# ============================================================
# EXECUTION PRINCIPALE
# ============================================================

def main():

    print("=" * 55)
    print("SPIKE1000 — SYSTEME D'ANALYSE")
    print("=" * 55)

    db = load_db()

    ws = None

    try:

        ws = connect_deriv()

        historical = collect_history(ws)

        merge_ticks(db, historical)

        save_db(db)

        live = collect_live(ws)

        merge_ticks(db, live)

        analyze_data(db)

        save_db(db)

        print("=" * 55)
        print("TERMINÉ")
        print("Base :", DB_FILE)
        print("Ticks :", len(db["ticks"]))
        print("Spikes :", len(db["spikes"]))
        print("=" * 55)

    except KeyboardInterrupt:

        print("Arrêt demandé.")

        save_db(db)

    except Exception as e:

        print("ERREUR :", e)

        record_error(db, str(e))

        raise

    finally:

        if ws:

            try:
                ws.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
