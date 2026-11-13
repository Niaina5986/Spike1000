#!/usr/bin/env python3
# ============================================================
# SPIKE1000 V10 - DERIV BOOM1000
# Collecte historique + flux live + analyse adaptative
# Lecture seule : aucun ordre ni trade.
# ============================================================

import os
import sys
import json
import time
import statistics
from pathlib import Path
from datetime import datetime, timezone

import websocket


# ============================================================
# CONFIGURATION
# ============================================================

APP_ID = os.getenv("DERIV_APP_ID", "1089")
SYMBOL = os.getenv("DERIV_SYMBOL", "BOOM1000")

HISTORY_COUNT = max(100, int(os.getenv("HISTORY_COUNT", "5000")))
COLLECT_SECONDS = max(10, int(os.getenv("COLLECT_SECONDS", "180")))
FUTURE_TICKS = max(1, int(os.getenv("FUTURE_TICKS", "20")))

VOLATILITY_MULTIPLIER = float(
    os.getenv("VOLATILITY_MULTIPLIER", "2.5")
)

MANUAL_THRESHOLD = os.getenv("SPIKE_THRESHOLD", "").strip()

WS_URL = (
    f"wss://ws.derivws.com/websockets/v3"
    f"?app_id={APP_ID}"
)

DB_FILE = Path(__file__).resolve().parent / "db.json"


# ============================================================
# UTILITAIRES
# ============================================================

def now():
    return datetime.now(timezone.utc).isoformat()


def percentile(values, q):

    if not values:
        return 0.0

    values = sorted(values)

    position = (len(values) - 1) * q

    low = int(position)
    high = min(low + 1, len(values) - 1)

    fraction = position - low

    return (
        values[low]
        + (values[high] - values[low]) * fraction
    )


# ============================================================
# BASE JSON
# ============================================================

def empty_db():

    return {
        "version": 10,
        "symbol": SYMBOL,
        "created_at": now(),
        "updated_at": None,
        "ticks": [],
        "spikes": [],
        "analysis": {},
        "prediction": {},
        "errors": []
    }


def load_db():

    if not DB_FILE.exists():
        return empty_db()

    try:

        with DB_FILE.open("r", encoding="utf-8") as f:
            db = json.load(f)

        if not isinstance(db, dict):
            raise ValueError("Format JSON invalide")

    except Exception as exc:

        backup = DB_FILE.with_name(
            "db.corrupt-" + str(int(time.time())) + ".json"
        )

        try:
            DB_FILE.replace(backup)
            print("Ancienne base sauvegardée :", backup.name)
        except Exception:
            print("Impossible de sauvegarder l'ancienne base.")

        print("Erreur ancienne base :", repr(exc))

        return empty_db()

    defaults = empty_db()

    for key, value in defaults.items():
        db.setdefault(key, value)

    for key in ("ticks", "spikes", "errors"):

        if not isinstance(db[key], list):
            db[key] = []

    for key in ("analysis", "prediction"):

        if not isinstance(db[key], dict):
            db[key] = {}

    db["symbol"] = SYMBOL
    db["version"] = 10

    return db


def save_db(db):

    db["updated_at"] = now()

    temp = DB_FILE.with_suffix(".tmp")

    with temp.open("w", encoding="utf-8") as f:

        json.dump(
            db,
            f,
            indent=2,
            ensure_ascii=False
        )

        f.flush()
        os.fsync(f.fileno())

    temp.replace(DB_FILE)

    print("Base sauvegardée :", DB_FILE)


def save_error(db, message):

    db["errors"].append({
        "time": now(),
        "message": str(message)
    })

    db["errors"] = db["errors"][-50:]

    save_db(db)


# ============================================================
# CONNEXION WEBSOCKET
# ============================================================

def connect_deriv():

    delays = [3, 6, 12, 20, 30]

    for attempt, delay in enumerate(delays, 1):

        try:

            print(
                f"Connexion Deriv : "
                f"{attempt}/{len(delays)}"
            )

            ws = websocket.create_connection(
                WS_URL,
                timeout=10,
                enable_multithread=True
            )

            ws.settimeout(2)

            print("Connexion Deriv réussie.")

            return ws

        except Exception as exc:

            print("Connexion échouée :", repr(exc))

            if attempt < len(delays):
                time.sleep(delay)

    raise RuntimeError("Impossible de joindre Deriv.")


def send(ws, data):

    ws.send(json.dumps(data))


def receive(ws, timeout=5):

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:

        try:

            raw = ws.recv()

            if not raw:
                continue

            data = json.loads(raw)

            if "error" in data:

                error = data["error"]

                raise RuntimeError(
                    error.get("message", "Erreur API Deriv")
                )

            return data

        except websocket.WebSocketTimeoutException:
            continue

    return None


def get_response(ws, req_id, timeout=60):

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:

        data = receive(ws, timeout=5)

        if not data:
            continue

        if data.get("req_id") == req_id:
            return data

    raise TimeoutError(
        f"Réponse Deriv absente pour req_id={req_id}"
    )


# ============================================================
# HISTORIQUE
# ============================================================

def collect_history(ws):

    print("Demande historique :", HISTORY_COUNT)

    send(ws, {
        "ticks_history": SYMBOL,
        "count": HISTORY_COUNT,
        "end": "latest",
        "style": "ticks",
        "req_id": 1001
    })

    data = get_response(ws, 1001, 90)

    history = data.get("history", {})

    prices = history.get("prices", [])
    times = history.get("times", [])

    rows = []

    for timestamp, price in zip(times, prices):

        try:

            price = float(price)

            if price <= 0:
                continue

            rows.append({
                "epoch": int(timestamp),
                "price": price,
                "source": "history"
            })

        except (TypeError, ValueError):
            continue

    rows.sort(key=lambda x: x["epoch"])

    if not rows:
        raise RuntimeError("Historique vide.")

    print("Historique reçu :", len(rows))

    return rows


# ============================================================
# COLLECTE LIVE AVEC RECONNEXION
# ============================================================

def collect_live(ws):

    print(
        "Collecte live pendant",
        COLLECT_SECONDS,
        "secondes."
    )

    rows = []

    deadline = time.monotonic() + COLLECT_SECONDS

    reconnects = 0
    last_ping = time.monotonic()

    subscription_id = None

    def subscribe(connection):

        send(connection, {
            "ticks": SYMBOL,
            "subscribe": 1,
            "req_id": 2001
        })

        print("Abonnement ticks demandé.")

    subscribe(ws)

    while time.monotonic() < deadline:

        try:

            # Maintien de la connexion
            if time.monotonic() - last_ping >= 25:

                send(ws, {"ping": 1})

                last_ping = time.monotonic()

            data = receive(ws, timeout=5)

            if not data:
                continue

            if data.get("subscription"):

                subscription_id = (
                    data["subscription"].get("id")
                )

            tick = data.get("tick")

            if tick:

                price = float(tick["quote"])
                epoch = int(tick["epoch"])

                if price > 0:

                    rows.append({
                        "epoch": epoch,
                        "price": price,
                        "source": "live"
                    })

            if data.get("msg_type") == "ping":
                continue

        except Exception as exc:

            reconnects += 1

            print(
                "Connexion interrompue :",
                repr(exc)
            )

            try:
                ws.close()
            except Exception:
                pass

            if time.monotonic() >= deadline:
                break

            if reconnects > 10:

                print("Trop de reconnexions.")

                break

            time.sleep(min(3 * reconnects, 15))

            try:

                ws = connect_deriv()

                subscribe(ws)

                subscription_id = None

                last_ping = time.monotonic()

                print("Flux live rétabli.")

            except Exception as reconnect_error:

                print(
                    "Reconnexion impossible :",
                    repr(reconnect_error)
                )

                time.sleep(5)

    if subscription_id:

        try:

            send(ws, {
                "forget": subscription_id
            })

        except Exception:
            pass

    print("Ticks live collectés :", len(rows))
    print("Reconnexions :", reconnects)

    return rows, ws


# ============================================================
# FUSION DES DONNEES
# ============================================================

def merge_ticks(db, new_rows):

    existing = {}

    for row in db["ticks"]:

        try:

            key = (
                int(row["epoch"]),
                float(row["price"])
            )

            existing[key] = row

        except Exception:
            continue

    for row in new_rows:

        try:

            key = (
                int(row["epoch"]),
                float(row["price"])
            )

            existing[key] = row

        except Exception:
            continue

    ordered = sorted(
        existing.values(),
        key=lambda x: (
            int(x["epoch"]),
            float(x["price"])
        )
    )

    db["ticks"] = ordered[-100000:]

    print("Total ticks :", len(db["ticks"]))


# ============================================================
# ANALYSE
# ============================================================

def calculate_returns(prices):

    returns = []

    for i in range(1, len(prices)):

        previous = prices[i - 1]
        current = prices[i]

        if previous <= 0:
            returns.append(0)
        else:
            returns.append(
                (current - previous) / previous
            )

    return returns


def get_features(returns, index):

    if index < 30:
        return None

    last10 = returns[index - 10:index]
    last30 = returns[index - 30:index]

    if len(last10) != 10 or len(last30) != 30:
        return None

    return {
        "negative_count": sum(
            1 for x in last10 if x < 0
        ),
        "average_movement": round(
            statistics.mean(abs(x) for x in last10),
            8
        ),
        "volatility": round(
            statistics.pstdev(last30),
            8
        ),
        "momentum10": round(sum(last10), 8),
        "momentum30": round(sum(last30), 8)
    }


# ============================================================
# SEUIL ADAPTATIF
# ============================================================

def calculate_threshold(returns, end_index):

    if MANUAL_THRESHOLD:

        threshold = float(MANUAL_THRESHOLD)

        if threshold <= 0:
            raise ValueError("Seuil manuel invalide.")

        return threshold

    values = [
        abs(x)
        for x in returns[1:end_index]
        if x != 0
    ]

    if len(values) < 30:
        raise ValueError("Historique insuffisant.")

    volatility = statistics.pstdev(values)

    limit = percentile(values, 0.99)

    threshold = max(
        volatility * VOLATILITY_MULTIPLIER,
        limit
    )

    return max(threshold, 0.00000001)


# ============================================================
# EVENEMENTS
# ============================================================

def detect_events(prices, epochs, returns, start, end):

    threshold = calculate_threshold(returns, end)

    candidates = []

    for i in range(max(1, start), min(end, len(prices))):

        movement = returns[i - 1]

        if movement >= threshold:

            candidates.append({
                "index": i,
                "epoch": epochs[i],
                "variation": movement,
                "price_before": prices[i - 1],
                "price_after": prices[i]
            })

    events = []

    for event in candidates:

        if (
            not events
            or event["index"] - events[-1]["last_index"]
            > FUTURE_TICKS
        ):

            events.append({
                **event,
                "last_index": event["index"]
            })

        else:

            events[-1]["last_index"] = event["index"]

            if event["variation"] > events[-1]["variation"]:

                events[-1].update(event)

    for event in events:
        event.pop("last_index", None)

    return threshold, events


# ============================================================
# DATASET
# ============================================================

def build_dataset(returns, events, start, end):

    event_indexes = [
        e["index"] for e in events
    ]

    dataset = []

    for i in range(max(30, start), end - FUTURE_TICKS):

        features = get_features(returns, i)

        if features is None:
            continue

        target = any(
            i < event_index <= i + FUTURE_TICKS
            for event_index in event_indexes
        )

        dataset.append({
            "index": i,
            "features": features,
            "target": target
        })

    return dataset


# ============================================================
# EVALUATION
# ============================================================

def feature_key(features):

    return json.dumps(
        features,
        sort_keys=True
    )


def analyze_patterns(training, testing):

    patterns = {}

    for item in training:

        key = feature_key(item["features"])

        p = patterns.setdefault(
            key,
            {"total": 0, "successes": 0}
        )

        p["total"] += 1

        if item["target"]:
            p["successes"] += 1

    tp = fp = fn = tn = 0

    for item in testing:

        key = feature_key(item["features"])

        p = patterns.get(
            key,
            {"total": 0, "successes": 0}
        )

        probability = (
            p["successes"] / p["total"]
            if p["total"] else 0
        )

        predicted = (
            p["total"] >= 20
            and probability >= 0.10
        )

        actual = item["target"]

        if predicted and actual:
            tp += 1
        elif predicted:
            fp += 1
        elif actual:
            fn += 1
        else:
            tn += 1

    precision = (
        tp / (tp + fp)
        if tp + fp else 0
    )

    recall = (
        tp / (tp + fn)
        if tp + fn else 0
    )

    best = []

    for p in patterns.values():

        if p["total"] < 20:
            continue

        best.append({
            "observations": p["total"],
            "successes": p["successes"],
            "probability_percent": round(
                100 * p["successes"] / p["total"],
                2
            )
        })

    best.sort(
        key=lambda x: x["probability_percent"],
        reverse=True
    )

    return {
        "true_positive": tp,
        "false_positive": fp,
        "false_negative": fn,
        "true_negative": tn,
        "precision_percent": round(precision * 100, 2),
        "recall_percent": round(recall * 100, 2),
        "best_patterns": best[:3]
    }


# ============================================================
# ANALYSE COMPLETE
# ============================================================

def analyze_data(db):

    ticks = db["ticks"]

    prices = [
        float(x["price"]) for x in ticks
    ]

    epochs = [
        int(x["epoch"]) for x in ticks
    ]

    total = len(prices)

    if total < 150:

        db["analysis"] = {
            "status": "collecting",
            "total_ticks": total
        }

        db["prediction"] = {
            "status": "WAIT",
            "reason": "Données insuffisantes."
        }

        print("Pas assez de données.")

        return

    returns = calculate_returns(prices)

    train_end = int(total * 0.60)
    validation_end = int(total * 0.80)

    train_threshold, train_events = detect_events(
        prices, epochs, returns,
        30, train_end - FUTURE_TICKS
    )

    validation_threshold, validation_events = detect_events(
        prices, epochs, returns,
        train_end, validation_end - FUTURE_TICKS
    )

    test_threshold, test_events = detect_events(
        prices, epochs, returns,
        validation_end, total - FUTURE_TICKS
    )

    training = build_dataset(
        returns, train_events, 30, train_end
    )

    validation = build_dataset(
        returns, validation_events, train_end, validation_end
    )

    testing = build_dataset(
        returns, test_events, validation_end, total
    )

    validation_result = analyze_patterns(
        training, validation
    )

    test_result = analyze_patterns(
        training, testing
    )

    all_events = (
        train_events
        + validation_events
        + test_events
    )

    db["spikes"] = [
        {
            "epoch": e["epoch"],
            "variation_percent": round(
                e["variation"] * 100, 6
            ),
            "price_before": e["price_before"],
            "price_after": e["price_after"]
        }
        for e in all_events[-100:]
    ]

    db["analysis"] = {

        "status": "completed",
        "version": "V10",
        "symbol": SYMBOL,
        "updated_at": now(),
        "total_ticks": total,

        "threshold_method": "Adaptive volatility",

        "thresholds": {
            "training": round(train_threshold, 8),
            "validation": round(validation_threshold, 8),
            "test": round(test_threshold, 8)
        },

        "unique_events": {
            "training": len(train_events),
            "validation": len(validation_events),
            "test": len(test_events)
        },

        "samples": {
            "training": len(training),
            "validation": len(validation),
            "test": len(testing)
        },

        "validation": {
            "precision_percent":
                validation_result["precision_percent"],
            "recall_percent":
                validation_result["recall_percent"]
        },

        "test": {
            "true_positive": test_result["true_positive"],
            "false_positive": test_result["false_positive"],
            "false_negative": test_result["false_negative"],
            "precision_percent":
                test_result["precision_percent"],
            "recall_percent":
                test_result["recall_percent"]
        },

        "best_patterns": test_result["best_patterns"],

        "warning": (
            "Analyse statistique expérimentale. "
            "Aucune garantie de prédiction."
        )
    }

    db["prediction"] = {
        "status": "WAIT",
        "updated_at": now(),
        "reason": (
            "Surveillance statistique. "
            "Aucun signal prédictif confirmé."
        )
    }

    print("--------------------------------")
    print("SPIKE1000 V10")
    print("Ticks :", total)
    print("Événements train :", len(train_events))
    print("Événements validation :", len(validation_events))
    print("Événements test :", len(test_events))
    print("Seuil :", round(train_threshold, 8))
    print("Précision test :", test_result["precision_percent"], "%")
    print("Statut : WAIT")
    print("--------------------------------")


# ============================================================
# MAIN
# ============================================================

def main():

    print("=" * 50)
    print("SPIKE1000 V10 - BOOM1000")
    print("Connexion Deriv publique")
    print("Lecture seule")
    print("=" * 50)

    db = load_db()

    ws = None

    try:

        ws = connect_deriv()

        history = collect_history(ws)

        merge_ticks(db, history)

        save_db(db)

        live, ws = collect_live(ws)

        merge_ticks(db, live)

        analyze_data(db)

        save_db(db)

        print("=" * 50)
        print("EXECUTION TERMINEE")
        print("Ticks :", len(db["ticks"]))
        print("Événements :", len(db["spikes"]))
        print("Base :", DB_FILE)
        print("=" * 50)

    except Exception as exc:

        print("ERREUR :", repr(exc))

        save_error(db, repr(exc))

        raise

    finally:

        if ws is not None:

            try:
                ws.close()
            except Exception:
                pass


if __name__ == "__main__":

    try:
        main()

    except Exception as error:

        print("ERREUR FATALE :", repr(error))

        raise