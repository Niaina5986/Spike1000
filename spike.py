#!/usr/bin/env python3
# ============================================================
# SPIKE1000 - COLLECTE ET ANALYSE STATISTIQUE
# Aucun trade, paiement ou retrait.
# ============================================================

import os
import sys
import json
import time
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
    "wss://ws.derivws.com/websockets/v3"
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

    with open(DB_FILE, "r", encoding="utf-8") as f:
        db = json.load(f)

    db.setdefault("ticks", [])
    db.setdefault("spikes", [])
    db.setdefault("analysis", {})
    db.setdefault("prediction", {})
    db.setdefault("errors", [])

    return db


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

    print("Base sauvegardée :", DB_FILE)


def record_error(db, message):

    db["errors"].append({
        "time": utc_now(),
        "message": str(message)
    })

    db["errors"] = db["errors"][-100:]

    save_db(db)


# ============================================================
# CONNEXION DERIV AVEC RECONNEXION AUTOMATIQUE
# ============================================================

def connect_deriv():

    delays = [5, 10, 20, 30, 60]
    last_error = None
    ws = None

    for attempt, delay in enumerate(delays, start=1):

        try:

            print(
                f"Connexion Deriv : tentative "
                f"{attempt}/{len(delays)}"
            )

            ws = websocket.create_connection(
                WS_URL,
                timeout=30,
                enable_multithread=True
            )

            ws.settimeout(5)

            print("WebSocket connecté.")

            break

        except Exception as error:

            last_error = error

            print("Erreur de connexion :", error)

            if attempt < len(delays):

                print(
                    f"Nouvelle tentative dans {delay} secondes..."
                )

                time.sleep(delay)

    if ws is None:

        raise RuntimeError(
            "Impossible de se connecter à Deriv après "
            f"{len(delays)} tentatives : {last_error}"
        )

    # Authentification facultative.
    # Les données publiques de ticks ne nécessitent
    # normalement pas de token.

    if TOKEN:

        try:

            print("Authentification Deriv...")

            ws.send(json.dumps({
                "authorize": TOKEN
            }))

            response = receive_response(ws, 20)

            if not response or "authorize" not in response:

                raise RuntimeError(
                    "Authentification Deriv refusée."
                )

            print("Authentification réussie.")
            print("Mode : lecture uniquement.")

        except Exception:

            try:
                ws.close()
            except Exception:
                pass

            raise

    else:

        print("Accès public Deriv.")

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

                message = data["error"].get(
                    "message",
                    "Erreur Deriv"
                )

                raise RuntimeError(message)

            return data

        except websocket.WebSocketTimeoutException:

            continue

    return None


# ============================================================
# HISTORIQUE
# ============================================================

def collect_history(ws):

    print("Demande de l'historique :", HISTORY_COUNT)

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

            print("Historique récupéré :", len(rows))

            return rows

    raise TimeoutError(
        "Impossible de récupérer l'historique."
    )


# ============================================================
# COLLECTE EN DIRECT
# ============================================================

def collect_live(ws):

    print("Collecte en direct...")

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

    print("Ticks en direct :", len(rows))

    return rows


# ============================================================
# FUSION DES DONNEES
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

    db["ticks"] = ordered[-100000:]

    print(
        "Total de ticks dans db.json :",
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
            returns.append(0.0)
        else:
            returns.append(
                (current - previous) / previous
            )

    return returns


def get_features_from_returns(returns, index):

    if index < 30:
        return None

    last10 = returns[index - 10:index]
    last30 = returns[index - 30:index]

    if len(last30) < 30:
        return None

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
# DETECTION DES SPIKES
# ============================================================

def detect_spikes(prices, epochs):

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
                "epoch": int(epochs[i]),
                "variation": variation,
                "price_before": previous,
                "price_after": current
            })

    return threshold, spikes


# ============================================================
# ANALYSE DES CONFIGURATIONS
# ============================================================

def analyze_data(db):

    ticks = db["ticks"]

    prices = [
        float(x["price"])
        for x in ticks
    ]

    epochs = [
        int(x["epoch"])
        for x in ticks
    ]

    if len(prices) < 100:

        db["analysis"] = {
            "status": "collecting",
            "ticks": len(prices),
            "message": "Données insuffisantes."
        }

        return

    print("Analyse statistique...")

    returns = calculate_returns(prices)

    threshold, spikes = detect_spikes(
        prices,
        epochs
    )

    spike_indices = {
        x["index"] for x in spikes
    }

    # Préparation des étiquettes futures.

    labeled = []

    for i in range(30, len(prices) - FUTURE_TICKS):

        features = get_features_from_returns(
            returns,
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

    # Séparation chronologique 70 / 30.

    split = int(len(labeled) * 0.70)

    training = labeled[:split]
    testing = labeled[split:]

    patterns = {}

    for item in training:

        key = json.dumps(
            item["features"]
        )

        if key not in patterns:

            patterns[key] = {
                "positive": 0,
                "total": 0
            }

        patterns[key]["total"] += 1

        if item["target"]:
            patterns[key]["positive"] += 1

    TP = 0
    FP = 0
    FN = 0
    TN = 0

    for item in testing:

        key = json.dumps(
            item["features"]
        )

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

    # Configurations historiques.

    best_patterns = []

    for key, value in patterns.items():

        total = value["total"]

        if total < 20:
            continue

        probability = (
            value["positive"] / total
        )

        best_patterns.append({
            "configuration": json.loads(key),
            "observations": total,
            "spikes": value["positive"],
            "probability": round(probability, 6)
        })

    best_patterns.sort(
        key=lambda x: (
            x["probability"],
            x["observations"]
        ),
        reverse=True
    )

    # Sauvegarde des spikes.

    db["spikes"] = [
        {
            "tick_index": x["index"],
            "epoch": x["epoch"],
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

    # Configuration actuelle.

    latest = get_features_from_returns(
        returns,
        len(prices) - 1
    )

    prediction = None

    if latest is not None:

        key = json.dumps(latest)

        pattern = patterns.get(
            key,
            {"positive": 0, "total": 0}
        )

        total = pattern["total"]

        probability = (
            pattern["positive"] / total
            if total else 0
        )

        if total >= 20:

            prediction = {
                "configuration": latest,
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
                "configuration": latest,
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
    print("Nombre de spikes :", len(spikes))
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

        # Sauvegarde immédiate de l'historique.

        save_db(db)

        live = collect_live(ws)

        merge_ticks(db, live)

        analyze_data(db)

        save_db(db)

        print("=" * 55)
        print("TERMINÉ")
        print("Ticks :", len(db["ticks"]))
        print("Spikes :", len(db["spikes"]))
        print("=" * 55)

    except KeyboardInterrupt:

        print("Arrêt demandé.")

        save_db(db)

    except Exception as error:

        print("ERREUR :", error)

        record_error(db, str(error))

        raise

    finally:

        if ws:

            try:
                ws.close()
            except Exception:
                pass


if __name__ == "__main__":

    main()
