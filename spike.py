#!/usr/bin/env python3
# ============================================================
# SPIKE1000 V10
# Collecte Deriv + analyse statistique adaptative
# Lecture seule : aucun trade ni ordre.
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
    print("ERREUR : websocket-client manquant.")
    sys.exit(1)


# ============================================================
# CONFIGURATION
# ============================================================

SYMBOL = os.getenv("DERIV_SYMBOL", "BOOM1000").strip()
HISTORY_COUNT = max(100, int(os.getenv("HISTORY_COUNT", "5000")))
COLLECT_SECONDS = max(10, int(os.getenv("COLLECT_SECONDS", "180")))
FUTURE_TICKS = max(1, int(os.getenv("FUTURE_TICKS", "20")))

VOLATILITY_MULTIPLIER = float(
    os.getenv("VOLATILITY_MULTIPLIER", "2.5")
)

MANUAL_THRESHOLD = os.getenv("SPIKE_THRESHOLD", "").strip()

WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"

BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "db.json"


# ============================================================
# UTILITAIRES
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def percentile(values, q):
    if not values:
        return 0.0

    values = sorted(values)

    position = (len(values) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(values) - 1)

    fraction = position - lower

    return (
        values[lower]
        + (values[upper] - values[lower]) * fraction
    )


# ============================================================
# BASE DE DONNEES JSON
# ============================================================

def empty_db():
    return {
        "version": 10,
        "symbol": SYMBOL,
        "created_at": utc_now(),
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
            raise ValueError("Format db.json invalide.")

    except Exception as exc:

        backup = DB_FILE.with_name(
            "db.corrupt-" + str(int(time.time())) + ".json"
        )

        try:
            DB_FILE.replace(backup)
            print("Ancienne base sauvegardée :", backup.name)
        except OSError:
            print("Impossible de sauvegarder l'ancienne base :", exc)

        db = empty_db()

    defaults = empty_db()

    for key, value in defaults.items():
        db.setdefault(key, value)

    if not isinstance(db.get("ticks"), list):
        db["ticks"] = []

    if not isinstance(db.get("spikes"), list):
        db["spikes"] = []

    if not isinstance(db.get("errors"), list):
        db["errors"] = []

    if not isinstance(db.get("analysis"), dict):
        db["analysis"] = {}

    if not isinstance(db.get("prediction"), dict):
        db["prediction"] = {}

    db["symbol"] = SYMBOL
    db["version"] = 10

    return db


def save_db(db):

    db["updated_at"] = utc_now()

    temporary = DB_FILE.with_suffix(".tmp")

    with temporary.open("w", encoding="utf-8") as f:

        json.dump(
            db,
            f,
            indent=2,
            ensure_ascii=False
        )

        f.flush()
        os.fsync(f.fileno())

    temporary.replace(DB_FILE)

    print("Base sauvegardée :", DB_FILE)


def record_error(db, message):

    db.setdefault("errors", []).append({
        "time": utc_now(),
        "message": str(message)
    })

    db["errors"] = db["errors"][-50:]

    save_db(db)


# ============================================================
# CONNEXION DERIV
# ============================================================

def connect_deriv():

    delays = [3, 6, 12, 20]
    last_error = None

    for attempt, delay in enumerate(delays, start=1):

        ws = None

        try:

            print(
                f"Connexion Deriv : "
                f"tentative {attempt}/{len(delays)}"
            )

            ws = websocket.create_connection(
                WS_URL,
                timeout=30,
                enable_multithread=True,
                origin="https://app.deriv.com"
            )

            ws.settimeout(2)

            print("Connexion WebSocket réussie.")
            print("Mode public : lecture seule.")

            return ws

        except Exception as exc:

            last_error = exc

            print("Erreur connexion :", repr(exc))

            if ws is not None:

                try:
                    ws.close()
                except Exception:
                    pass

            if attempt < len(delays):
                time.sleep(delay)

    raise RuntimeError(
        f"Connexion Deriv impossible : {last_error}"
    )


def receive_message(ws, timeout=10):

    deadline = time.monotonic() + timeout

    while time.monotonic() < deadline:

        try:

            raw = ws.recv()

            if not raw:
                continue

            data = json.loads(raw)

            if "error" in data:

                error = data.get("error") or {}

                raise RuntimeError(
                    error.get("message", "Erreur Deriv")
                )

            return data

        except websocket.WebSocketTimeoutException:
            continue

    return None


def send_request(ws, payload):

    ws.send(json.dumps(payload))


# ============================================================
# COLLECTE HISTORIQUE
# ============================================================

def collect_history(ws):

    print(
        f"Collecte de {HISTORY_COUNT} ticks "
        f"historiques pour {SYMBOL}..."
    )

    send_request(ws, {
        "ticks_history": SYMBOL,
        "count": HISTORY_COUNT,
        "end": "latest",
        "style": "ticks",
        "req_id": 1001
    })

    deadline = time.monotonic() + 60

    while time.monotonic() < deadline:

        data = receive_message(ws, timeout=5)

        if not data:
            continue

        history = data.get("history")

        if history is None:
            continue

        prices = history.get("prices", [])
        times = history.get("times", [])

        rows = []

        for timestamp, price in zip(times, prices):

            try:

                p = float(price)

                if p <= 0:
                    continue

                rows.append({
                    "epoch": int(timestamp),
                    "price": p,
                    "source": "history"
                })

            except (TypeError, ValueError):
                continue

        if not rows:
            raise RuntimeError(
                "Historique Deriv vide."
            )

        rows.sort(
            key=lambda x: (x["epoch"], x["price"])
        )

        print("Ticks historiques reçus :", len(rows))

        return rows

    raise TimeoutError(
        "Délai dépassé pour la collecte historique."
    )


# ============================================================
# COLLECTE EN DIRECT
# ============================================================

def collect_live(ws):

    print(
        f"Collecte en direct pendant "
        f"{COLLECT_SECONDS} secondes..."
    )

    send_request(ws, {
        "ticks": SYMBOL,
        "subscribe": 1,
        "req_id": 1002
    })

    deadline = time.monotonic() + COLLECT_SECONDS

    rows = []
    subscription_id = None

    while time.monotonic() < deadline:

        data = receive_message(ws, timeout=5)

        if not data:
            continue

        if data.get("subscription"):

            subscription_id = data[
                "subscription"
            ].get("id", subscription_id)

        tick = data.get("tick")

        if not tick:
            continue

        try:

            epoch = int(tick["epoch"])
            price = float(tick["quote"])

            if price <= 0:
                continue

            rows.append({
                "epoch": epoch,
                "price": price,
                "source": "live"
            })

        except (KeyError, TypeError, ValueError):
            continue

    if subscription_id:

        try:

            send_request(ws, {
                "forget": subscription_id,
                "req_id": 1003
            })

        except Exception as exc:

            print(
                "Avertissement désabonnement :",
                repr(exc)
            )

    print("Ticks en direct reçus :", len(rows))

    return rows


# ============================================================
# FUSION DES TICKS
# ============================================================

def merge_ticks(db, new_rows):

    existing = {}

    for row in db.get("ticks", []):

        try:

            epoch = int(row["epoch"])
            price = float(row["price"])

            if price > 0:

                existing[(epoch, price)] = {
                    "epoch": epoch,
                    "price": price,
                    "source": row.get("source", "previous")
                }

        except (KeyError, TypeError, ValueError):
            continue

    for row in new_rows:

        try:

            epoch = int(row["epoch"])
            price = float(row["price"])

            if price <= 0:
                continue

            key = (epoch, price)

            if key not in existing:

                existing[key] = {
                    "epoch": epoch,
                    "price": price,
                    "source": row.get("source", "unknown")
                }

        except (KeyError, TypeError, ValueError):
            continue

    ordered = sorted(
        existing.values(),
        key=lambda x: (x["epoch"], x["price"])
    )

    db["ticks"] = ordered[-100000:]

    print(
        "Total de ticks enregistrés :",
        len(db["ticks"])
    )


# ============================================================
# RENDEMENTS ET INDICATEURS
# ============================================================

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


def get_features(returns, index):

    if index < 30:
        return None

    last10 = returns[index - 10:index]
    last30 = returns[index - 30:index]

    if len(last10) != 10 or len(last30) != 30:
        return None

    negative_count = sum(
        1 for value in last10 if value < 0
    )

    average_movement = statistics.mean(
        abs(value) for value in last10
    )

    volatility = statistics.pstdev(last30)

    momentum10 = sum(last10)
    momentum30 = sum(last30)

    return {
        "negative_count": min(10, negative_count),
        "average_movement": round(average_movement, 8),
        "volatility": round(volatility, 8),
        "momentum10": (
            -1 if momentum10 < 0
            else 1 if momentum10 > 0
            else 0
        ),
        "momentum30": (
            -1 if momentum30 < 0
            else 1 if momentum30 > 0
            else 0
        )
    }


# ============================================================
# SEUIL AUTOMATIQUE
# ============================================================

def calculate_adaptive_threshold(returns, end_index):

    if MANUAL_THRESHOLD:

        threshold = float(MANUAL_THRESHOLD)

        if threshold <= 0:
            raise ValueError(
                "SPIKE_THRESHOLD doit être positif."
            )

        return threshold

    historical = [
        abs(value)
        for value in returns[1:end_index]
        if value != 0
    ]

    if len(historical) < 30:
        raise ValueError(
            "Pas assez de données pour calculer la volatilité."
        )

    volatility = statistics.pstdev(historical)

    # Le seuil est calculé uniquement sur l'historique
    # disponible avant la période analysée.
    threshold = volatility * VOLATILITY_MULTIPLIER

    # Plancher basé sur le 99e percentile des mouvements.
    percentile_limit = percentile(historical, 0.99)

    threshold = max(threshold, percentile_limit)

    return max(threshold, 0.00000001)


# ============================================================
# DETECTION ET REGROUPEMENT DES EVENEMENTS
# ============================================================

def detect_events(prices, epochs, returns, start, end):

    threshold = calculate_adaptive_threshold(
        returns,
        end
    )

    candidates = []

    for i in range(max(1, start), min(end, len(prices))):

        variation = returns[i - 1]

        if variation >= threshold:

            candidates.append({
                "index": i,
                "epoch": int(epochs[i]),
                "variation": variation,
                "price_before": prices[i - 1],
                "price_after": prices[i]
            })

    # Fusion des mouvements rapprochés.
    # Un groupe ne compte que comme un événement.
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

            if (
                event["variation"]
                > events[-1]["variation"]
            ):

                events[-1].update({
                    "index": event["index"],
                    "epoch": event["epoch"],
                    "variation": event["variation"],
                    "price_before": event["price_before"],
                    "price_after": event["price_after"]
                })

    for event in events:
        event.pop("last_index", None)

    return threshold, events


# ============================================================
# CREATION DES EXEMPLES HISTORIQUES
# ============================================================

def build_dataset(returns, events, start, end):

    event_indexes = [
        event["index"] for event in events
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
# ANALYSE DES CONFIGURATIONS
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

        pattern = patterns.setdefault(
            key,
            {"total": 0, "successes": 0}
        )

        pattern["total"] += 1

        if item["target"]:
            pattern["successes"] += 1

    tp = 0
    fp = 0
    fn = 0
    tn = 0

    for item in testing:

        key = feature_key(item["features"])

        pattern = patterns.get(
            key,
            {"total": 0, "successes": 0}
        )

        total = pattern["total"]

        probability = (
            pattern["successes"] / total
            if total else 0
        )

        predicted = (
            total >= 20
            and probability >= 0.10
        )

        actual = item["target"]

        if predicted and actual:
            tp += 1

        elif predicted and not actual:
            fp += 1

        elif not predicted and actual:
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

    best_patterns = []

    for key, value in patterns.items():

        total = value["total"]

        if total < 20:
            continue

        probability = value["successes"] / total

        best_patterns.append({
            "observations": total,
            "successes": value["successes"],
            "probability_percent": round(
                probability * 100, 2
            )
        })

    best_patterns.sort(
        key=lambda x: (
            x["probability_percent"],
            x["successes"]
        ),
        reverse=True
    )

    return {
        "patterns": best_patterns[:3],
        "true_positive": tp,
        "false_positive": fp,
        "false_negative": fn,
        "true_negative": tn,
        "precision_percent": round(
            precision * 100, 2
        ),
        "recall_percent": round(
            recall * 100, 2
        )
    }


# ============================================================
# ANALYSE DES PRECURSEURS
# ============================================================

def compare_precursors(training, events):

    event_indexes = {
        event["index"] for event in events
    }

    before = []
    normal = []

    for item in training:

        destination = (
            before
            if any(
                item["index"] < event_index
                <= item["index"] + FUTURE_TICKS
                for event_index in event_indexes
            )
            else normal
        )

        destination.append(item["features"])

    fields = [
        "negative_count",
        "average_movement",
        "volatility",
        "momentum10",
        "momentum30"
    ]

    result = {}

    for field in fields:

        a = [x[field] for x in before]
        b = [x[field] for x in normal]

        if a and b:

            result[field] = {
                "before_events": round(
                    statistics.mean(a), 8
                ),
                "normal": round(
                    statistics.mean(b), 8
                )
            }

    return result


# ============================================================
# SURVEILLANCE ACTUELLE
# ============================================================

def current_status(returns, patterns):

    latest = get_features(
        returns,
        len(returns)
    )

    if latest is None:

        return {
            "status": "WAIT",
            "reason": "Indicateurs insuffisants."
        }

    if not patterns:

        return {
            "status": "WAIT",
            "reason": "Pas de configuration suffisamment étudiée."
        }

    # La version actuelle ne considère pas un simple
    # mouvement baissier comme une prédiction certaine.
    return {
        "status": "WAIT",
        "latest_features": latest,
        "reason": (
            "Surveillance statistique. "
            "Aucun signal prédictif confirmé."
        )
    }


# ============================================================
# ANALYSE PRINCIPALE
# ============================================================

def analyze_data(db):

    ticks = db["ticks"]

    prices = [
        float(row["price"])
        for row in ticks
    ]

    epochs = [
        int(row["epoch"])
        for row in ticks
    ]

    if len(prices) < 150:

        db["analysis"] = {
            "status": "collecting",
            "total_ticks": len(prices),
            "message": "Données insuffisantes."
        }

        db["prediction"] = {
            "status": "WAIT",
            "reason": "Au moins 150 ticks sont nécessaires."
        }

        print("Analyse reportée : données insuffisantes.")

        return

    print("Analyse V10 en cours...")

    returns = calculate_returns(prices)

    total = len(prices)

    train_end = int(total * 0.60)
    validation_end = int(total * 0.80)

    # Les périodes sont séparées chronologiquement.
    # Les fenêtres futures ne traversent pas les frontières.

    train_threshold, train_events = detect_events(
        prices,
        epochs,
        returns,
        30,
        train_end - FUTURE_TICKS
    )

    validation_threshold, validation_events = detect_events(
        prices,
        epochs,
        returns,
        train_end,
        validation_end - FUTURE_TICKS
    )

    test_threshold, test_events = detect_events(
        prices,
        epochs,
        returns,
        validation_end,
        total - FUTURE_TICKS
    )

    training = build_dataset(
        returns,
        train_events,
        30,
        train_end
    )

    validation = build_dataset(
        returns,
        validation_events,
        train_end,
        validation_end
    )

    testing = build_dataset(
        returns,
        test_events,
        validation_end,
        total
    )

    train_analysis = analyze_patterns(
        training,
        training
    )

    validation_analysis = analyze_patterns(
        training,
        validation
    )

    test_analysis = analyze_patterns(
        training,
        testing
    )

    precursors = compare_precursors(
        training,
        train_events
    )

    status = current_status(
        returns,
        train_analysis["patterns"]
    )

    all_events = (
        train_events
        + validation_events
        + test_events
    )

    db["spikes"] = [
        {
            "epoch": event["epoch"],
            "variation_percent": round(
                event["variation"] * 100, 6
            ),
            "price_before": event["price_before"],
            "price_after": event["price_after"]
        }
        for event in all_events[-100:]
    ]

    db["analysis"] = {

        "status": "completed",
        "version": "V10",
        "updated_at": utc_now(),
        "symbol": SYMBOL,
        "total_ticks": total,

        "threshold_method": (
            "Automatic volatility threshold"
        ),

        "volatility_multiplier": VOLATILITY_MULTIPLIER,

        "thresholds": {
            "training": round(train_threshold, 8),
            "validation": round(validation_threshold, 8),
            "test": round(test_threshold, 8)
        },

        "future_ticks": FUTURE_TICKS,

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
                validation_analysis["precision_percent"],
            "recall_percent":
                validation_analysis["recall_percent"]
        },

        "test": {
            "true_positive":
                test_analysis["true_positive"],
            "false_positive":
                test_analysis["false_positive"],
            "false_negative":
                test_analysis["false_negative"],
            "precision_percent":
                test_analysis["precision_percent"],
            "recall_percent":
                test_analysis["recall_percent"]
        },

        "best_patterns": train_analysis["patterns"],

        "precursors": precursors,

        "warning": (
            "Résultats statistiques expérimentaux. "
            "Aucune garantie de mouvement futur."
        )
    }

    db["prediction"] = {
        "updated_at": utc_now(),
        "status": status["status"],
        "details": status,
        "warning": (
            "Ce système analyse les données. "
            "Il ne passe aucun ordre."
        )
    }

    print("------------------------------------")
    print("SPIKE1000 V10")
    print("Ticks :", total)
    print("Événements entraînement :", len(train_events))
    print("Événements validation :", len(validation_events))
    print("Événements test :", len(test_events))
    print("Seuil automatique :", round(train_threshold, 8))
    print("Précision test :", test_analysis["precision_percent"], "%")
    print("Statut :", status["status"])
    print("------------------------------------")


# ============================================================
# EXECUTION PRINCIPALE
# ============================================================

def main():

    print("=" * 55)
    print("SPIKE1000 V10 - BOOM1000")
    print("Collecte et analyse statistique")
    print("Deriv API publique - lecture seule")
    print("=" * 55)

    db = load_db()

    ws = None

    try:

        ws = connect_deriv()

        history = collect_history(ws)

        merge_ticks(db, history)

        save_db(db)

        live = collect_live(ws)

        merge_ticks(db, live)

        analyze_data(db)

        save_db(db)

        print("=" * 55)
        print("EXECUTION TERMINEE")
        print("Ticks enregistrés :", len(db["ticks"]))
        print("Événements détectés :", len(db["spikes"]))
        print("Fichier :", DB_FILE)
        print("=" * 55)

    except KeyboardInterrupt:

        print("Arrêt demandé.")
        save_db(db)

    except Exception as exc:

        print("ERREUR :", repr(exc))

        try:
            record_error(db, repr(exc))
        except Exception as save_exc:
            print("Erreur sauvegarde :", repr(save_exc))

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