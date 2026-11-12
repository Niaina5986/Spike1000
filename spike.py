#!/usr/bin/env python3
# ============================================================
# SPIKE1000 - MOTEUR DE COLLECTE ET D'ANALYSE
# DERIV PUBLIC API - MODE LECTURE SEULE
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

SYMBOL = os.getenv("DERIV_SYMBOL", "BOOM1000")

HISTORY_COUNT = int(os.getenv("HISTORY_COUNT", "5000"))
COLLECT_SECONDS = int(os.getenv("COLLECT_SECONDS", "180"))
FUTURE_TICKS = int(os.getenv("FUTURE_TICKS", "20"))

WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"

DB_FILE = Path(__file__).resolve().parent / "db.json"


# ============================================================
# UTILITAIRES
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def empty_db():
    return {
        "version": 4,
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
        with open(DB_FILE, "r", encoding="utf-8") as f:
            db = json.load(f)

        if not isinstance(db, dict):
            raise ValueError("Format JSON invalide")

    except Exception as exc:

        print("Erreur lecture DB :", repr(exc))

        backup = DB_FILE.with_name(
            "db.corrupt-" + str(int(time.time())) + ".json"
        )

        try:
            DB_FILE.replace(backup)
            print("Ancienne base sauvegardée :", backup.name)
        except Exception:
            pass

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

    return db


def save_db(db):

    db["updated_at"] = utc_now()

    temporary = DB_FILE.with_suffix(".tmp")

    with open(temporary, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())

    temporary.replace(DB_FILE)

    print("Base sauvegardée :", DB_FILE)


def record_error(db, message):

    db.setdefault("errors", []).append({
        "time": utc_now(),
        "message": str(message)
    })

    db["errors"] = db["errors"][-100:]

    save_db(db)


# ============================================================
# CONNEXION DERIV
# ============================================================

def connect_deriv():

    delays = [5, 10, 20, 30, 60]

    last_error = None

    for attempt, delay in enumerate(delays, start=1):

        try:

            print(
                f"Connexion Deriv : "
                f"tentative {attempt}/{len(delays)}"
            )

            # Même méthode que le test_deriv.py réussi.
            ws = websocket.create_connection(
                WS_URL,
                timeout=30
            )

            print("Connexion Deriv réussie.")
            return ws

        except Exception as exc:

            last_error = exc

            print("Erreur connexion :", repr(exc))

            if attempt < len(delays):
                print(f"Nouvelle tentative dans {delay} secondes.")
                time.sleep(delay)

    raise RuntimeError(
        f"Connexion Deriv impossible : {last_error}"
    )


# ============================================================
# RECEPTION DES MESSAGES HISTORIQUES
# ============================================================

def receive_message(ws, timeout=10):

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
                    error.get("message", "Erreur Deriv")
                )

            return data

        except websocket.WebSocketTimeoutException:
            continue

    return None


# ============================================================
# HISTORIQUE DERIV
# ============================================================

def collect_history():

    print(
        f"Demande de {HISTORY_COUNT} ticks historiques "
        f"pour {SYMBOL}..."
    )

    ws = None

    try:

        ws = connect_deriv()

        ws.send(json.dumps({
            "ticks_history": SYMBOL,
            "count": HISTORY_COUNT,
            "end": "latest",
            "style": "ticks",
            "req_id": 1001
        }))

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

                    price = float(price)

                    if price > 0:

                        rows.append({
                            "epoch": int(timestamp),
                            "price": price,
                            "source": "history"
                        })

                except (ValueError, TypeError):
                    continue

            if not rows:
                raise RuntimeError("Historique Deriv vide.")

            rows.sort(key=lambda x: x["epoch"])

            print("Historique récupéré :", len(rows), "ticks")

            return rows

        raise TimeoutError(
            "Aucune réponse historique après 60 secondes."
        )

    finally:

        if ws is not None:
            try:
                ws.close()
            except Exception:
                pass


# ============================================================
# COLLECTE LIVE
# MÉTHODE IDENTIQUE AU TEST DERIV RÉUSSI
# ============================================================

def collect_live():

    print(
        f"Collecte des ticks en direct pendant "
        f"{COLLECT_SECONDS} secondes..."
    )

    rows = []
    ws = None
    subscription_id = None

    try:

        # Connexion identique au test réussi.
        ws = websocket.create_connection(
            WS_URL,
            timeout=30
        )

        print("Connexion live réussie.")

        # Abonnement identique au test réussi.
        ws.send(json.dumps({
            "ticks": SYMBOL,
            "subscribe": 1,
            "req_id": 2001
        }))

        print("Abonnement envoyé.")

        deadline = time.monotonic() + COLLECT_SECONDS

        while time.monotonic() < deadline:

            try:

                raw = ws.recv()

                if not raw:
                    continue

                data = json.loads(raw)

                if "error" in data:

                    print("Erreur Deriv :", data["error"])
                    continue

                if data.get("subscription"):

                    subscription_id = data[
                        "subscription"
                    ].get("id", subscription_id)

                tick = data.get("tick")

                if tick:

                    try:

                        epoch = int(tick["epoch"])
                        price = float(tick["quote"])

                        if price > 0:

                            rows.append({
                                "epoch": epoch,
                                "price": price,
                                "source": "live"
                            })

                            if len(rows) % 10 == 0:

                                print(
                                    "Ticks live reçus :",
                                    len(rows)
                                )

                    except (KeyError, ValueError, TypeError):
                        continue

            except websocket.WebSocketTimeoutException:

                print("En attente des prochains ticks...")
                continue

            except websocket.WebSocketConnectionClosedException as exc:

                print(
                    "Connexion live interrompue :",
                    repr(exc)
                )

                break

            except Exception as exc:

                print(
                    "Erreur réception live :",
                    repr(exc)
                )

                break

    except Exception as exc:

        print("Erreur collecte live :", repr(exc))

    finally:

        if ws is not None:

            if subscription_id:

                try:

                    ws.send(json.dumps({
                        "forget": subscription_id,
                        "req_id": 2002
                    }))

                except Exception:
                    pass

            try:
                ws.close()
            except Exception:
                pass

    print("Collecte live terminée :", len(rows), "ticks.")

    return rows


# ============================================================
# FUSION DES DONNEES
# ============================================================

def merge_ticks(db, new_rows):

    existing = {}

    for row in db["ticks"]:

        try:

            epoch = int(row["epoch"])
            price = float(row["price"])

            if price > 0:

                existing[(epoch, price)] = {
                    "epoch": epoch,
                    "price": price,
                    "source": row.get("source", "previous")
                }

        except Exception:
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

        except Exception:
            continue

    ordered = sorted(
        existing.values(),
        key=lambda x: (x["epoch"], x["price"])
    )

    db["ticks"] = ordered[-100000:]

    print("Total ticks uniques :", len(db["ticks"]))


# ============================================================
# STATISTIQUES
# ============================================================

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


def calculate_returns(prices):

    returns = []

    for i in range(1, len(prices)):

        previous = prices[i - 1]
        current = prices[i]

        if previous > 0:

            returns.append(
                (current - previous) / previous
            )

        else:

            returns.append(0.0)

    return returns


# ============================================================
# DETECTION DES SPIKES
# ============================================================

def detect_spikes(prices, epochs):

    returns = calculate_returns(prices)

    positive = [
        value for value in returns if value > 0
    ]

    threshold = percentile(positive, 0.995)

    if threshold <= 0:
        return threshold, []

    spikes = []

    for i in range(1, len(prices)):

        previous = prices[i - 1]

        if previous <= 0:
            continue

        variation = (
            prices[i] - previous
        ) / previous

        if variation >= threshold:

            spikes.append({
                "index": i,
                "epoch": int(epochs[i]),
                "variation": variation,
                "price_before": previous,
                "price_after": prices[i]
            })

    return threshold, spikes


# ============================================================
# CARACTERISTIQUES STATISTIQUES
# ============================================================

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

    return (
        min(4, negative_count // 2),
        min(4, int(average_movement * 100000)),
        min(4, int(volatility * 100000)),
        -1 if momentum10 < -0.0002 else
        1 if momentum10 > 0.0002 else 0,
        -1 if momentum30 < -0.0005 else
        1 if momentum30 > 0.0005 else 0
    )


# ============================================================
# ANALYSE COMPLETE
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

    if len(prices) < 100:

        db["analysis"] = {
            "status": "collecting",
            "total_ticks": len(prices),
            "message": "Données insuffisantes."
        }

        db["prediction"] = {
            "result": None,
            "warning": "Au moins 100 ticks nécessaires."
        }

        print("Données insuffisantes pour analyser.")

        return

    print("Analyse statistique en cours...")

    returns = calculate_returns(prices)

    threshold, spikes = detect_spikes(
        prices,
        epochs
    )

    spike_indices = {
        item["index"] for item in spikes
    }

    samples = []

    for i in range(30, len(prices) - FUTURE_TICKS):

        features = get_features(returns, i)

        if features is None:
            continue

        target = any(
            i < spike <= i + FUTURE_TICKS
            for spike in spike_indices
        )

        samples.append({
            "index": i,
            "features": features,
            "target": target
        })

    split = int(len(samples) * 0.70)

    training = samples[:split]
    testing = samples[split:]

    patterns = {}

    for item in training:

        key = json.dumps(item["features"])

        pattern = patterns.setdefault(
            key,
            {"positive": 0, "total": 0}
        )

        pattern["total"] += 1

        if item["target"]:
            pattern["positive"] += 1

    tp = fp = fn = tn = 0

    for item in testing:

        key = json.dumps(item["features"])

        pattern = patterns.get(
            key,
            {"positive": 0, "total": 0}
        )

        total = pattern["total"]

        probability = (
            pattern["positive"] / total
            if total else 0.0
        )

        predicted = (
            total >= 20 and probability >= 0.10
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
        if tp + fp else 0.0
    )

    recall = (
        tp / (tp + fn)
        if tp + fn else 0.0
    )

    false_positive_rate = (
        fp / (fp + tn)
        if fp + tn else 0.0
    )

    best_patterns = []

    for key, value in patterns.items():

        total = value["total"]

        if total < 20:
            continue

        best_patterns.append({
            "configuration": json.loads(key),
            "observations": total,
            "spikes": value["positive"],
            "probability": round(
                value["positive"] / total,
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
            "epoch": item["epoch"],
            "variation": item["variation"],
            "price_before": item["price_before"],
            "price_after": item["price_after"]
        }
        for item in spikes[-5000:]
    ]

    db["analysis"] = {
        "status": "completed",
        "updated_at": utc_now(),
        "symbol": SYMBOL,
        "total_ticks": len(prices),
        "total_spikes": len(spikes),
        "spike_threshold": threshold,
        "training_samples": len(training),
        "testing_samples": len(testing),
        "true_positive": tp,
        "false_positive": fp,
        "false_negative": fn,
        "true_negative": tn,
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "false_positive_rate": round(
            false_positive_rate,
            6
        ),
        "top_patterns": best_patterns[:30],
        "note": (
            "Analyse statistique expérimentale. "
            "Aucune garantie sur les mouvements futurs."
        )
    }

    latest_features = get_features(
        returns,
        len(prices) - 1
    )

    prediction = None

    if latest_features is not None:

        key = json.dumps(latest_features)

        pattern = patterns.get(
            key,
            {"positive": 0, "total": 0}
        )

        total = pattern["total"]

        if total >= 20:

            probability = (
                pattern["positive"] / total
            )

            prediction = {
                "configuration": latest_features,
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
                "configuration": latest_features,
                "observations": total,
                "historical_probability": None,
                "signal": "DONNEES INSUFFISANTES"
            }

    db["prediction"] = {
        "updated_at": utc_now(),
        "result": prediction,
        "warning": (
            "Résultat statistique expérimental, "
            "sans garantie de prédiction."
        )
    }

    print("Analyse terminée.")
    print("Spikes détectés :", len(spikes))
    print("Précision :", round(precision * 100, 2), "%")
    print("Rappel :", round(recall * 100, 2), "%")


# ============================================================
# EXECUTION PRINCIPALE
# ============================================================

def main():

    print("=" * 55)
    print("SPIKE1000 - COLLECTE ET ANALYSE")
    print("DERIV PUBLIC API - READ ONLY")
    print("=" * 55)

    db = load_db()

    # ETAPE 1 : HISTORIQUE

    try:

        history = collect_history()

        merge_ticks(db, history)

        save_db(db)

    except Exception as exc:

        print("ERREUR HISTORIQUE :", repr(exc))

        record_error(db, repr(exc))

        if not db["ticks"]:

            raise RuntimeError(
                "Impossible de récupérer les données historiques."
            )

    # ETAPE 2 : COLLECTE EN DIRECT

    try:

        live = collect_live()

        merge_ticks(db, live)

        save_db(db)

    except Exception as exc:

        print(
            "Collecte live indisponible :",
            repr(exc)
        )

        record_error(db, repr(exc))

    # ETAPE 3 : ANALYSE

    try:

        analyze_data(db)

    except Exception as exc:

        print("ERREUR ANALYSE :", repr(exc))

        record_error(db, repr(exc))

        raise

    # ETAPE 4 : SAUVEGARDE

    save_db(db)

    print("=" * 55)
    print("SPIKE1000 TERMINE")
    print("Ticks :", len(db["ticks"]))
    print("Spikes :", len(db["spikes"]))
    print("Fichier :", DB_FILE)
    print("=" * 55)


if __name__ == "__main__":

    try:
        main()

    except Exception as exc:

        print("ECHEC DU PROGRAMME :", repr(exc))
        sys.exit(1)