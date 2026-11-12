#!/usr/bin/env python3
# ============================================================
# SPIKE1000 - COLLECTE ET ANALYSE STATISTIQUE
# Lecture seule : aucun trade, paiement ou retrait.
# Nouvelle API publique Deriv.
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
MANUAL_THRESHOLD = os.getenv("SPIKE_THRESHOLD", "").strip()

WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"

BASE_DIR = Path(__file__).resolve().parent
DB_FILE = BASE_DIR / "db.json"


# ============================================================
# UTILITAIRES ET BASE JSON
# ============================================================

def utc_now():
    return datetime.now(timezone.utc).isoformat()


def empty_db():
    return {
        "version": 2,
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
            raise ValueError("La racine de db.json doit être un objet JSON.")

    except (json.JSONDecodeError, OSError, ValueError) as exc:
        backup = DB_FILE.with_name(
            "db.corrupt-" + str(int(time.time())) + ".json"
        )

        try:
            DB_FILE.replace(backup)
            print("Ancien db.json déplacé vers :", backup.name)
        except OSError:
            print("Impossible de déplacer db.json :", exc)

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

    with temporary.open("w", encoding="utf-8") as f:
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
        ws = None

        try:
            print(
                f"Connexion nouvelle API Deriv : "
                f"tentative {attempt}/{len(delays)}"
            )

            ws = websocket.create_connection(
                WS_URL,
                timeout=30,
                enable_multithread=True,
                origin="https://app.deriv.com"
            )

            ws.settimeout(2)

            print("WebSocket connecté à la nouvelle API.")
            print("Mode public : lecture seule, sans authentification.")

            return ws

        except Exception as exc:
            last_error = exc

            print("Erreur de connexion :", repr(exc))

            if ws is not None:
                try:
                    ws.close()
                except Exception:
                    pass

            if attempt < len(delays):
                print(f"Nouvelle tentative dans {delay} secondes...")
                time.sleep(delay)

    raise RuntimeError(
        f"Connexion à Deriv impossible après "
        f"{len(delays)} tentatives : {last_error}"
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
                err = data.get("error") or {}

                raise RuntimeError(
                    err.get("message", "Erreur renvoyée par Deriv")
                )

            return data

        except websocket.WebSocketTimeoutException:
            continue

    return None


def send_request(ws, payload):
    ws.send(json.dumps(payload))


# ============================================================
# HISTORIQUE
# ============================================================

def collect_history(ws):
    print(
        f"Demande de {HISTORY_COUNT} ticks historiques "
        f"pour {SYMBOL}..."
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

                if p > 0:
                    rows.append({
                        "epoch": int(timestamp),
                        "price": p,
                        "source": "history"
                    })

            except (TypeError, ValueError):
                continue

        if not rows:
            raise RuntimeError(
                "Deriv a répondu avec un historique vide."
            )

        rows.sort(key=lambda row: row["epoch"])

        print("Historique récupéré :", len(rows), "ticks")

        return rows

    raise TimeoutError(
        "Aucune réponse d'historique reçue dans les 60 secondes."
    )


# ============================================================
# COLLECTE EN DIRECT
# ============================================================

def collect_live(ws):
    print(
        f"Collecte des ticks en direct pendant "
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
            subscription_id = data["subscription"].get(
                "id",
                subscription_id
            )

        tick = data.get("tick")

        if not tick:
            continue

        try:
            epoch = int(tick["epoch"])
            quote = float(tick["quote"])

            if quote > 0:
                rows.append({
                    "epoch": epoch,
                    "price": quote,
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
                "Avertissement : désabonnement non confirmé :",
                exc
            )

    print("Ticks en direct reçus :", len(rows))

    return rows


# ============================================================
# FUSION ET NETTOYAGE
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
        "Total de ticks uniques dans db.json :",
        len(db["ticks"])
    )


# ============================================================
# OUTILS STATISTIQUES
# ============================================================

def percentile(values, q):
    if not values:
        return 0.0

    ordered = sorted(values)

    position = (len(ordered) - 1) * q

    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)

    fraction = position - lower

    return (
        ordered[lower]
        + (ordered[upper] - ordered[lower]) * fraction
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


def get_features_from_returns(returns, index):
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
# DETECTION DES SPIKES
# ============================================================

def detect_spikes(prices, epochs):
    returns = calculate_returns(prices)

    positive_returns = [
        value for value in returns if value > 0
    ]

    if MANUAL_THRESHOLD:
        threshold = float(MANUAL_THRESHOLD)

        if threshold <= 0:
            raise ValueError(
                "SPIKE_THRESHOLD doit être supérieur à zéro."
            )

    else:
        threshold = percentile(
            positive_returns,
            0.995
        )

    threshold = max(threshold, 0.00000001)

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
# ANALYSE CHRONOLOGIQUE
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
            "message": (
                "Données insuffisantes : "
                "au moins 100 ticks sont nécessaires."
            )
        }

        db["prediction"] = {
            "result": None,
            "warning": (
                "Pas assez de données pour produire une estimation."
            )
        }

        print("Analyse reportée : données insuffisantes.")

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

    labeled = []

    for i in range(30, len(prices) - FUTURE_TICKS):
        features = get_features_from_returns(
            returns,
            i
        )

        if features is None:
            continue

        target = any(
            i < spike_index <= i + FUTURE_TICKS
            for spike_index in spike_indices
        )

        labeled.append({
            "index": i,
            "features": features,
            "target": target
        })

    split = int(len(labeled) * 0.70)

    training = labeled[:split]
    testing = labeled[split:]

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

        rate = (
            pattern["positive"] / total
            if total else 0.0
        )

        predicted = (
            total >= 20 and rate >= 0.10
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
        key=lambda row: (
            row["probability"],
            row["observations"]
        ),
        reverse=True
    )

    db["spikes"] = [
        {
            "tick_index": item["index"],
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
        "future_ticks": FUTURE_TICKS,
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
        "method_note": (
            "Analyse statistique expérimentale. "
            "Les résultats historiques ne garantissent "
            "pas les mouvements futurs."
        )
    }

    latest_features = get_features_from_returns(
        returns,
        len(prices) - 1
    )

    result = None

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

            result = {
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
            result = {
                "configuration": latest_features,
                "observations": total,
                "historical_probability": None,
                "signal": "DONNEES INSUFFISANTES"
            }

    db["prediction"] = {
        "updated_at": utc_now(),
        "result": result,
        "warning": (
            "Estimation statistique expérimentale, non garantie. "
            "Ce programme ne passe aucun ordre."
        )
    }

    print("Analyse terminée.")
    print("Spikes détectés :", len(spikes))

    print(
        "Précision historique test :",
        round(precision * 100, 2),
        "%"
    )

    print(
        "Rappel historique test :",
        round(recall * 100, 2),
        "%"
    )


# ============================================================
# EXECUTION PRINCIPALE
# ============================================================

def main():
    print("=" * 58)

    print(
        "SPIKE1000 — MOTEUR DE COLLECTE ET D'ANALYSE"
    )

    print(
        "Deriv nouvelle API publique — lecture seule"
    )

    print("=" * 58)

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

        print("=" * 58)
        print("EXECUTION TERMINEE")

        print(
            "Ticks enregistrés :",
            len(db["ticks"])
        )

        print(
            "Spikes enregistrés :",
            len(db["spikes"])
        )

        print("Fichier :", DB_FILE)

        print("=" * 58)

    except KeyboardInterrupt:
        print("Arrêt demandé.")
        save_db(db)

    except Exception as exc:
        print("ERREUR :", repr(exc))

        try:
            record_error(db, repr(exc))

        except Exception as save_exc:
            print(
                "Impossible de sauvegarder l'erreur :",
                repr(save_exc)
            )

        raise

    finally:
        if ws is not None:
            try:
                ws.close()

            except Exception:
                pass


if __name__ == "__main__":
    main()
