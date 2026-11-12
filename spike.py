import os
import sys
import json
import time
import statistics
import websocket

from pathlib import Path
from datetime import datetime, timezone


# ============================================================
# CONFIGURATION
# ============================================================

VERSION = 5

SYMBOL = os.getenv("DERIV_SYMBOL", "BOOM1000")

HISTORY_COUNT = int(os.getenv("HISTORY_COUNT", "5000"))
COLLECT_SECONDS = int(os.getenv("COLLECT_SECONDS", "180"))
FUTURE_TICKS = int(os.getenv("FUTURE_TICKS", "20"))

MAX_TICKS = 100000
TRAIN_RATIO = 0.70
MIN_PATTERN_OBSERVATIONS = 20
MIN_PATTERN_PROBABILITY = 0.10

WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"

DB_FILE = Path(__file__).resolve().parent / "db.json"


# ============================================================
# DATE
# ============================================================

def now_iso():
    return datetime.now(timezone.utc).isoformat()


# ============================================================
# DATABASE
# ============================================================

def empty_db():
    return {
        "version": VERSION,
        "symbol": SYMBOL,
        "created_at": now_iso(),
        "updated_at": now_iso(),
        "ticks": [],
        "spikes": [],
        "analysis": {},
        "prediction": {},
        "errors": []
    }


def load_db():
    if not DB_FILE.exists():
        print("Nouvelle base de données.")
        return empty_db()

    try:
        with open(DB_FILE, "r", encoding="utf-8") as file:
            db = json.load(file)

        if not isinstance(db, dict):
            return empty_db()

        defaults = empty_db()

        for key, value in defaults.items():
            if key not in db:
                db[key] = value

        if not isinstance(db["ticks"], list):
            db["ticks"] = []

        if not isinstance(db["spikes"], list):
            db["spikes"] = []

        if not isinstance(db["errors"], list):
            db["errors"] = []

        db["version"] = VERSION
        db["symbol"] = SYMBOL

        return db

    except Exception as error:
        print("Erreur lecture DB:", error)
        return empty_db()


def save_db(db):
    db["updated_at"] = now_iso()

    temp_file = DB_FILE.with_suffix(".tmp")

    with open(temp_file, "w", encoding="utf-8") as file:
        json.dump(db, file, indent=2, ensure_ascii=False)
        file.flush()
        os.fsync(file.fileno())

    os.replace(temp_file, DB_FILE)

    print("Base sauvegardée.")
    print("Ticks enregistrés:", len(db["ticks"]))


def record_error(db, message):
    db["errors"].append({
        "time": now_iso(),
        "message": str(message)
    })

    db["errors"] = db["errors"][-100:]


# ============================================================
# CONNEXION DERIV
# ============================================================

def connect_deriv():
    delays = [5, 10, 20, 30, 60]

    for attempt, delay in enumerate(delays, start=1):
        try:
            print("Connexion Deriv:", attempt)

            ws = websocket.create_connection(
                WS_URL,
                timeout=30
            )

            print("Connexion réussie.")
            return ws

        except Exception as error:
            print("Connexion échouée:", error)

            if attempt < len(delays):
                time.sleep(delay)

    raise RuntimeError("Impossible de se connecter à Deriv.")


def receive_message(ws, timeout=30):
    ws.settimeout(timeout)

    while True:
        raw = ws.recv()
        data = json.loads(raw)

        if "error" in data:
            raise RuntimeError(data["error"].get("message", "Erreur Deriv"))

        return data


# ============================================================
# HISTORIQUE
# ============================================================

def collect_history():
    print("\nCollecte historique:", SYMBOL)

    ws = connect_deriv()

    try:
        request = {
            "ticks_history": SYMBOL,
            "count": HISTORY_COUNT,
            "end": "latest",
            "style": "ticks",
            "req_id": 1001
        }

        ws.send(json.dumps(request))

        while True:
            data = receive_message(ws)

            history = data.get("history")

            if history:
                prices = history.get("prices", [])
                times = history.get("times", [])

                rows = []

                for epoch, price in zip(times, prices):
                    rows.append({
                        "epoch": int(epoch),
                        "price": float(price),
                        "source": "history"
                    })

                print("Ticks historiques reçus:", len(rows))
                return rows

    finally:
        ws.close()


# ============================================================
# COLLECTE EN DIRECT
# ============================================================

def collect_live():
    print("\nCollecte en direct:", COLLECT_SECONDS, "secondes")

    ws = connect_deriv()

    rows = []
    start = time.time()

    try:
        request = {
            "ticks": SYMBOL,
            "subscribe": 1,
            "req_id": 2001
        }

        ws.send(json.dumps(request))
        ws.settimeout(10)

        while time.time() - start < COLLECT_SECONDS:
            try:
                raw = ws.recv()
                data = json.loads(raw)

                if "error" in data:
                    raise RuntimeError(
                        data["error"].get("message", "Erreur Deriv")
                    )

                tick = data.get("tick")

                if tick:
                    rows.append({
                        "epoch": int(tick["epoch"]),
                        "price": float(tick["quote"]),
                        "source": "live"
                    })

                    print(
                        "Tick:",
                        tick["epoch"],
                        tick["quote"]
                    )

            except websocket.WebSocketTimeoutException:
                continue

        print("Ticks live reçus:", len(rows))

    except Exception as error:
        print("Collecte live interrompue:", error)

    finally:
        try:
            ws.close()
        except Exception:
            pass

    return rows


# ============================================================
# FUSION DES DONNÉES
# ============================================================

def merge_ticks(db, new_rows):
    combined = {}

    for row in db["ticks"] + new_rows:
        try:
            epoch = int(row["epoch"])
            price = float(row["price"])

            key = (epoch, price)

            combined[key] = {
                "epoch": epoch,
                "price": price
            }

        except (KeyError, TypeError, ValueError):
            continue

    result = sorted(
        combined.values(),
        key=lambda item: item["epoch"]
    )

    db["ticks"] = result[-MAX_TICKS:]

    return db["ticks"]


# ============================================================
# CALCULS STATISTIQUES
# ============================================================

def percentile(values, percentage):
    if not values:
        return 0.0

    values = sorted(values)

    position = (len(values) - 1) * percentage / 100

    lower = int(position)
    upper = min(lower + 1, len(values) - 1)

    fraction = position - lower

    return (
        values[lower] * (1 - fraction)
        + values[upper] * fraction
    )


def calculate_returns(prices):
    returns = []

    for i in range(1, len(prices)):
        previous = prices[i - 1]
        current = prices[i]

        if previous <= 0:
            returns.append(0.0)
        else:
            returns.append((current - previous) / previous)

    return returns


# ============================================================
# CONFIGURATIONS AVANT SPIKE
# ============================================================

def get_features(returns, index, threshold):
    windows = [1, 3, 5, 10, 30]

    features = []

    for window in windows:
        start = max(0, index - window)

        previous_returns = returns[start:index]

        if not previous_returns:
            features.append(0)
            continue

        maximum = max(
            [max(0.0, value) for value in previous_returns]
        )

        if threshold <= 0:
            ratio = 0
        else:
            ratio = maximum / threshold

        if ratio < 0.10:
            category = 0
        elif ratio < 0.25:
            category = 1
        elif ratio < 0.50:
            category = 2
        elif ratio < 1.00:
            category = 3
        else:
            category = 4

        features.append(category)

    return tuple(features)


# ============================================================
# APPRENTISSAGE ET TEST
# ============================================================

def analyze_data(db):
    ticks = db["ticks"]

    if len(ticks) < 100:
        print("Pas assez de données pour analyser.")
        db["analysis"] = {
            "status": "insufficient_data",
            "total_ticks": len(ticks)
        }
        return

    prices = [float(tick["price"]) for tick in ticks]
    epochs = [int(tick["epoch"]) for tick in ticks]

    returns = calculate_returns(prices)

    if len(returns) < 100:
        return

    # --------------------------------------------------------
    # Séparation chronologique
    # --------------------------------------------------------

    split_index = int(len(returns) * TRAIN_RATIO)

    training_returns = returns[:split_index]

    positive_training_returns = [
        value for value in training_returns
        if value > 0
    ]

    if len(positive_training_returns) < 10:
        print("Pas assez de mouvements positifs.")
        return

    # Le seuil est appris uniquement sur la période
    # d'entraînement. Il ne regarde pas les données futures.

    spike_threshold = percentile(
        positive_training_returns,
        99.5
    )

    if spike_threshold <= 0:
        return

    # --------------------------------------------------------
    # Détection des spikes avec seuil fixe
    # --------------------------------------------------------

    spike_indices = [
        index
        for index, value in enumerate(returns)
        if value >= spike_threshold
    ]

    db["spikes"] = [
        {
            "epoch": epochs[index + 1],
            "variation": returns[index],
            "price_before": prices[index],
            "price_after": prices[index + 1]
        }
        for index in spike_indices
    ]

    # --------------------------------------------------------
    # Construction des échantillons
    # --------------------------------------------------------

    samples = []

    for index in range(30, len(returns) - FUTURE_TICKS):
        features = get_features(
            returns,
            index,
            spike_threshold
        )

        future_end = index + FUTURE_TICKS

        target = any(
            index <= spike_index < future_end
            for spike_index in spike_indices
        )

        samples.append({
            "index": index,
            "epoch": epochs[index],
            "features": features,
            "target": int(target)
        })

    if len(samples) < 50:
        print("Pas assez d'échantillons.")
        return

    # --------------------------------------------------------
    # Séparation apprentissage / test
    # --------------------------------------------------------

    sample_split = int(len(samples) * TRAIN_RATIO)

    training_samples = samples[:sample_split]
    testing_samples = samples[sample_split:]

    # --------------------------------------------------------
    # Apprentissage des configurations
    # --------------------------------------------------------

    patterns = {}

    for sample in training_samples:
        key = str(list(sample["features"]))

        if key not in patterns:
            patterns[key] = {
                "observations": 0,
                "spikes": 0
            }

        patterns[key]["observations"] += 1
        patterns[key]["spikes"] += sample["target"]

    for pattern in patterns.values():
        observations = pattern["observations"]
        spikes = pattern["spikes"]

        pattern["probability"] = (
            spikes / observations
            if observations else 0
        )

    # --------------------------------------------------------
    # Évaluation sur données inconnues
    # --------------------------------------------------------

    true_positive = 0
    false_positive = 0
    false_negative = 0
    true_negative = 0

    for sample in testing_samples:
        key = str(list(sample["features"]))

        pattern = patterns.get(key)

        prediction = False

        if pattern:
            if (
                pattern["observations"]
                >= MIN_PATTERN_OBSERVATIONS
                and pattern["probability"]
                >= MIN_PATTERN_PROBABILITY
            ):
                prediction = True

        actual = bool(sample["target"])

        if prediction and actual:
            true_positive += 1

        elif prediction and not actual:
            false_positive += 1

        elif not prediction and actual:
            false_negative += 1

        else:
            true_negative += 1

    total_test = len(testing_samples)

    accuracy = (
        (true_positive + true_negative) / total_test
        if total_test else 0
    )

    precision = (
        true_positive / (true_positive + false_positive)
        if true_positive + false_positive else 0
    )

    recall = (
        true_positive / (true_positive + false_negative)
        if true_positive + false_negative else 0
    )

    false_positive_rate = (
        false_positive / (false_positive + true_negative)
        if false_positive + true_negative else 0
    )

    # --------------------------------------------------------
    # Classement des configurations
    # --------------------------------------------------------

    top_patterns = []

    for configuration, values in patterns.items():
        top_patterns.append({
            "configuration": json.loads(configuration),
            "observations": values["observations"],
            "spikes": values["spikes"],
            "probability": round(
                values["probability"],
                6
            )
        })

    top_patterns.sort(
        key=lambda item: (
            item["probability"],
            item["observations"]
        ),
        reverse=True
    )

    # --------------------------------------------------------
    # Analyse de la configuration actuelle
    # --------------------------------------------------------

    current_features = get_features(
        returns,
        len(returns),
        spike_threshold
    )

    current_key = str(list(current_features))

    current_pattern = patterns.get(current_key)

    if current_pattern:
        current_probability = current_pattern["probability"]
        current_observations = current_pattern["observations"]
    else:
        current_probability = 0
        current_observations = 0

    if (
        current_observations >= MIN_PATTERN_OBSERVATIONS
        and current_probability >= MIN_PATTERN_PROBABILITY
    ):
        signal = "CONFIGURATION A SURVEILLER"
    else:
        signal = "AUCUN SIGNAL FORT"

    # --------------------------------------------------------
    # Résultats
    # --------------------------------------------------------

    training_spikes = sum(
        sample["target"]
        for sample in training_samples
    )

    testing_spikes = sum(
        sample["target"]
        for sample in testing_samples
    )

    db["analysis"] = {
        "status": "completed",
        "version": VERSION,
        "total_ticks": len(ticks),
        "total_samples": len(samples),

        "training_samples": len(training_samples),
        "testing_samples": len(testing_samples),

        "training_spikes": training_spikes,
        "testing_spikes": testing_spikes,
        "total_spikes_detected": len(spike_indices),

        "spike_threshold": spike_threshold,

        "true_positive": true_positive,
        "false_positive": false_positive,
        "false_negative": false_negative,
        "true_negative": true_negative,

        "accuracy": round(accuracy, 6),
        "precision": round(precision, 6),
        "recall": round(recall, 6),
        "false_positive_rate": round(
            false_positive_rate,
            6
        ),

        "data_start_epoch": epochs[0],
        "data_end_epoch": epochs[-1],

        "top_patterns": top_patterns[:20],

        "note": (
            "Analyse statistique expérimentale. "
            "Le seuil est appris sur les données "
            "d'entraînement. Aucun résultat ne garantit "
            "les mouvements futurs."
        )
    }

    db["prediction"] = {
        "updated_at": now_iso(),
        "result": {
            "configuration": list(current_features),
            "observations": current_observations,
            "historical_probability": round(
                current_probability,
                6
            ),
            "signal": signal
        }
    }

    print("\n========== ANALYSE SPIKE1000 ==========")
    print("Ticks:", len(ticks))
    print("Spikes détectés:", len(spike_indices))
    print("Spikes apprentissage:", training_spikes)
    print("Spikes test:", testing_spikes)
    print("Précision:", round(precision * 100, 2), "%")
    print("Rappel:", round(recall * 100, 2), "%")
    print("Exactitude:", round(accuracy * 100, 2), "%")
    print("Signal:", signal)
    print("=======================================")


# ============================================================
# PROGRAMME PRINCIPAL
# ============================================================

def main():
    print("\n===================================")
    print("       SPIKE1000 ENGINE V5")
    print("===================================")

    db = load_db()

    print("Symbole:", SYMBOL)
    print("Ticks existants:", len(db["ticks"]))

    # 1. Collecte historique

    try:
        history = collect_history()
        merge_ticks(db, history)
        save_db(db)

    except Exception as error:
        print("Erreur historique:", error)
        record_error(db, error)
        save_db(db)

    # 2. Collecte en direct

    try:
        live = collect_live()
        merge_ticks(db, live)
        save_db(db)

    except Exception as error:
        print("Erreur live:", error)
        record_error(db, error)
        save_db(db)

    # 3. Analyse statistique

    try:
        analyze_data(db)

    except Exception as error:
        print("Erreur analyse:", error)
        record_error(db, error)

    # 4. Sauvegarde finale

    save_db(db)

    print("\nSPIKE1000 terminé.")
    print("Mode: lecture seule.")
    print("Aucun trade exécuté.")


if __name__ == "__main__":
    main()