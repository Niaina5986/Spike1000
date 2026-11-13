import json
import os
import time
from datetime import datetime, timezone

import websocket


# =====================================================
# CONFIGURATION
# =====================================================

SYMBOL = "BOOM1000"
GRANULARITY = 60
CANDLE_COUNT = 1000
FUTURE_CANDLES = 5

WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"
DB_FILE = "db.json"


# =====================================================
# CONNEXION DERIV
# =====================================================

def get_candles():
    last_error = None

    for attempt in range(3):
        ws = None

        try:
            ws = websocket.create_connection(
                WS_URL,
                timeout=25
            )

            request = {
                "ticks_history": SYMBOL,
                "adjust_start_time": 1,
                "count": CANDLE_COUNT,
                "end": "latest",
                "granularity": GRANULARITY,
                "style": "candles",
                "req_id": 1001
            }

            ws.send(json.dumps(request))

            while True:
                response = json.loads(ws.recv())

                if "error" in response:
                    raise RuntimeError(
                        response["error"].get("message", "Deriv error")
                    )

                if response.get("msg_type") == "candles":
                    candles = response.get("candles", [])

                    if len(candles) < 150:
                        raise RuntimeError("Pas assez de bougies reçues")

                    return sorted(
                        candles,
                        key=lambda x: int(x["epoch"])
                    )

        except Exception as exc:
            last_error = str(exc)
            time.sleep(3)

        finally:
            if ws:
                try:
                    ws.close()
                except Exception:
                    pass

    raise RuntimeError(f"Connexion Deriv impossible : {last_error}")


# =====================================================
# INDICATEURS
# =====================================================

def calculate_indicators(candles):
    closes = [float(c["close"]) for c in candles]
    highs = [float(c["high"]) for c in candles]
    lows = [float(c["low"]) for c in candles]
    opens = [float(c["open"]) for c in candles]

    def ema(values, period):
        result = [None] * len(values)

        if len(values) < period:
            return result

        current = sum(values[:period]) / period
        result[period - 1] = current

        multiplier = 2 / (period + 1)

        for i in range(period, len(values)):
            current = (
                values[i] * multiplier
                + current * (1 - multiplier)
            )
            result[i] = current

        return result

    ema5 = ema(closes, 5)
    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)

    tr = []

    for i in range(len(candles)):
        if i == 0:
            tr.append(highs[i] - lows[i])
        else:
            tr.append(max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1])
            ))

    atr = [None] * len(candles)

    for i in range(13, len(candles)):
        atr[i] = sum(tr[i - 13:i + 1]) / 14

    rsi = [None] * len(candles)

    for i in range(14, len(candles)):
        gains = []
        losses = []

        for j in range(i - 13, i + 1):
            change = closes[j] - closes[j - 1]
            gains.append(max(change, 0))
            losses.append(max(-change, 0))

        avg_gain = sum(gains) / 14
        avg_loss = sum(losses) / 14

        if avg_loss == 0:
            rsi[i] = 100
        else:
            rs = avg_gain / avg_loss
            rsi[i] = 100 - (100 / (1 + rs))

    features = []

    for i in range(len(candles)):
        if (
            atr[i] is None
            or ema5[i] is None
            or ema20[i] is None
            or ema50[i] is None
            or rsi[i] is None
            or atr[i] <= 0
        ):
            features.append(None)
            continue

        a = atr[i]

        red_count = sum(
            1 for j in range(max(0, i - 4), i + 1)
            if closes[j] < opens[j]
        )

        green_count = sum(
            1 for j in range(max(0, i - 4), i + 1)
            if closes[j] > opens[j]
        )

        candle_range = max(highs[i] - lows[i], 0.000001)

        features.append({
            "rsi": round(rsi[i], 2),
            "ema20_distance": round((closes[i] - ema20[i]) / a, 3),
            "ema50_distance": round((closes[i] - ema50[i]) / a, 3),
            "change3": round(
                (closes[i] - closes[max(0, i - 3)]) / a, 3
            ),
            "body": round((closes[i] - opens[i]) / a, 3),
            "red_count": red_count,
            "green_count": green_count,
            "atr": round(a, 3)
        })

    return closes, highs, lows, atr, features


# =====================================================
# SEUIL AUTOMATIQUE DE VOLATILITE
# =====================================================

def calculate_threshold(atr, start, end):
    values = [
        x for x in atr[start:end]
        if x is not None and x > 0
    ]

    if len(values) < 20:
        raise RuntimeError("Historique ATR insuffisant")

    values.sort()

    median_atr = values[len(values) // 2]

    # Seuil dynamique : 2,5 fois l'ATR médian
    return median_atr * 2.5


# =====================================================
# DETECTION DES EVENEMENTS
# =====================================================

def detect_events(candles, highs, closes, atr, start, end):
    events = []

    for i in range(start, end - FUTURE_CANDLES):
        if atr[i] is None or atr[i] <= 0:
            continue

        threshold = atr[i] * 2.5

        future_high = max(
            highs[i + 1:i + 1 + FUTURE_CANDLES]
        )

        excursion = future_high - closes[i]

        if excursion >= threshold:
            events.append({
                "index": i,
                "epoch": int(candles[i]["epoch"]),
                "excursion": round(excursion, 3),
                "threshold": round(threshold, 3)
            })

    # Fusion des fenêtres qui correspondent à une même hausse.
    # Une hausse ne doit pas être comptée plusieurs fois.
    grouped = []

    for event in events:
        if (
            not grouped
            or event["index"] - grouped[-1]["last_index"]
            > FUTURE_CANDLES
        ):
            grouped.append({
                "index": event["index"],
                "epoch": event["epoch"],
                "excursion": event["excursion"],
                "threshold": event["threshold"],
                "last_index": event["index"]
            })
        else:
            grouped[-1]["last_index"] = event["index"]

            if event["excursion"] > grouped[-1]["excursion"]:
                grouped[-1]["excursion"] = event["excursion"]

    for event in grouped:
        event.pop("last_index", None)

    return grouped


# =====================================================
# ANALYSE DES PRECURSEURS
# =====================================================

def compare_precursors(features, events, start, end):
    fields = [
        "rsi",
        "ema20_distance",
        "ema50_distance",
        "change3",
        "body",
        "red_count",
        "green_count"
    ]

    before = {field: [] for field in fields}
    normal = {field: [] for field in fields}

    event_indexes = {
        event["index"] for event in events
    }

    for index in range(start, end):
        f = features[index]

        if f is None:
            continue

        destination = (
            before
            if index in event_indexes
            else normal
        )

        for field in fields:
            destination[field].append(f[field])

    comparison = {}

    for field in fields:
        if before[field] and normal[field]:
            comparison[field] = {
                "before_events": round(
                    sum(before[field]) / len(before[field]), 2
                ),
                "normal": round(
                    sum(normal[field]) / len(normal[field]), 2
                )
            }

    return comparison


# =====================================================
# RECHERCHE DE CONDITIONS
# =====================================================

def evaluate_conditions(features, events, start, end):
    event_indexes = [e["index"] for e in events]

    conditions = [
        ("RSI <= 25", lambda f: f["rsi"] <= 25),
        ("RSI <= 30", lambda f: f["rsi"] <= 30),
        ("RSI <= 35", lambda f: f["rsi"] <= 35),
        ("EMA50 distance <= -1", lambda f: f["ema50_distance"] <= -1),
        ("EMA50 distance <= -2", lambda f: f["ema50_distance"] <= -2),
        ("Change3 <= -1 ATR", lambda f: f["change3"] <= -1),
        ("Change3 <= -2 ATR", lambda f: f["change3"] <= -2),
        ("Red candles >= 4", lambda f: f["red_count"] >= 4)
    ]

    results = []

    for name, condition in conditions:
        signals = 0
        successes = 0

        for i in range(start, end - FUTURE_CANDLES):
            f = features[i]

            if f is None or not condition(f):
                continue

            signals += 1

            if any(
                i < event_index <= i + FUTURE_CANDLES
                for event_index in event_indexes
            ):
                successes += 1

        precision = (
            round(successes / signals * 100, 2)
            if signals else 0
        )

        results.append({
            "condition": name,
            "signals": signals,
            "successes": successes,
            "precision_percent": precision
        })

    results.sort(
        key=lambda x: (
            x["precision_percent"],
            x["successes"]
        ),
        reverse=True
    )

    return results


# =====================================================
# SURVEILLANCE ACTUELLE
# =====================================================

def current_status(features, training_patterns):
    latest = features[-1]

    if latest is None:
        return {
            "status": "WAIT",
            "reason": "Indicateurs indisponibles"
        }

    valid_patterns = [
        p for p in training_patterns
        if p["successes"] >= 2
        and p["signals"] >= 10
        and p["precision_percent"] >= 5
    ]

    if not valid_patterns:
        return {
            "status": "WAIT",
            "reason": "Aucun modèle suffisamment validé"
        }

    conditions = {
        "RSI <= 25": latest["rsi"] <= 25,
        "RSI <= 30": latest["rsi"] <= 30,
        "RSI <= 35": latest["rsi"] <= 35,
        "EMA50 distance <= -1": latest["ema50_distance"] <= -1,
        "EMA50 distance <= -2": latest["ema50_distance"] <= -2,
        "Change3 <= -1 ATR": latest["change3"] <= -1,
        "Change3 <= -2 ATR": latest["change3"] <= -2,
        "Red candles >= 4": latest["red_count"] >= 4
    }

    matches = [
        p for p in valid_patterns
        if conditions.get(p["condition"], False)
    ]

    if matches:
        return {
            "status": "SIGNAL",
            "matched_conditions": [
                p["condition"] for p in matches[:3]
            ],
            "reason": "Conditions historiques présentes"
        }

    return {
        "status": "NO SIGNAL",
        "reason": "Conditions historiques absentes"
    }


# =====================================================
# EXECUTION PRINCIPALE
# =====================================================

def main():
    print("Connexion à Deriv...")

    candles = get_candles()

    print(f"{len(candles)} bougies reçues.")

    closes, highs, lows, atr, features = calculate_indicators(candles)

    total = len(candles)

    train_end = int(total * 0.60)
    validation_end = int(total * 0.80)

    # Les fenêtres futures restent dans leur propre segment.
    training_events = detect_events(
        candles, highs, closes, atr, 50,
        train_end - FUTURE_CANDLES
    )

    validation_events = detect_events(
        candles, highs, closes, atr,
        train_end,
        validation_end - FUTURE_CANDLES
    )

    test_events = detect_events(
        candles, highs, closes, atr,
        validation_end,
        total - FUTURE_CANDLES
    )

    threshold = calculate_threshold(
        atr, 50, train_end
    )

    training_patterns = evaluate_conditions(
        features,
        training_events,
        50,
        train_end
    )

    validation_patterns = evaluate_conditions(
        features,
        validation_events,
        train_end,
        validation_end
    )

    test_patterns = evaluate_conditions(
        features,
        test_events,
        validation_end,
        total
    )

    precursor_comparison = compare_precursors(
        features,
        training_events,
        50,
        train_end
    )

    status = current_status(
        features,
        training_patterns
    )

    result = {
        "version": "V10",
        "updated_at": datetime.now(timezone.utc).isoformat(),
        "market": SYMBOL,
        "timeframe_seconds": GRANULARITY,
        "candles": total,
        "threshold_method": "ATR_AUTO_X_2.5",
        "training_threshold": round(threshold, 3),
        "future_window_minutes": FUTURE_CANDLES,
        "unique_events": {
            "training": len(training_events),
            "validation": len(validation_events),
            "test": len(test_events)
        },
        "best_patterns": {
            "training": training_patterns[:3],
            "validation": validation_patterns[:3],
            "test": test_patterns[:3]
        },
        "precursors": precursor_comparison,
        "current": status
    }

    with open(DB_FILE, "w", encoding="utf-8") as file:
        json.dump(result, file, indent=2, ensure_ascii=False)

    print("\n========== SPIKE1000 V10 ==========")
    print("Bougies :", total)
    print("Événements uniques :", len(training_events) +
          len(validation_events) + len(test_events))
    print("Training :", len(training_events))
    print("Validation :", len(validation_events))
    print("Test :", len(test_events))
    print("Seuil automatique :", round(threshold, 3))
    print("Statut actuel :", status["status"])
    print("Fichier db.json mis à jour.")
    print("===================================")


if __name__ == "__main__":
    try:
        main()
    except Exception as error:
        print("ERREUR :", str(error))
        raise

2. Vérifie "requirements.txt"

Il doit contenir :

websocket-client

3. Ne modifie pas ton workflow GitHub Actions

Garde ton workflow actuel s'il installe déjà "requirements.txt" et lance :

python spike.py

4. Résultat attendu

Le fichier "db.json" contiendra seulement les informations principales :

- Nombre d'événements uniques détectés.
- Événements en entraînement, validation et test.
- Trois meilleures conditions.
- Comparaison des précurseurs.
- État actuel : "SIGNAL", "WAIT" ou "NO SIGNAL".

Attention : "SIGNAL" signifie que des conditions historiques sont présentes, pas qu'une hausse est garantie. La validation et le test permettront de voir si ces conditions se reproduisent réellement.

Lance maintenant GitHub Actions et envoie-moi le résultat de "db.json". Je pourrai vérifier si V10 détecte de vrais événements ou s'il faut encore corriger son analyse.