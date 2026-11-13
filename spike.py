import json
import os
import time
import itertools
from datetime import datetime, timezone

import websocket

# ==========================================================
# SPIKE1000 V9 - PRECURSOR DETECTOR
# BOOM1000 - BOUGIES DE 1 MINUTE
# ANALYSE UNIQUEMENT - AUCUN TRADE
# ==========================================================

SYMBOL = "BOOM1000"
GRANULARITY = 60
CANDLE_COUNT = 1000
FUTURE_CANDLES = 5

WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"
DB_FILE = "db.json"

TRAIN_RATIO = 0.60
VALIDATION_RATIO = 0.20

MIN_TRAIN_EVENTS = 3
MIN_VALIDATION_EVENTS = 2


def now():
    return datetime.now(timezone.utc).isoformat()


def save_json(data):
    temp = DB_FILE + ".tmp"

    with open(temp, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    os.replace(temp, DB_FILE)


def load_previous():
    if not os.path.exists(DB_FILE):
        return {}

    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return {}


# ==========================================================
# RECUPERATION DES BOUGIES
# ==========================================================

def get_candles():

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

        deadline = time.time() + 30

        while time.time() < deadline:

            response = json.loads(ws.recv())

            if response.get("error"):
                raise RuntimeError(
                    response["error"].get("message", "Erreur Deriv")
                )

            if response.get("req_id") != 1001:
                continue

            raw = response.get("candles", [])

            if not raw:
                raise RuntimeError("Aucune bougie reçue.")

            candles = []

            for c in raw:

                candle = {
                    "epoch": int(c["epoch"]),
                    "open": float(c["open"]),
                    "high": float(c["high"]),
                    "low": float(c["low"]),
                    "close": float(c["close"])
                }

                if (
                    candle["low"] <= 0
                    or candle["high"] < candle["low"]
                    or candle["high"] < candle["open"]
                    or candle["high"] < candle["close"]
                    or candle["low"] > candle["open"]
                    or candle["low"] > candle["close"]
                ):
                    continue

                candles.append(candle)

            candles.sort(key=lambda x: x["epoch"])

            unique = {}

            for c in candles:
                unique[c["epoch"]] = c

            candles = list(unique.values())

            if len(candles) < 150:
                raise RuntimeError(
                    f"Historique insuffisant : {len(candles)}"
                )

            return candles

        raise TimeoutError("Délai Deriv dépassé.")

    finally:
        if ws:
            try:
                ws.close()
            except Exception:
                pass


# ==========================================================
# INDICATEURS
# ==========================================================

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


def rsi(values, period=14):

    result = [None] * len(values)

    if len(values) <= period:
        return result

    changes = [
        values[i] - values[i - 1]
        for i in range(1, len(values))
    ]

    gains = [max(x, 0) for x in changes]
    losses = [max(-x, 0) for x in changes]

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    result[period] = (
        100 if avg_loss == 0
        else 100 - 100 / (1 + avg_gain / avg_loss)
    )

    for i in range(period + 1, len(values)):

        avg_gain = (
            avg_gain * (period - 1) + gains[i - 1]
        ) / period

        avg_loss = (
            avg_loss * (period - 1) + losses[i - 1]
        ) / period

        result[i] = (
            100 if avg_loss == 0
            else 100 - 100 / (1 + avg_gain / avg_loss)
        )

    return result


def atr(highs, lows, closes, period=14):

    result = [None] * len(closes)
    trs = []

    for i in range(len(closes)):

        if i == 0:
            tr = highs[i] - lows[i]
        else:
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1])
            )

        trs.append(tr)

    if len(trs) < period:
        return result

    current = sum(trs[:period]) / period
    result[period - 1] = current

    for i in range(period, len(trs)):

        current = (
            current * (period - 1) + trs[i]
        ) / period

        result[i] = current

    return result


# ==========================================================
# CONSTRUCTION DES CARACTERISTIQUES
# ==========================================================

def build_features(candles):

    closes = [c["close"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]

    ema5 = ema(closes, 5)
    ema20 = ema(closes, 20)
    ema50 = ema(closes, 50)

    rsi14 = rsi(closes, 14)
    atr14 = atr(highs, lows, closes, 14)

    features = []

    for i, c in enumerate(candles):

        if i < 50:
            features.append(None)
            continue

        o = c["open"]
        h = c["high"]
        l = c["low"]
        cl = c["close"]

        candle_range = max(h - l, 0.000001)

        body = cl - o
        upper_wick = h - max(o, cl)
        lower_wick = min(o, cl) - l

        previous = candles[i - 1]

        change1 = cl - previous["close"]

        change3 = (
            cl - candles[i - 3]["close"]
        )

        change5 = (
            cl - candles[i - 5]["close"]
        )

        red_count = 0
        green_count = 0

        for j in range(i - 4, i + 1):

            if candles[j]["close"] < candles[j]["open"]:
                red_count += 1

            elif candles[j]["close"] > candles[j]["open"]:
                green_count += 1

        features.append({

            "rsi14": rsi14[i],

            "ema5_above_ema20": int(
                ema5[i] > ema20[i]
            ),

            "ema20_above_ema50": int(
                ema20[i] > ema50[i]
            ),

            "distance_ema20": (
                cl - ema20[i]
            ) / max(atr14[i], 0.000001),

            "distance_ema50": (
                cl - ema50[i]
            ) / max(atr14[i], 0.000001),

            "body_atr": (
                body / max(atr14[i], 0.000001)
            ),

            "upper_wick_ratio": (
                upper_wick / candle_range
            ),

            "lower_wick_ratio": (
                lower_wick / candle_range
            ),

            "range_atr": (
                candle_range / max(atr14[i], 0.000001)
            ),

            "change1_atr": (
                change1 / max(atr14[i], 0.000001)
            ),

            "change3_atr": (
                change3 / max(atr14[i], 0.000001)
            ),

            "change5_atr": (
                change5 / max(atr14[i], 0.000001)
            ),

            "red_count": red_count,
            "green_count": green_count,

            "atr": atr14[i]
        })

    return features


# ==========================================================
# EXCURSION FUTURE
# ==========================================================

def future_excursions(candles):

    values = [None] * len(candles)

    for i in range(len(candles) - FUTURE_CANDLES):

        future = candles[
            i + 1:i + 1 + FUTURE_CANDLES
        ]

        # Hausse maximale depuis la clôture du signal.
        highest = max(c["high"] for c in future)

        values[i] = max(
            0,
            highest - candles[i]["close"]
        )

    return values


def percentile(values, p):

    values = sorted(values)

    if not values:
        return None

    position = (len(values) - 1) * p / 100

    low = int(position)
    high = min(low + 1, len(values) - 1)

    weight = position - low

    return (
        values[low] * (1 - weight)
        + values[high] * weight
    )


# ==========================================================
# CREATION DES CONDITIONS
# ==========================================================

def make_conditions():

    conditions = []

    # RSI
    for level in [25, 30, 35, 40, 45]:
        conditions.append({
            "feature": "rsi14",
            "operator": "<=",
            "value": level
        })

    # EMA
    for feature in [
        "ema5_above_ema20",
        "ema20_above_ema50"
    ]:
        conditions.append({
            "feature": feature,
            "operator": "==",
            "value": 1
        })

    # Mèches
    for feature in [
        "upper_wick_ratio",
        "lower_wick_ratio"
    ]:
        for level in [0.2, 0.35, 0.5, 0.65]:
            conditions.append({
                "feature": feature,
                "operator": ">=",
                "value": level
            })

    # Pression des dernières bougies
    for feature in [
        "change1_atr",
        "change3_atr",
        "change5_atr",
        "body_atr",
        "distance_ema20",
        "distance_ema50"
    ]:
        for level in [-1.5, -0.75, 0, 0.5, 1.0]:
            conditions.append({
                "feature": feature,
                "operator": "<=",
                "value": level
            })

    # Séries de bougies rouges ou vertes
    for feature in ["red_count", "green_count"]:
        for level in [2, 3, 4, 5]:
            conditions.append({
                "feature": feature,
                "operator": ">=",
                "value": level
            })

    return conditions


def condition_matches(row, condition):

    value = row.get(condition["feature"])

    if value is None:
        return False

    operator = condition["operator"]
    target = condition["value"]

    if operator == "<=":
        return value <= target

    if operator == ">=":
        return value >= target

    if operator == "==":
        return value == target

    return False


# ==========================================================
# EVALUATION
# ==========================================================

def evaluate(
    features,
    excursions,
    start,
    end,
    threshold,
    condition
):

    signals = 0
    successes = 0

    for i in range(start, end):

        row = features[i]
        future = excursions[i]

        if row is None or future is None:
            continue

        if not condition_matches(row, condition):
            continue

        signals += 1

        if future >= threshold:
            successes += 1

    precision = (
        successes / signals * 100
        if signals else 0
    )

    return {
        "signals": signals,
        "successes": successes,
        "precision": round(precision, 2)
    }


# ==========================================================
# COMPARAISON AVANT LES GRANDES HAUSSES
# ==========================================================

def compare_precursors(features, excursions, start, end, threshold):

    spike_rows = []
    normal_rows = []

    for i in range(start, end):

        row = features[i]
        future = excursions[i]

        if row is None or future is None:
            continue

        if future >= threshold:
            spike_rows.append(row)
        else:
            normal_rows.append(row)

    numeric_features = [
        "rsi14",
        "distance_ema20",
        "distance_ema50",
        "body_atr",
        "upper_wick_ratio",
        "lower_wick_ratio",
        "range_atr",
        "change1_atr",
        "change3_atr",
        "change5_atr",
        "red_count",
        "green_count"
    ]

    comparison = {}

    for feature in numeric_features:

        spike_values = [
            r[feature] for r in spike_rows
            if r[feature] is not None
        ]

        normal_values = [
            r[feature] for r in normal_rows
            if r[feature] is not None
        ]

        if not spike_values or not normal_values:
            continue

        comparison[feature] = {
            "before_spikes_average": round(
                sum(spike_values) / len(spike_values), 5
            ),
            "normal_average": round(
                sum(normal_values) / len(normal_values), 5
            ),
            "spike_samples": len(spike_values),
            "normal_samples": len(normal_values)
        }

    return comparison


# ==========================================================
# ANALYSE PRINCIPALE
# ==========================================================

def analyze(candles):

    n = len(candles)

    train_end = int(n * TRAIN_RATIO)
    validation_end = int(
        n * (TRAIN_RATIO + VALIDATION_RATIO)
    )

    features = build_features(candles)
    excursions = future_excursions(candles)

    train_values = [
        excursions[i]
        for i in range(50, train_end)
        if excursions[i] is not None
        and excursions[i] > 0
    ]

    if len(train_values) < 10:

        return {
            "status": "insufficient_training_data",
            "total_candles": n,
            "positive_excursions": len(train_values)
        }, [], {}

    threshold = percentile(train_values, 95)

    conditions = make_conditions()

    results = []

    for condition in conditions:

        train = evaluate(
            features,
            excursions,
            50,
            train_end - FUTURE_CANDLES,
            threshold,
            condition
        )

        if train["signals"] < MIN_TRAIN_EVENTS:
            continue

        validation = evaluate(
            features,
            excursions,
            train_end,
            validation_end - FUTURE_CANDLES,
            threshold,
            condition
        )

        if validation["signals"] < MIN_VALIDATION_EVENTS:
            continue

        results.append({
            "condition": condition,
            "train": train,
            "validation": validation
        })

    results.sort(
        key=lambda x: (
            x["validation"]["precision"],
            x["validation"]["successes"],
            x["train"]["precision"]
        ),
        reverse=True
    )

    final_patterns = []

    for item in results[:15]:

        test = evaluate(
            features,
            excursions,
            validation_end,
            n - FUTURE_CANDLES,
            threshold,
            item["condition"]
        )

        final_patterns.append({
            "condition": item["condition"],
            "train": item["train"],
            "validation": item["validation"],
            "test": test
        })

    spikes = []

    for i, c in enumerate(candles):

        value = excursions[i]

        if value is None or value < threshold:
            continue

        spikes.append({
            "epoch": c["epoch"],
            "open": c["open"],
            "high": c["high"],
            "low": c["low"],
            "close": c["close"],
            "future_upward_excursion": round(value, 5)
        })

    comparison = compare_precursors(
        features,
        excursions,
        50,
        train_end - FUTURE_CANDLES,
        threshold
    )

    summary = {

        "version": 9,
        "status": "analysis_completed",
        "updated_at": now(),

        "symbol": SYMBOL,
        "timeframe_seconds": GRANULARITY,

        "total_candles": n,
        "train_candles": train_end,
        "validation_candles": validation_end - train_end,
        "test_candles": n - validation_end,

        "spike_threshold": round(threshold, 5),

        "threshold_method": (
            "95th_percentile_of_positive_future_excursions_train"
        ),

        "future_candles": FUTURE_CANDLES,

        "conditions_tested": len(conditions),
        "conditions_qualified": len(results),

        "training_spikes": sum(
            1 for x in excursions[:train_end]
            if x is not None and x >= threshold
        ),

        "validation_spikes": sum(
            1 for x in excursions[train_end:validation_end]
            if x is not None and x >= threshold
        ),

        "test_spikes": sum(
            1 for x in excursions[validation_end:]
            if x is not None and x >= threshold
        )
    }

    return summary, spikes, {
        "patterns": final_patterns,
        "precursor_comparison": comparison
    }


# ==========================================================
# MAIN
# ==========================================================

def main():

    print("====================================")
    print("SPIKE1000 V9")
    print("BOOM1000 - 1 MINUTE")
    print("PRECURSOR DETECTOR")
    print("====================================")

    previous = load_previous()

    output = {
        "version": 9,
        "symbol": SYMBOL,
        "timeframe": "1 minute",
        "updated_at": now(),
        "candles": [],
        "analysis": {},
        "spikes": [],
        "patterns": [],
        "precursor_comparison": {},
        "errors": []
    }

    try:

        print("Connexion Deriv...")

        candles = get_candles()

        print("Bougies :", len(candles))

        print("Analyse des précurseurs...")

        summary, spikes, details = analyze(candles)

        output["candles"] = candles
        output["analysis"] = summary
        output["spikes"] = spikes

        output["patterns"] = details.get(
            "patterns", []
        )

        output["precursor_comparison"] = details.get(
            "precursor_comparison", {}
        )

        print("Statut :", summary.get("status"))
        print("Seuil :", summary.get("spike_threshold"))
        print("Grandes hausses :", len(spikes))
        print(
            "Conditions retenues :",
            summary.get("conditions_qualified", 0)
        )

    except Exception as e:

        output["errors"].append(str(e))

        output["candles"] = previous.get("candles", [])
        output["analysis"] = previous.get("analysis", {})
        output["spikes"] = previous.get("spikes", [])
        output["patterns"] = previous.get("patterns", [])
        output["precursor_comparison"] = previous.get(
            "precursor_comparison", {}
        )

        print("ERREUR :", str(e))

    save_json(output)

    print("db.json sauvegardé.")


if __name__ == "__main__":
    main()