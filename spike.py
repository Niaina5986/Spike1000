import json
import os
import time
import itertools
from datetime import datetime, timezone

import websocket


# ==========================================================
# SPIKE1000 V8 - ANALYSE BOOM1000 PAR BOUGIES 1 MINUTE
# Analyse uniquement : aucun trade, aucun ordre.
# ==========================================================

SYMBOL = "BOOM1000"
GRANULARITY = 60
CANDLE_COUNT = 1000
FUTURE_CANDLES = 5

WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"

DB_FILE = "db.json"

RSI_PERIODS = [6, 9, 14]
RSI_LEVELS = [25, 30, 35]

EMA_PAIRS = [
    (5, 20),
    (8, 30),
    (12, 50)
]

STOCH_PERIODS = [5, 9, 14]
STOCH_LEVELS = [20, 30]

MIN_SIGNALS = 3


# ==========================================================
# UTILITAIRES
# ==========================================================

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
# CONNEXION DERIV
# ==========================================================

def get_candles():

    ws = None

    try:
        ws = websocket.create_connection(
            WS_URL,
            timeout=25,
            enable_multithread=True
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

            raw = ws.recv()
            response = json.loads(raw)

            if response.get("error"):
                raise RuntimeError(
                    response["error"].get("message", "Erreur Deriv")
                )

            if response.get("req_id") != 1001:
                continue

            candles = response.get("candles")

            if not candles:
                raise RuntimeError(
                    "Deriv n'a retourné aucune bougie."
                )

            result = []

            for c in candles:

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

                result.append(candle)

            # Ordre chronologique et suppression des doublons exacts.
            result.sort(key=lambda x: x["epoch"])

            unique = {}
            for candle in result:
                unique[candle["epoch"]] = candle

            result = list(unique.values())

            if len(result) < 100:
                raise RuntimeError(
                    f"Historique insuffisant : {len(result)} bougies."
                )

            return result

        raise TimeoutError("Délai dépassé pendant la récupération.")

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

    multiplier = 2 / (period + 1)

    current = sum(values[:period]) / period

    result[period - 1] = current

    for i in range(period, len(values)):

        current = (
            values[i] * multiplier
            + current * (1 - multiplier)
        )

        result[i] = current

    return result


def rsi(values, period):

    result = [None] * len(values)

    if len(values) <= period:
        return result

    gains = []
    losses = []

    for i in range(1, len(values)):

        change = values[i] - values[i - 1]

        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period

    if avg_loss == 0:
        result[period] = 100
    else:
        result[period] = (
            100 - 100 / (1 + avg_gain / avg_loss)
        )

    for i in range(period + 1, len(values)):

        avg_gain = (
            avg_gain * (period - 1) + gains[i - 1]
        ) / period

        avg_loss = (
            avg_loss * (period - 1) + losses[i - 1]
        ) / period

        if avg_loss == 0:
            result[i] = 100
        else:
            result[i] = (
                100 - 100 / (1 + avg_gain / avg_loss)
            )

    return result


def stochastic(highs, lows, closes, period):

    result = [None] * len(closes)

    for i in range(period - 1, len(closes)):

        highest = max(highs[i - period + 1:i + 1])
        lowest = min(lows[i - period + 1:i + 1])

        if highest == lowest:
            result[i] = 50
        else:
            result[i] = (
                (closes[i] - lowest)
                / (highest - lowest)
            ) * 100

    return result


def atr(highs, lows, closes, period=14):

    result = [None] * len(closes)

    if len(closes) <= period:
        return result

    true_ranges = []

    for i in range(len(closes)):

        if i == 0:
            tr = highs[i] - lows[i]
        else:
            tr = max(
                highs[i] - lows[i],
                abs(highs[i] - closes[i - 1]),
                abs(lows[i] - closes[i - 1])
            )

        true_ranges.append(tr)

    current = sum(true_ranges[:period]) / period
    result[period - 1] = current

    for i in range(period, len(closes)):

        current = (
            current * (period - 1) + true_ranges[i]
        ) / period

        result[i] = current

    return result


# ==========================================================
# PRÉPARATION DES DONNÉES
# ==========================================================

def prepare_data(candles):

    opens = [c["open"] for c in candles]
    highs = [c["high"] for c in candles]
    lows = [c["low"] for c in candles]
    closes = [c["close"] for c in candles]

    data = {
        "rsi": {},
        "ema": {},
        "stoch": {},
        "atr": atr(highs, lows, closes)
    }

    for p in RSI_PERIODS:
        data["rsi"][p] = rsi(closes, p)

    for fast, slow in EMA_PAIRS:
        data["ema"][(fast, slow)] = (
            ema(closes, fast),
            ema(closes, slow)
        )

    for p in STOCH_PERIODS:
        data["stoch"][p] = stochastic(
            highs, lows, closes, p
        )

    return data


# ==========================================================
# DÉTECTION DES GRANDES BOUGIES HAUSSIÈRES
# ==========================================================

def upward_excursions(candles):

    # Mesure de l'amplitude entre l'ouverture et le plus haut.
    # Ce n'est pas une reconstitution exacte du mouvement intrabougie.

    return [
        max(0, c["high"] - c["open"])
        for c in candles
    ]


def percentile(values, percentage):

    if not values:
        return None

    values = sorted(values)

    position = (len(values) - 1) * percentage / 100

    lower = int(position)
    upper = min(lower + 1, len(values) - 1)

    weight = position - lower

    return (
        values[lower] * (1 - weight)
        + values[upper] * weight
    )


# ==========================================================
# CONSTRUCTION DES CONFIGURATIONS
# ==========================================================

def make_configurations():

    configurations = []

    for values in itertools.product(
        RSI_PERIODS,
        RSI_LEVELS,
        EMA_PAIRS,
        STOCH_PERIODS,
        STOCH_LEVELS
    ):

        rsi_period, rsi_level, ema_pair, stoch_period, stoch_level = values

        configurations.append({
            "rsi_period": rsi_period,
            "rsi_level": rsi_level,
            "ema_fast": ema_pair[0],
            "ema_slow": ema_pair[1],
            "stoch_period": stoch_period,
            "stoch_level": stoch_level
        })

    return configurations


# ==========================================================
# SIGNAL
# ==========================================================

def generate_signal(i, config, indicators):

    rp = config["rsi_period"]
    rsi_level = config["rsi_level"]

    sp = config["stoch_period"]
    stoch_level = config["stoch_level"]

    fast = config["ema_fast"]
    slow = config["ema_slow"]

    rsi_values = indicators["rsi"][rp]
    stoch_values = indicators["stoch"][sp]

    ema_fast = indicators["ema"][(fast, slow)][0]
    ema_slow = indicators["ema"][(fast, slow)][1]

    if i < 1:
        return False

    required = [
        rsi_values[i],
        rsi_values[i - 1],
        stoch_values[i],
        stoch_values[i - 1],
        ema_fast[i],
        ema_slow[i]
    ]

    if any(x is None for x in required):
        return False

    # RSI remonte après une zone basse.
    rsi_condition = (
        rsi_values[i - 1] < rsi_level
        and rsi_values[i] > rsi_values[i - 1]
    )

    # Stochastic croise vers le haut.
    stoch_condition = (
        stoch_values[i - 1] < stoch_level
        and stoch_values[i] > stoch_values[i - 1]
    )

    # Confirmation de tendance.
    ema_condition = ema_fast[i] >= ema_slow[i]

    return (
        rsi_condition
        and stoch_condition
        and ema_condition
    )


# ==========================================================
# ÉVALUATION
# ==========================================================

def evaluate(candles, indicators, config,
             start, end, threshold):

    signals = 0
    successes = 0

    for i in range(start, end - FUTURE_CANDLES):

        if not generate_signal(i, config, indicators):
            continue

        signals += 1

        future = candles[i + 1:i + 1 + FUTURE_CANDLES]

        found = any(
            c["high"] - c["open"] >= threshold
            for c in future
        )

        if found:
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


def find_spikes(candles, threshold):

    spikes = []

    for i, c in enumerate(candles):

        size = c["high"] - c["open"]

        if size >= threshold:

            spikes.append({
                "epoch": c["epoch"],
                "time": datetime.fromtimestamp(
                    c["epoch"], timezone.utc
                ).isoformat(),
                "open": c["open"],
                "high": c["high"],
                "low": c["low"],
                "close": c["close"],
                "upward_excursion": round(size, 5)
            })

    return spikes


# ==========================================================
# ANALYSE PRINCIPALE
# ==========================================================

def analyze(candles):

    indicators = prepare_data(candles)

    n = len(candles)

    train_end = int(n * 0.60)
    validation_end = int(n * 0.80)

    excursions = upward_excursions(candles)

    positive_train = [
        x for x in excursions[:train_end] if x > 0
    ]

    if len(positive_train) < 10:

        return {
            "status": "insufficient_positive_excursions",
            "positive_train": len(positive_train),
            "total_candles": n
        }, [], {}

    threshold = percentile(positive_train, 95)

    if threshold is None or threshold <= 0:

        return {
            "status": "invalid_spike_threshold",
            "total_candles": n
        }, [], {}

    spikes = find_spikes(candles, threshold)

    configurations = make_configurations()

    results = []

    for config in configurations:

        train = evaluate(
            candles,
            indicators,
            config,
            0,
            train_end,
            threshold
        )

        if train["signals"] < MIN_SIGNALS:
            continue

        validation = evaluate(
            candles,
            indicators,
            config,
            train_end,
            validation_end,
            threshold
        )

        if validation["signals"] < 2:
            continue

        results.append({
            "config": config,
            "train": train,
            "validation": validation
        })

    # Priorité à la précision de validation,
    # puis au nombre de signaux observés.

    results.sort(
        key=lambda x: (
            x["validation"]["precision"],
            x["validation"]["signals"],
            x["train"]["precision"]
        ),
        reverse=True
    )

    top = results[:10]

    final_results = []

    for item in top:

        test = evaluate(
            candles,
            indicators,
            item["config"],
            validation_end,
            n,
            threshold
        )

        final_results.append({
            "config": item["config"],
            "train": item["train"],
            "validation": item["validation"],
            "test": test
        })

    positive_changes = sum(
        1 for i in range(1, n)
        if candles[i]["close"] > candles[i - 1]["close"]
    )

    negative_changes = sum(
        1 for i in range(1, n)
        if candles[i]["close"] < candles[i - 1]["close"]
    )

    zero_changes = (
        n - 1 - positive_changes - negative_changes
    )

    summary = {
        "version": 8,
        "status": "analysis_completed",
        "updated_at": now(),
        "symbol": SYMBOL,
        "timeframe_seconds": GRANULARITY,
        "total_candles": n,
        "train_candles": train_end,
        "validation_candles": validation_end - train_end,
        "test_candles": n - validation_end,
        "positive_close_changes": positive_changes,
        "negative_close_changes": negative_changes,
        "zero_close_changes": zero_changes,
        "largest_upward_excursion": round(
            max(excursions), 5
        ),
        "spike_threshold": round(threshold, 5),
        "threshold_method": "95th_percentile_train",
        "detected_spikes_total": len(spikes),
        "configurations_tested": len(configurations),
        "configurations_qualified": len(results),
        "future_candles": FUTURE_CANDLES
    }

    return summary, spikes, final_results


# ==========================================================
# PROGRAMME PRINCIPAL
# ==========================================================

def main():

    print("====================================")
    print("SPIKE1000 V8 - BOOM1000")
    print("Analyse des bougies 1 minute")
    print("====================================")

    previous = load_previous()

    output = {
        "symbol": SYMBOL,
        "timeframe": "1 minute",
        "updated_at": now(),
        "candles": [],
        "analysis": {},
        "spikes": [],
        "patterns": [],
        "errors": []
    }

    try:

        print("Connexion à Deriv...")

        candles = get_candles()

        print(f"Bougies récupérées : {len(candles)}")

        print("Calcul des indicateurs...")

        summary, spikes, patterns = analyze(candles)

        output["candles"] = candles
        output["analysis"] = summary
        output["spikes"] = spikes
        output["patterns"] = patterns

        print("Analyse terminée.")
        print("Statut :", summary.get("status"))
        print("Grandes bougies détectées :", len(spikes))
        print("Configurations retenues :", len(patterns))

    except Exception as e:

        output["errors"].append(str(e))

        # Garder les résultats précédents en cas d'échec.
        output["candles"] = previous.get("candles", [])
        output["analysis"] = previous.get("analysis", {})
        output["spikes"] = previous.get("spikes", [])
        output["patterns"] = previous.get("patterns", [])

        print("ERREUR :", str(e))

    save_json(output)

    print("db.json sauvegardé.")


if __name__ == "__main__":
    main()