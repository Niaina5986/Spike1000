import os
import json
import time
import itertools
import websocket

from pathlib import Path
from datetime import datetime, timezone


# ============================================================
# SPIKE1000 V6 - MULTI INDICATOR ENGINE
# ============================================================

VERSION = 6

SYMBOL = os.getenv("DERIV_SYMBOL", "BOOM1000")

HISTORY_COUNT = int(os.getenv("HISTORY_COUNT", "5000"))
COLLECT_SECONDS = int(os.getenv("COLLECT_SECONDS", "180"))
FUTURE_TICKS = int(os.getenv("FUTURE_TICKS", "20"))

MAX_TICKS = 100000

WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"

DB_FILE = Path(__file__).resolve().parent / "db.json"

MIN_TRAIN_SIGNALS = 15
MIN_TEST_SIGNALS = 20

TARGET_PRECISION = 0.80

MAX_COMBINATIONS = 15000


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
        return empty_db()

    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            db = json.load(f)

        if not isinstance(db, dict):
            return empty_db()

        defaults = empty_db()

        for key, value in defaults.items():
            if key not in db:
                db[key] = value

        for key in ("ticks", "spikes", "errors"):
            if not isinstance(db[key], list):
                db[key] = []

        if not isinstance(db["analysis"], dict):
            db["analysis"] = {}

        if not isinstance(db["prediction"], dict):
            db["prediction"] = {}

        db["version"] = VERSION
        db["symbol"] = SYMBOL

        return db

    except Exception as error:
        print("Erreur DB:", error)
        return empty_db()


def save_db(db):
    db["updated_at"] = now_iso()

    temp = DB_FILE.with_suffix(".tmp")

    with open(temp, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())

    os.replace(temp, DB_FILE)

    print("DB sauvegardée.")
    print("Nombre de ticks:", len(db["ticks"]))


def record_error(db, error):
    db["errors"].append({
        "time": now_iso(),
        "message": str(error)
    })

    db["errors"] = db["errors"][-50:]


# ============================================================
# DERIV CONNECTION
# ============================================================

def connect_deriv():
    delays = [5, 10, 20, 30, 60]

    for attempt, delay in enumerate(delays, 1):
        try:
            print("Connexion Deriv:", attempt)

            ws = websocket.create_connection(
                WS_URL,
                timeout=30
            )

            print("Connexion réussie.")
            return ws

        except Exception as error:
            print("Connexion impossible:", error)

            if attempt < len(delays):
                time.sleep(delay)

    raise RuntimeError("Connexion Deriv impossible")


# ============================================================
# HISTORICAL DATA
# ============================================================

def collect_history():
    print("\nCollecte historique...")

    ws = connect_deriv()

    try:
        ws.send(json.dumps({
            "ticks_history": SYMBOL,
            "count": HISTORY_COUNT,
            "end": "latest",
            "style": "ticks",
            "req_id": 1001
        }))

        while True:
            data = json.loads(ws.recv())

            if "error" in data:
                raise RuntimeError(data["error"])

            history = data.get("history")

            if history:
                rows = []

                for epoch, price in zip(
                    history["times"],
                    history["prices"]
                ):
                    rows.append({
                        "epoch": int(epoch),
                        "price": float(price)
                    })

                print("Historique:", len(rows))
                return rows

    finally:
        ws.close()


# ============================================================
# LIVE DATA
# ============================================================

def collect_live():
    print("\nCollecte live:", COLLECT_SECONDS, "secondes")

    ws = connect_deriv()

    rows = []
    start = time.time()

    try:
        ws.send(json.dumps({
            "ticks": SYMBOL,
            "subscribe": 1,
            "req_id": 2001
        }))

        ws.settimeout(10)

        while time.time() - start < COLLECT_SECONDS:
            try:
                data = json.loads(ws.recv())

                if "error" in data:
                    raise RuntimeError(data["error"])

                tick = data.get("tick")

                if tick:
                    rows.append({
                        "epoch": int(tick["epoch"]),
                        "price": float(tick["quote"])
                    })

            except websocket.WebSocketTimeoutException:
                continue

    except Exception as error:
        print("Erreur live:", error)

    finally:
        ws.close()

    print("Ticks live:", len(rows))

    return rows


# ============================================================
# MERGE TICKS
# ============================================================

def merge_ticks(db, new_rows):
    combined = {}

    for row in db["ticks"] + new_rows:
        try:
            epoch = int(row["epoch"])
            price = float(row["price"])

            combined[(epoch, price)] = {
                "epoch": epoch,
                "price": price
            }

        except (KeyError, ValueError, TypeError):
            continue

    db["ticks"] = sorted(
        combined.values(),
        key=lambda x: x["epoch"]
    )[-MAX_TICKS:]

    return db["ticks"]


# ============================================================
# EMA
# ============================================================

def calculate_ema(prices, period):
    result = [None] * len(prices)

    if len(prices) < period:
        return result

    multiplier = 2 / (period + 1)

    ema = sum(prices[:period]) / period

    result[period - 1] = ema

    for i in range(period, len(prices)):
        ema = (
            prices[i] * multiplier
            + ema * (1 - multiplier)
        )

        result[i] = ema

    return result


# ============================================================
# RSI - WILDER
# ============================================================

def calculate_rsi(prices, period):
    result = [None] * len(prices)

    if len(prices) <= period:
        return result

    gains = []
    losses = []

    for i in range(1, period + 1):
        change = prices[i] - prices[i - 1]

        gains.append(max(change, 0))
        losses.append(max(-change, 0))

    avg_gain = sum(gains) / period
    avg_loss = sum(losses) / period

    def rsi_value(gain, loss):
        if loss == 0:
            return 100.0 if gain > 0 else 50.0

        rs = gain / loss

        return 100 - (100 / (1 + rs))

    result[period] = rsi_value(
        avg_gain,
        avg_loss
    )

    for i in range(period + 1, len(prices)):
        change = prices[i] - prices[i - 1]

        gain = max(change, 0)
        loss = max(-change, 0)

        avg_gain = (
            avg_gain * (period - 1) + gain
        ) / period

        avg_loss = (
            avg_loss * (period - 1) + loss
        ) / period

        result[i] = rsi_value(
            avg_gain,
            avg_loss
        )

    return result


# ============================================================
# STOCHASTIC
# ============================================================

def calculate_stochastic(prices, k_period, d_period):
    k_values = [None] * len(prices)
    d_values = [None] * len(prices)

    for i in range(k_period - 1, len(prices)):
        window = prices[i - k_period + 1:i + 1]

        lowest = min(window)
        highest = max(window)

        if highest == lowest:
            k_values[i] = 50.0
        else:
            k_values[i] = (
                (prices[i] - lowest)
                / (highest - lowest)
            ) * 100

    for i in range(len(prices)):
        if i < k_period + d_period - 2:
            continue

        window = k_values[i - d_period + 1:i + 1]

        if all(value is not None for value in window):
            d_values[i] = sum(window) / d_period

    return k_values, d_values


# ============================================================
# PERCENTILE
# ============================================================

def percentile(values, percentage):
    values = sorted(values)

    if not values:
        return 0

    position = (len(values) - 1) * percentage / 100

    low = int(position)
    high = min(low + 1, len(values) - 1)

    fraction = position - low

    return (
        values[low] * (1 - fraction)
        + values[high] * fraction
    )


# ============================================================
# SPIKE DETECTION
# ============================================================

def calculate_returns(prices):
    result = [0.0]

    for i in range(1, len(prices)):
        previous = prices[i - 1]

        if previous <= 0:
            result.append(0.0)
        else:
            result.append(
                (prices[i] - previous) / previous
            )

    return result


# ============================================================
# INDICATOR CACHE
# ============================================================

def build_indicators(prices):
    print("\nCalcul des indicateurs...")

    cache = {
        "rsi": {},
        "ema": {},
        "stochastic": {}
    }

    rsi_periods = [5, 6, 7, 9, 14, 21]

    ema_periods = [
        3, 5, 8, 10, 12, 15, 20,
        25, 30, 40, 50, 60, 80, 100
    ]

    stochastic_periods = [5, 7, 9, 14]

    for period in rsi_periods:
        cache["rsi"][period] = calculate_rsi(
            prices,
            period
        )

    for period in ema_periods:
        cache["ema"][period] = calculate_ema(
            prices,
            period
        )

    for period in stochastic_periods:
        for d_period in [3, 5]:
            cache["stochastic"][(period, d_period)] = (
                calculate_stochastic(
                    prices,
                    period,
                    d_period
                )
            )

    return cache


# ============================================================
# COMBINATION GENERATOR
# ============================================================

def generate_combinations():
    combinations = []

    rsi_periods = [5, 6, 7, 9, 14, 21]
    rsi_levels = [20, 25, 30, 35, 40, 45]

    ema_fast = [3, 5, 8, 10, 12, 15, 20]
    ema_slow = [20, 25, 30, 40, 50, 60, 80, 100]

    stochastic_periods = [5, 7, 9, 14]
    stochastic_d = [3, 5]

    stochastic_levels = [10, 15, 20, 25, 30]

    for values in itertools.product(
        rsi_periods,
        rsi_levels,
        ema_fast,
        ema_slow,
        stochastic_periods,
        stochastic_d,
        stochastic_levels
    ):
        (
            rp,
            rl,
            ef,
            es,
            sk,
            sd,
            sl
        ) = values

        if ef >= es:
            continue

        combinations.append({
            "rsi_period": rp,
            "rsi_level": rl,
            "ema_fast": ef,
            "ema_slow": es,
            "stochastic_k": sk,
            "stochastic_d": sd,
            "stochastic_level": sl
        })

        if len(combinations) >= MAX_COMBINATIONS:
            break

    return combinations


# ============================================================
# SIGNAL ENGINE
# ============================================================

def check_signal(config, indicators, index):
    rp = config["rsi_period"]
    rl = config["rsi_level"]

    ef = config["ema_fast"]
    es = config["ema_slow"]

    sk = config["stochastic_k"]
    sd = config["stochastic_d"]
    sl = config["stochastic_level"]

    rsi = indicators["rsi"][rp]

    fast = indicators["ema"][ef]
    slow = indicators["ema"][es]

    k, d = indicators["stochastic"][(sk, sd)]

    if index < 1:
        return False

    if any(x[index] is None for x in [
        rsi, fast, slow, k, d
    ]):
        return False

    if any(x[index - 1] is None for x in [
        rsi, k, d
    ]):
        return False

    # RSI remonte depuis sa zone basse.

    rsi_condition = (
        rsi[index] >= rsi[index - 1]
        and rsi[index - 1] <= rl
    )

    # Stochastic croise à la hausse en zone basse.

    stochastic_condition = (
        k[index] > d[index]
        and k[index - 1] <= d[index - 1]
        and k[index - 1] <= sl
    )

    # Tendance EMA haussière.

    ema_condition = (
        fast[index] > slow[index]
    )

    return (
        rsi_condition
        and stochastic_condition
        and ema_condition
    )


# ============================================================
# BACKTEST
# ============================================================

def evaluate_combination(
    config,
    indicators,
    spike_flags,
    start,
    end
):
    signals = 0
    hits = 0

    for index in range(start, end - FUTURE_TICKS):
        if not check_signal(
            config,
            indicators,
            index
        ):
            continue

        signals += 1

        future = spike_flags[
            index + 1:index + FUTURE_TICKS + 1
        ]

        if any(future):
            hits += 1

    probability = (
        hits / signals
        if signals else 0
    )

    return {
        "signals": signals,
        "hits": hits,
        "probability": probability
    }


# ============================================================
# ANALYSIS ENGINE
# ============================================================

def analyze_data(db):
    ticks = db["ticks"]

    print("\n========== ANALYSE V6 ==========")

    if len(ticks) < 1000:
        db["analysis"] = {
            "status": "insufficient_data",
            "total_ticks": len(ticks),
            "minimum_required": 1000
        }

        db["prediction"] = {
            "status": "waiting_for_data"
        }

        print("Minimum 1000 ticks requis.")
        return

    prices = [float(x["price"]) for x in ticks]

    returns = calculate_returns(prices)

    # Split chronologique :
    # 60 % apprentissage
    # 20 % validation
    # 20 % test indépendant

    n = len(prices)

    train_end = int(n * 0.60)
    validation_end = int(n * 0.80)

    positive_returns = [
        returns[i]
        for i in range(1, train_end)
        if returns[i] > 0
    ]

    if len(positive_returns) < 20:
        db["analysis"] = {
            "status": "insufficient_positive_returns",
            "total_ticks": n
        }
        return

    spike_threshold = percentile(
        positive_returns,
        99.5
    )

    spike_flags = [
        value >= spike_threshold
        for value in returns
    ]

    db["spikes"] = [
        {
            "epoch": ticks[i]["epoch"],
            "price": prices[i],
            "variation": returns[i]
        }
        for i in range(1, len(returns))
        if spike_flags[i]
    ]

    indicators = build_indicators(prices)

    combinations = generate_combinations()

    print("Ticks:", n)
    print("Combinaisons:", len(combinations))
    print("Seuil spike:", spike_threshold)

    # Recherche sur entraînement.

    training_results = []

    for config in combinations:
        result = evaluate_combination(
            config,
            indicators,
            spike_flags,
            100,
            train_end
        )

        if result["signals"] < MIN_TRAIN_SIGNALS:
            continue

        training_results.append({
            "config": config,
            **result
        })

    # Validation : choisir les meilleures configurations.

    training_results.sort(
        key=lambda x: (
            x["probability"],
            x["signals"]
        ),
        reverse=True
    )

    candidates = training_results[:100]

    validation_results = []

    for item in candidates:
        result = evaluate_combination(
            item["config"],
            indicators,
            spike_flags,
            train_end,
            validation_end
        )

        if result["signals"] < 5:
            continue

        validation_results.append({
            "config": item["config"],
            **result
        })

    validation_results.sort(
        key=lambda x: (
            x["probability"],
            x["signals"]
        ),
        reverse=True
    )

    # Test final : seules les configurations choisies
    # sur les données antérieures sont évaluées ici.

    final_results = []

    for item in validation_results[:20]:
        result = evaluate_combination(
            item["config"],
            indicators,
            spike_flags,
            validation_end,
            n - FUTURE_TICKS
        )

        if result["signals"] < MIN_TEST_SIGNALS:
            continue

        final_results.append({
            "config": item["config"],
            **result
        })

    final_results.sort(
        key=lambda x: (
            x["probability"],
            x["signals"]
        ),
        reverse=True
    )

    best = []

    for item in final_results[:10]:
        c = item["config"]

        best.append({
            "rsi": f"RSI {c['rsi_period']}",
            "rsi_level": c["rsi_level"],
            "ema": (
                f"EMA {c['ema_fast']}/"
                f"{c['ema_slow']}"
            ),
            "stochastic": (
                f"{c['stochastic_k']}/"
                f"{c['stochastic_d']}"
            ),
            "stochastic_level": c["stochastic_level"],
            "signals_tested": item["signals"],
            "spikes_detected": item["hits"],
            "spike_probability": round(
                item["probability"],
                4
            )
        })

    db["analysis"] = {
        "status": "completed",
        "version": VERSION,
        "total_ticks": n,
        "combinations_tested": len(combinations),
        "combinations_qualified": len(training_results),
        "spike_threshold": spike_threshold,
        "total_spikes": len(db["spikes"]),
        "training_ticks": train_end,
        "validation_ticks": validation_end - train_end,
        "test_ticks": n - validation_end,
        "best_configurations": best,
        "target_probability": TARGET_PRECISION,
        "note": (
            "Probabilités historiques expérimentales. "
            "Aucune garantie de spike futur."
        )
    }

    if best:
        best_config = best[0]

        db["prediction"] = {
            "updated_at": now_iso(),
            "signal": (
                "CONFIGURATION HISTORIQUEMENT FAVORABLE"
                if best_config["spike_probability"] >= TARGET_PRECISION
                else "AUCUNE CONFIGURATION A 80%"
            ),
            "best_configuration": best_config
        }

    else:
        db["prediction"] = {
            "updated_at": now_iso(),
            "signal": "DONNEES INSUFFISANTES POUR VALIDER",
            "best_configuration": None
        }

    print("\n========== RESULTATS ==========")
    print("Combinaisons testées:", len(combinations))
    print("Combinaisons qualifiées:", len(training_results))
    print("Spikes:", len(db["spikes"]))

    if best:
        for i, item in enumerate(best[:5], 1):
            print(
                i,
                item["rsi"],
                "EMA:", item["ema"],
                "STO:", item["stochastic"],
                "Probabilité:",
                item["spike_probability"] * 100,
                "%",
                "Signaux:",
                item["signals_tested"]
            )
    else:
        print("Aucune configuration validée.")

    print("================================")


# ============================================================
# MAIN
# ============================================================

def main():
    print("\n===================================")
    print("       SPIKE1000 ENGINE V6")
    print("       MULTI INDICATOR")
    print("===================================")

    db = load_db()

    print("Ticks existants:", len(db["ticks"]))

    try:
        history = collect_history()
        merge_ticks(db, history)
        save_db(db)

    except Exception as error:
        print("Erreur historique:", error)
        record_error(db, error)

    try:
        live = collect_live()
        merge_ticks(db, live)
        save_db(db)

    except Exception as error:
        print("Erreur live:", error)
        record_error(db, error)

    try:
        analyze_data(db)

    except Exception as error:
        print("Erreur analyse:", error)
        record_error(db, error)

        db["analysis"] = {
            "status": "error",
            "message": str(error)
        }

        db["prediction"] = {
            "signal": "ANALYSE IMPOSSIBLE"
        }

    save_db(db)

    print("\nSPIKE1000 V6 terminé.")
    print("Lecture seule.")
    print("Aucun trade exécuté.")


if __name__ == "__main__":
    main()