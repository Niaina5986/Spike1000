#!/usr/bin/env python3
# SPIKE1000 V7
# Analyse BOOM1000 en lecture seule.
# Aucun trade, achat ou vente.

import json
import math
import os
import time
from datetime import datetime, timezone

import websocket

VERSION = 7
SYMBOL = "BOOM1000"
WS_URL = "wss://api.derivws.com/trading/v1/options/ws/public"

DB_FILE = "db.json"
HISTORY_COUNT = 5000
COLLECT_SECONDS = 180
FUTURE_TICKS = 20
MAX_STORED_TICKS = 30000


def now():
    return datetime.now(timezone.utc).isoformat()


def empty_db():
    return {
        "ticks": [],
        "spikes": [],
        "analysis": {},
        "prediction": {},
        "errors": []
    }


def load_db():
    if not os.path.exists(DB_FILE):
        return empty_db()

    try:
        with open(DB_FILE, "r", encoding="utf-8") as f:
            db = json.load(f)

        if not isinstance(db, dict):
            return empty_db()

        db.setdefault("ticks", [])
        db.setdefault("spikes", [])
        db.setdefault("analysis", {})
        db.setdefault("prediction", {})
        db.setdefault("errors", [])

        return db

    except Exception as e:
        print("Erreur lecture DB:", e)
        return empty_db()


def save_db(db):
    temp = DB_FILE + ".tmp"

    with open(temp, "w", encoding="utf-8") as f:
        json.dump(db, f, indent=2, ensure_ascii=False)

    os.replace(temp, DB_FILE)


def clean_ticks(rows):
    unique = {}

    for row in rows:
        try:
            epoch = int(row["epoch"])
            price = float(row["price"])

            if epoch > 0 and price > 0 and math.isfinite(price):
                unique[epoch] = {
                    "epoch": epoch,
                    "price": price
                }

        except (KeyError, TypeError, ValueError):
            continue

    return [unique[k] for k in sorted(unique)]


# ==========================================
# DERIV HISTORIQUE
# ==========================================

def get_history():

    ws = websocket.create_connection(
        WS_URL,
        timeout=30
    )

    try:

        request = {
            "ticks_history": SYMBOL,
            "count": HISTORY_COUNT,
            "end": "latest",
            "style": "ticks",
            "req_id": 7001
        }

        ws.send(json.dumps(request))

        deadline = time.time() + 35

        while time.time() < deadline:

            msg = json.loads(ws.recv())

            if msg.get("error"):
                raise RuntimeError(
                    msg["error"].get("message")
                )

            if msg.get("req_id") == 7001:

                history = msg.get("history", {})

                prices = history.get("prices", [])
                times = history.get("times", [])

                rows = []

                for t, p in zip(times, prices):
                    rows.append({
                        "epoch": t,
                        "price": p
                    })

                return clean_ticks(rows)

        raise TimeoutError("Historique non reçu")

    finally:
        ws.close()


# ==========================================
# DERIV LIVE
# ==========================================

def collect_live(seconds):

    if seconds <= 0:
        return []

    ws = websocket.create_connection(
        WS_URL,
        timeout=10
    )

    ws.settimeout(2)

    rows = []

    try:

        ws.send(json.dumps({
            "ticks": SYMBOL,
            "subscribe": 1,
            "req_id": 7002
        }))

        deadline = time.time() + seconds

        while time.time() < deadline:

            try:
                msg = json.loads(ws.recv())

            except websocket.WebSocketTimeoutException:
                continue

            if msg.get("error"):
                raise RuntimeError(
                    msg["error"].get("message")
                )

            tick = msg.get("tick")

            if tick:

                rows.append({
                    "epoch": tick.get("epoch", time.time()),
                    "price": tick["quote"]
                })

        return clean_ticks(rows)

    finally:
        ws.close()


# ==========================================
# MATHEMATIQUES
# ==========================================

def percentile(values, pct):

    values = sorted(values)

    if not values:
        return None

    pos = (len(values) - 1) * pct / 100

    low = int(pos)
    high = math.ceil(pos)

    if low == high:
        return values[low]

    return (
        values[low]
        + (values[high] - values[low])
        * (pos - low)
    )


def rsi_series(prices, period):

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

    def calculate(g, l):

        if l == 0:
            return 100 if g > 0 else 50

        rs = g / l

        return 100 - (100 / (1 + rs))

    result[period] = calculate(
        avg_gain,
        avg_loss
    )

    for i in range(period + 1, len(prices)):

        change = prices[i] - prices[i - 1]

        gain = max(change, 0)
        loss = max(-change, 0)

        avg_gain = (
            (period - 1) * avg_gain + gain
        ) / period

        avg_loss = (
            (period - 1) * avg_loss + loss
        ) / period

        result[i] = calculate(
            avg_gain,
            avg_loss
        )

    return result


def ema_series(values, period):

    result = [None] * len(values)

    if not values:
        return result

    alpha = 2 / (period + 1)

    ema = values[0]

    result[0] = ema

    for i in range(1, len(values)):

        ema = (
            alpha * values[i]
            + (1 - alpha) * ema
        )

        result[i] = ema

    return result


def stochastic_series(prices, period, d_period=3):

    k = [None] * len(prices)
    d = [None] * len(prices)

    for i in range(period - 1, len(prices)):

        window = prices[
            i - period + 1:i + 1
        ]

        low = min(window)
        high = max(window)

        if high == low:
            k[i] = 50

        else:
            k[i] = (
                100 * (prices[i] - low)
                / (high - low)
            )

    for i in range(d_period - 1, len(prices)):

        window = k[
            i - d_period + 1:i + 1
        ]

        if all(x is not None for x in window):

            d[i] = sum(window) / d_period

    return k, d


# ==========================================
# INDICATEURS
# ==========================================

def build_indicators(prices):

    rsi = {
        p: rsi_series(prices, p)
        for p in (6, 9, 14)
    }

    ema = {
        p: ema_series(prices, p)
        for p in (5, 8, 12, 20, 30, 50)
    }

    stochastic = {
        p: stochastic_series(prices, p, 3)
        for p in (5, 9, 14)
    }

    return rsi, ema, stochastic


# ==========================================
# CONFIGURATIONS
# ==========================================

def generate_configs():

    result = []

    for rp in (6, 9, 14):

        for rsi_low in (30, 35, 40):

            for rsi_high in (65, 70, 75):

                for fast, slow in (
                    (5, 20),
                    (8, 30),
                    (12, 50)
                ):

                    for kp in (5, 9, 14):

                        for stoch_low in (20, 30):

                            result.append((
                                rp,
                                rsi_low,
                                rsi_high,
                                fast,
                                slow,
                                kp,
                                stoch_low
                            ))

    return result


# ==========================================
# SIGNAL
# ==========================================

def is_signal(
    i,
    cfg,
    rsi_map,
    ema_map,
    stoch_map
):

    (
        rp,
        low,
        high,
        fast,
        slow,
        kp,
        stoch_low
    ) = cfg

    rsi = rsi_map[rp]

    k, d = stoch_map[kp]

    if i < 1:
        return False

    values = (
        rsi[i],
        rsi[i - 1],
        k[i],
        k[i - 1],
        d[i],
        d[i - 1]
    )

    if any(x is None for x in values):
        return False

    # RSI remonte depuis une zone basse.

    rsi_rebound = (
        rsi[i - 1] <= low
        and rsi[i] > rsi[i - 1]
    )

    # Évite les RSI déjà trop élevés.

    rsi_not_overheated = rsi[i] < high

    # Croisement haussier du Stochastic.

    stochastic_cross = (
        k[i - 1] <= d[i - 1]
        and k[i] > d[i]
        and k[i - 1] <= stoch_low
    )

    # Filtre de tendance EMA.

    ema_trend = (
        ema_map[fast][i]
        >= ema_map[slow][i]
    )

    return (
        rsi_rebound
        and rsi_not_overheated
        and stochastic_cross
        and ema_trend
    )


# ==========================================
# ÉVALUATION
# ==========================================

def evaluate(
    cfg,
    start,
    end,
    flags,
    rsi_map,
    ema_map,
    stoch_map
):

    signals = 0
    hits = 0

    last_signal = -FUTURE_TICKS

    stop = min(
        end,
        len(flags) - FUTURE_TICKS
    )

    for i in range(max(1, start), stop):

        if i - last_signal <= FUTURE_TICKS:
            continue

        if is_signal(
            i,
            cfg,
            rsi_map,
            ema_map,
            stoch_map
        ):

            signals += 1

            if any(
                flags[i + 1:i + FUTURE_TICKS + 1]
            ):
                hits += 1

            last_signal = i

    rate = hits / signals if signals else 0

    return {
        "signals": signals,
        "hits": hits,
        "hit_rate": rate
    }


# ==========================================
# ANALYSE PRINCIPALE
# ==========================================

def analyze(db):

    ticks = clean_ticks(db.get("ticks", []))

    prices = [
        x["price"]
        for x in ticks
    ]

    total = len(prices)

    if total < 100:

        db["analysis"] = {
            "version": VERSION,
            "status": "insufficient_data",
            "total_ticks": total,
            "updated_at": now()
        }

        db["prediction"] = {
            "status": "not_enough_data"
        }

        db["spikes"] = []

        return

    changes = [0.0]

    for i in range(1, total):

        changes.append(
            prices[i] - prices[i - 1]
        )

    train_end = int(total * 0.60)
    validation_end = int(total * 0.80)

    positive = [
        x for x in changes[1:train_end]
        if x > 0
    ]

    negative = sum(
        1 for x in changes[1:train_end]
        if x < 0
    )

    zero = sum(
        1 for x in changes[1:train_end]
        if x == 0
    )

    # Seuil de spike calculé sur les hausses de l'entraînement.

    if len(positive) >= 5:

        threshold = percentile(
            positive,
            95
        )

        threshold_method = "95th_percentile"

    elif positive:

        threshold = max(positive)

        threshold_method = "provisional_small_sample"

    else:

        threshold = None

        threshold_method = "no_positive_change"

    flags = [False] * total
    spikes = []

    if threshold is not None:

        for i in range(1, total):

            if (
                changes[i] > 0
                and changes[i] >= threshold
            ):

                flags[i] = True

                spikes.append({
                    "epoch": ticks[i]["epoch"],
                    "price": ticks[i]["price"],
                    "change": round(changes[i], 8)
                })

    diagnostics = {
        "total_ticks": total,
        "train_ticks": train_end,
        "positive_changes_train": len(positive),
        "negative_changes_train": negative,
        "zero_changes_train": zero,
        "largest_positive_change_train": (
            round(max(positive), 8)
            if positive else None
        ),
        "spike_threshold_points": (
            round(threshold, 8)
            if threshold is not None else None
        ),
        "threshold_method": threshold_method,
        "detected_spikes_total": len(spikes),
        "future_ticks": FUTURE_TICKS
    }

    db["spikes"] = spikes[-100:]

    # On ne bloque plus à 20 variations positives.
    # Il faut toutefois quelques observations pour définir un seuil.

    if threshold is None or len(positive) < 5:

        db["analysis"] = {
            "version": VERSION,
            "status": "insufficient_positive_returns",
            "updated_at": now(),
            **diagnostics
        }

        db["prediction"] = {
            "status": "not_enough_positive_training_data"
        }

        return

    rsi_map, ema_map, stoch_map = build_indicators(prices)

    all_configs = generate_configs()

    training = []

    for cfg in all_configs:

        result = evaluate(
            cfg,
            1,
            train_end,
            flags,
            rsi_map,
            ema_map,
            stoch_map
        )

        if result["signals"] >= 3:

            training.append((
                result["hit_rate"],
                result["hits"],
                result["signals"],
                cfg
            ))

    training.sort(
        reverse=True,
        key=lambda x: (x[0], x[1], x[2])
    )

    validation = []

    for _, _, _, cfg in training[:100]:

        result = evaluate(
            cfg,
            train_end,
            validation_end,
            flags,
            rsi_map,
            ema_map,
            stoch_map
        )

        if result["signals"] >= 2:

            validation.append((
                result["hit_rate"],
                result["hits"],
                result["signals"],
                cfg
            ))

    validation.sort(
        reverse=True,
        key=lambda x: (x[0], x[1], x[2])
    )

    tested = []

    for _, _, _, cfg in validation[:30]:

        result = evaluate(
            cfg,
            validation_end,
            total,
            flags,
            rsi_map,
            ema_map,
            stoch_map
        )

        if result["signals"] >= 2:

            tested.append((
                result["hit_rate"],
                result["hits"],
                result["signals"],
                cfg
            ))

    tested.sort(
        reverse=True,
        key=lambda x: (x[0], x[1], x[2])
    )

    best = []

    for rate, hits, signals, cfg in tested[:10]:

        (
            rp,
            low,
            high,
            fast,
            slow,
            kp,
            stoch_low
        ) = cfg

        best.append({

            "rsi_period": rp,
            "rsi_low": low,
            "rsi_high_filter": high,

            "ema_fast": fast,
            "ema_slow": slow,

            "stochastic_k": kp,
            "stochastic_level": stoch_low,

            "test_signals": signals,
            "test_hits": hits,
            "test_hit_rate": round(rate, 4)

        })

    test_start = validation_end
    test_end = max(
        validation_end,
        total - FUTURE_TICKS
    )

    opportunities = max(
        0,
        test_end - test_start
    )

    baseline_hits = sum(
        1
        for i in range(test_start, test_end)
        if any(
            flags[i + 1:i + FUTURE_TICKS + 1]
        )
    )

    baseline = (
        baseline_hits / opportunities
        if opportunities else 0
    )

    status = (
        "ok"
        if best
        else "no_reliable_configuration"
    )

    db["analysis"] = {

        "version": VERSION,
        "status": status,
        "updated_at": now(),

        **diagnostics,

        "configurations_tested": len(all_configs),

        "training_configurations_with_signals": len(training),

        "validation_configurations_with_signals": len(validation),

        "test_configurations_with_signals": len(tested),

        "baseline_test_hit_rate": round(baseline, 4),

        "best_configurations": best,

        "warning": (
            "Un petit nombre de signaux ne permet pas "
            "de conclure à une performance fiable."
        )
    }

    if best:

        db["prediction"] = {

            "status": "experimental",
            "symbol": SYMBOL,
            "updated_at": now(),

            "best_configuration": best[0],

            "baseline_test_hit_rate": round(
                baseline,
                4
            ),

            "note": (
                "Résultat statistique expérimental. "
                "Aucun trade n'est exécuté."
            )
        }

    else:

        db["prediction"] = {
            "status": "no_validated_signal"
        }


# ==========================================
# EXÉCUTION
# ==========================================

def main():

    db = load_db()

    errors = []

    try:

        print("Téléchargement historique", SYMBOL)

        history = get_history()

        print("Ticks historiques :", len(history))

        db["ticks"] = clean_ticks(
            db.get("ticks", []) + history
        )

    except Exception as e:

        errors.append("historique: " + str(e))

        print("Erreur historique :", e)

    try:

        print(
            "Collecte live pendant",
            COLLECT_SECONDS,
            "secondes"
        )

        live = collect_live(COLLECT_SECONDS)

        print("Ticks live :", len(live))

        db["ticks"] = clean_ticks(
            db.get("ticks", []) + live
        )

    except Exception as e:

        errors.append("live: " + str(e))

        print("Erreur live :", e)

    db["ticks"] = clean_ticks(
        db.get("ticks", [])
    )[-MAX_STORED_TICKS:]

    db["errors"] = errors[-20:]

    print("Total ticks :", len(db["ticks"]))

    analyze(db)

    analysis = db.get("analysis", {})

    print("\n=== DIAGNOSTIC SPIKE1000 V7 ===")

    for key in (
        "status",
        "total_ticks",
        "train_ticks",
        "positive_changes_train",
        "negative_changes_train",
        "zero_changes_train",
        "largest_positive_change_train",
        "spike_threshold_points",
        "detected_spikes_total",
        "configurations_tested"
    ):

        if key in analysis:
            print(key, ":", analysis[key])

    print("================================\n")

    save_db(db)

    print("db.json sauvegardé.")

    if errors:
        print("Erreurs :", errors)


if __name__ == "__main__":
    main()