"""
Analytics processing module for MayaCardio Edge Pipeline.

Implements Section 3 processing logic (identical online and offline):
- Window w: samples 400w to 400w + 399 of each sensor
- Outputs:
  - missing_1, missing_2: count of missing samples
  - complete: true if both are 0
  - per channel mean, min, max over available samples
  - magnitude: mean of sqrt(ch1^2 + ch2^2) over available Sensor 2 samples
  - heart_rate: beats per minute from Sensor 1, Channel 2 (5-second rolling window)
  - mode: 'online' or 'offline'
"""

import math
from database import get_db_connection


def _stats(values):
    """Compute (mean, min, max) rounded to 1 decimal place, or (None, None, None)."""
    if not values:
        return None, None, None
    return (
        round(sum(values) / len(values), 1),
        round(min(values), 1),
        round(max(values), 1)
    )


def compute_window_metrics(session_id, window_w, *args, **kwargs):
    """
    Compute metrics for window w according to Section 3.
    Accepts:
      compute_window_metrics(session_id, window_w, mode_str)
      compute_window_metrics(session_id, window_w, sensor_id, mode_str)
    """
    # Parse flexible arguments
    sensor_id = 1
    mode_str = "offline"
    if len(args) == 1:
        if isinstance(args[0], int):
            sensor_id = args[0]
        else:
            mode_str = str(args[0])
    elif len(args) >= 2:
        sensor_id = args[0]
        mode_str = str(args[1])
    
    if "mode_str" in kwargs:
        mode_str = kwargs["mode_str"]
    elif "mode" in kwargs:
        mode_str = kwargs["mode"]

    conn = get_db_connection()
    cursor = conn.cursor()

    start_sample = 400 * window_w
    end_sample = start_sample + 399
    expected_indices = set(range(start_sample, end_sample + 1))

    # Fetch Sensor 1 samples in window
    cursor.execute("""
        SELECT sample_index, ch1, ch2, ch3 FROM raw_samples
        WHERE session_id = ? AND sensor = 1 AND sample_index BETWEEN ? AND ?
        ORDER BY sample_index ASC
    """, (session_id, start_sample, end_sample))
    s1_rows = cursor.fetchall()
    s1_map = {r["sample_index"]: r for r in s1_rows}
    missing_1 = len(expected_indices - set(s1_map.keys()))

    # Fetch Sensor 2 samples in window
    cursor.execute("""
        SELECT sample_index, ch1, ch2 FROM raw_samples
        WHERE session_id = ? AND sensor = 2 AND sample_index BETWEEN ? AND ?
        ORDER BY sample_index ASC
    """, (session_id, start_sample, end_sample))
    s2_rows = cursor.fetchall()
    s2_map = {r["sample_index"]: r for r in s2_rows}
    missing_2 = len(expected_indices - set(s2_map.keys()))

    complete = (missing_1 == 0 and missing_2 == 0)

    # Per-channel stats for Sensor 1
    s1_ch1_vals = [r["ch1"] for r in s1_rows if r["ch1"] is not None]
    s1_ch2_vals = [r["ch2"] for r in s1_rows if r["ch2"] is not None]
    s1_ch3_vals = [r["ch3"] for r in s1_rows if r["ch3"] is not None]

    s1_ch1_mean, s1_ch1_min, s1_ch1_max = _stats(s1_ch1_vals)
    s1_ch2_mean, s1_ch2_min, s1_ch2_max = _stats(s1_ch2_vals)
    s1_ch3_mean, s1_ch3_min, s1_ch3_max = _stats(s1_ch3_vals)

    # Per-channel stats for Sensor 2
    s2_ch1_vals = [r["ch1"] for r in s2_rows if r["ch1"] is not None]
    s2_ch2_vals = [r["ch2"] for r in s2_rows if r["ch2"] is not None]

    s2_ch1_mean, s2_ch1_min, s2_ch1_max = _stats(s2_ch1_vals)
    s2_ch2_mean, s2_ch2_min, s2_ch2_max = _stats(s2_ch2_vals)

    # Magnitude: Mean of sqrt(ch1^2 + ch2^2) over available Sensor 2 samples
    s2_mags = [
        math.sqrt(r["ch1"]**2 + r["ch2"]**2)
        for r in s2_rows
        if r["ch1"] is not None and r["ch2"] is not None
    ]
    magnitude = round(sum(s2_mags) / len(s2_mags), 1) if s2_mags else None

    # Heart Rate Calculation: Sensor 1, Channel 2 peaks over 5 second rolling window
    heart_rate = None
    if window_w >= 4:
        five_sec_start = 400 * (window_w - 4)
        five_sec_end = 400 * (window_w + 1) - 1
        five_sec_total = five_sec_end - five_sec_start + 1  # 2000 samples

        # Requirement: heart_rate is null if any sample in those 5 seconds is missing
        cursor.execute("""
            SELECT sample_index, ch2 FROM raw_samples
            WHERE session_id = ? AND sensor = 1 AND sample_index BETWEEN ? AND ?
            ORDER BY sample_index ASC
        """, (session_id, five_sec_start, five_sec_end))
        hr_rows = cursor.fetchall()

        if len(hr_rows) == five_sec_total:
            # Build sample array indexed from five_sec_start
            ch2_data = [r["ch2"] for r in hr_rows]
            peaks = []

            # Peak detection rule:
            # sample i is a peak when:
            # 1. the 800 samples before it, and sample after it, are available
            # 2. larger than both neighbours (i-1 and i+1)
            # 3. larger than half max of 800 samples before it
            # 4. at least 100 samples after previous peak
            for i in range(800, len(ch2_data) - 1):
                val = ch2_data[i]
                prev_val = ch2_data[i - 1]
                next_val = ch2_data[i + 1]

                # Check condition 2: larger than both neighbours
                if val <= prev_val or val <= next_val:
                    continue

                past_800 = ch2_data[i - 800 : i]
                # Check condition 3: larger than half max of 800 before
                max_past = max(past_800)
                if val <= (0.5 * max_past):
                    continue

                # Check condition 4: at least 100 samples after previous peak
                if not peaks or (i - peaks[-1]) >= 100:
                    peaks.append(i)

            # Rule: heart_rate is null if fewer than 2 peaks
            if len(peaks) >= 2:
                gaps = [peaks[j] - peaks[j - 1] for j in range(1, len(peaks))]
                mean_gap_samples = sum(gaps) / len(gaps)
                mean_gap_seconds = mean_gap_samples / 400.0
                if mean_gap_seconds > 0:
                    heart_rate = round(60.0 / mean_gap_seconds, 1)

    # Save to window_results
    cursor.execute("""
        INSERT OR REPLACE INTO window_results (
            session_id, window_id, sensor, mode, missing_samples,
            missing_1, missing_2, complete,
            s1_ch1_mean, s1_ch1_min, s1_ch1_max,
            s1_ch2_mean, s1_ch2_min, s1_ch2_max,
            s1_ch3_mean, s1_ch3_min, s1_ch3_max,
            s2_ch1_mean, s2_ch1_min, s2_ch1_max,
            s2_ch2_mean, s2_ch2_min, s2_ch2_max,
            mean_val, min_val, max_val,
            magnitude, heart_rate
        ) VALUES (
            ?, ?, ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?, ?,
            ?, ?
        )
    """, (
        session_id, window_w, sensor_id, mode_str, missing_1,
        missing_1, missing_2, 1 if complete else 0,
        s1_ch1_mean, s1_ch1_min, s1_ch1_max,
        s1_ch2_mean, s1_ch2_min, s1_ch2_max,
        s1_ch3_mean, s1_ch3_min, s1_ch3_max,
        s2_ch1_mean, s2_ch1_min, s2_ch1_max,
        s2_ch2_mean, s2_ch2_min, s2_ch2_max,
        s1_ch1_mean, s1_ch1_min, s1_ch1_max,
        magnitude, heart_rate
    ))

    conn.commit()
    conn.close()

    return {
        "session_id": session_id,
        "window_id": window_w,
        "mode": mode_str,
        "missing_1": missing_1,
        "missing_2": missing_2,
        "complete": complete,
        "s1_ch1": {"mean": s1_ch1_mean, "min": s1_ch1_min, "max": s1_ch1_max},
        "s1_ch2": {"mean": s1_ch2_mean, "min": s1_ch2_min, "max": s1_ch2_max},
        "s1_ch3": {"mean": s1_ch3_mean, "min": s1_ch3_min, "max": s1_ch3_max},
        "s2_ch1": {"mean": s2_ch1_mean, "min": s2_ch1_min, "max": s2_ch1_max},
        "s2_ch2": {"mean": s2_ch2_mean, "min": s2_ch2_min, "max": s2_ch2_max},
        "magnitude": magnitude,
        "heart_rate": heart_rate
    }