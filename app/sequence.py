"""相邻开机段的时序核查（纯函数）。

段号即记录仪的重启顺序：按段号升序排列后，逐对核对「前段末点」与「后段首点」
在已发布校时版中的时刻。

判定规则（严格三态，缺一不可）：
- pass    ：两个边界读数都已校时，且前段末点 UTC 严格早于后段首点 UTC；
- conflict：两个边界读数都已校时，但前段末点 UTC 不早于（>=）后段首点 UTC；
- pending ：任一边界读数未进入已发布校时版（无锚点 / 超出锚点范围），
            时序无法判定。**待核绝不降级为通过**。

「首点 / 末点」指该段原始读数中本地秒数最小 / 最大者（同段秒数唯一），
不依赖校时结果；校时只决定它们在已发布版本中的 UTC。
"""
from __future__ import annotations


def _endpoint(reading: dict, calibrated: dict) -> dict:
    cal = calibrated.get(reading["id"])
    return {
        "reading_id": reading["id"],
        "batch_id": reading["batch_id"],
        "row_no": reading["row_no"],
        "segment": reading["segment"],
        "local_seconds": reading["local_seconds"],
        "utc_time": cal["utc_time"] if cal else None,
        "anchor_version": cal["anchor_version"] if cal else None,
    }


def build_sequence(readings, calibrated_rows) -> tuple[list[dict], list[dict]]:
    """返回 (segments, checks)。

    readings: 含 id/segment/local_seconds/batch_id/row_no 的原始读数
    calibrated_rows: compute_calibration 的结果（含 reading_id/utc_time/anchor_version）
    """
    calibrated = {r["reading_id"]: r for r in calibrated_rows}

    by_segment: dict[int, list[dict]] = {}
    for r in readings:
        by_segment.setdefault(r["segment"], []).append(r)

    segments: list[dict] = []
    for segment in sorted(by_segment):
        pts = sorted(by_segment[segment], key=lambda r: r["local_seconds"])
        first = _endpoint(pts[0], calibrated)
        last = _endpoint(pts[-1], calibrated)
        missing = []
        if first["utc_time"] is None:
            missing.append("首点")
        if last["utc_time"] is None:
            missing.append("末点")
        segments.append({
            "segment": segment,
            "point_count": len(pts),
            "calibrated_count": sum(1 for p in pts if p["id"] in calibrated),
            "first": first,
            "last": last,
            "boundary_status": "pending" if missing else "ok",
            "boundary_reason": (
                "、".join(missing) + "未进入已发布校时版" if missing else ""
            ),
        })

    checks: list[dict] = []
    for prev, nxt in zip(segments, segments[1:]):
        missing = []
        if prev["last"]["utc_time"] is None:
            missing.append("前段末点")
        if nxt["first"]["utc_time"] is None:
            missing.append("后段首点")
        if missing:
            status = "pending"
            reason = "、".join(missing) + "未校时，时序无法判定"
        elif prev["last"]["utc_time"] >= nxt["first"]["utc_time"]:
            status = "conflict"
            reason = (
                f"前段末点 {prev['last']['utc_time']} 不早于"
                f"后段首点 {nxt['first']['utc_time']}"
            )
        else:
            status = "pass"
            reason = ""
        checks.append({
            "prev_segment": prev["segment"],
            "next_segment": nxt["segment"],
            "status": status,
            "reason": reason,
            "prev": prev["last"],
            "next": nxt["first"],
        })
    return segments, checks


def segment_delivery(segments: list[dict], checks: list[dict]) -> dict[int, dict]:
    """按核查结论推导每段的按日供数资格。

    - 参与任一冲突 → blocked（不可交付）；
    - 本段首/末边界读数未校时，或参与任一待核 → pending（待核，暂不可交付）；
    - 否则 → deliverable。
    冲突优先于待核；任何未校时的边界都不会被显示为通过或可交付。
    """
    related: dict[int, list[dict]] = {s["segment"]: [] for s in segments}
    own_pending = {s["segment"]: s["boundary_status"] == "pending" for s in segments}
    for c in checks:
        related[c["prev_segment"]].append(c)
        related[c["next_segment"]].append(c)

    out: dict[int, dict] = {}
    for seg, cs in related.items():
        conflicts = [c for c in cs if c["status"] == "conflict"]
        pendings = [c for c in cs if c["status"] == "pending"]
        if conflicts:
            peers = "、".join(str(c["next_segment"] if c["prev_segment"] == seg else c["prev_segment"])
                              for c in conflicts)
            out[seg] = {"status": "blocked", "reason": f"与段 {peers} 的边界时序冲突"}
        elif own_pending[seg]:
            out[seg] = {"status": "pending", "reason": "本段边界读数未进入已发布校时版，待核"}
        elif pendings:
            peers = "、".join(str(c["next_segment"] if c["prev_segment"] == seg else c["prev_segment"])
                              for c in pendings)
            out[seg] = {"status": "pending", "reason": f"与段 {peers} 的边界读数未校时，待核"}
        else:
            out[seg] = {"status": "deliverable", "reason": ""}
    return out
