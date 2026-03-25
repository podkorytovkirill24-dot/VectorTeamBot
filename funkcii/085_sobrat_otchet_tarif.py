def _report_rows(conn: sqlite3.Connection, statuses: List[str], limit: Optional[int] = 50) -> List[sqlite3.Row]:
    if not statuses:
        return []
    placeholders = ",".join("?" * len(statuses))
    if limit is None or int(limit) <= 0:
        return conn.execute(
            f"SELECT q.phone, q.user_id, q.username, q.status, q.assigned_at, q.completed_at, "
            f"t.duration_min, t.name AS tariff "
            f"FROM queue_numbers q LEFT JOIN tariffs t ON q.tariff_id = t.id "
            f"WHERE q.status IN ({placeholders}) "
            f"ORDER BY q.completed_at DESC",
            (*statuses,),
        ).fetchall()
    return conn.execute(
        f"SELECT q.phone, q.user_id, q.username, q.status, q.assigned_at, q.completed_at, "
        f"t.duration_min, t.name AS tariff "
        f"FROM queue_numbers q LEFT JOIN tariffs t ON q.tariff_id = t.id "
        f"WHERE q.status IN ({placeholders}) "
        f"ORDER BY q.completed_at DESC LIMIT ?",
        (*statuses, int(limit)),
    ).fetchall()


def _duration_info(row: sqlite3.Row) -> Tuple[Optional[int], Optional[int], str]:
    assigned_at = row["assigned_at"]
    completed_at = row["completed_at"]
    if not assigned_at or not completed_at:
        return None, None, "—"
    duration_sec = int(completed_at) - int(assigned_at)
    if duration_sec < 0:
        return None, None, "—"
    tariff_min = int(row["duration_min"] or 0)
    mark = "—"
    if tariff_min > 0:
        mark = "✅" if duration_sec >= tariff_min * 60 else "❌"
    duration_min = duration_sec // 60
    return duration_min, tariff_min if tariff_min > 0 else None, mark


def _time_label(ts: Optional[int]) -> str:
    return format_ts(ts) if ts else "-"


def build_report_stood(conn: sqlite3.Connection, limit: Optional[int] = 50) -> str:
    rows = _report_rows(conn, ["success"], limit=limit)
    period_label = (
        "Период: все завершённые" if not limit or int(limit) <= 0 else f"Период: последние {int(limit)} завершённых"
    )
    lines = [
        "✅ Отстоявшие номера",
        period_label,
        "",
    ]
    if not rows:
        lines.append("Нет данных.")
        return "\n".join(lines)
    for r in rows:
        user = format_user_label(r["user_id"], r["username"])
        start = _time_label(r["assigned_at"])
        end = _time_label(r["completed_at"])
        duration_min, tariff_min, mark = _duration_info(r)
        duration_label = f"{duration_min}м" if duration_min is not None else "-"
        tariff_label = f"{tariff_min}м" if tariff_min is not None else "-"
        lines.append(
            f"{r['phone']} | {user} | старт: {start} | встал: {end} | "
            f"{duration_label} | тариф: {tariff_label} | {mark}"
        )
    return "\n".join(lines)


def build_report_csv(rows: List[sqlite3.Row]) -> str:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow(
        [
            "phone",
            "user_id",
            "username",
            "status",
            "assigned_at",
            "completed_at",
            "stood_min",
            "tariff",
            "tariff_min",
        ]
    )
    for r in rows:
        assigned_at = r["assigned_at"]
        completed_at = r["completed_at"]
        stood_min = ""
        if assigned_at and completed_at:
            stood_min = int(max(0, (int(completed_at) - int(assigned_at)) // 60))
        writer.writerow(
            [
                r["phone"],
                r["user_id"],
                r["username"] or "",
                r["status"],
                format_ts(assigned_at),
                format_ts(completed_at),
                stood_min,
                r["tariff"] or "",
                int(r["duration_min"] or 0),
            ]
        )
    return output.getvalue()


def build_report_tariff(conn: sqlite3.Connection) -> str:
    return build_report_stood(conn)
