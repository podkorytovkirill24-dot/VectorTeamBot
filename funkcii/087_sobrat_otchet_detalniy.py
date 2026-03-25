def build_report_not_stood(conn: sqlite3.Connection, limit: Optional[int] = 50) -> str:
    rows = _report_rows(conn, ["slip"], limit=limit)
    period_label = (
        "Период: все завершённые" if not limit or int(limit) <= 0 else f"Период: последние {int(limit)} завершённых"
    )
    lines = [
        "❌ Не отстоявшие номера",
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
            f"{r['phone']} | {user} | старт: {start} | слетел: {end} | "
            f"{duration_label} | тариф: {tariff_label} | {mark}"
        )
    return "\n".join(lines)


def build_report_detailed(conn: sqlite3.Connection) -> str:
    return build_report_not_stood(conn)
