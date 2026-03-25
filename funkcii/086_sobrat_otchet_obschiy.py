def build_report_general(conn: sqlite3.Connection, limit: Optional[int] = 50) -> str:
    rows = _report_rows(conn, ["success", "slip"], limit=limit)
    period_label = (
        "Период: все завершённые" if not limit or int(limit) <= 0 else f"Период: последние {int(limit)} завершённых"
    )
    lines = [
        "📋 Общий отчет",
        period_label,
        "",
    ]
    if not rows:
        lines.append("Нет данных.")
        return "\n".join(lines)
    for r in rows:
        user = format_user_label(r["user_id"], r["username"])
        start = _time_label(r["assigned_at"])
        stood_time = _time_label(r["completed_at"]) if r["status"] == "success" else "-"
        slip_time = _time_label(r["completed_at"]) if r["status"] == "slip" else "-"
        duration_min, tariff_min, mark = _duration_info(r)
        duration_label = f"{duration_min}м" if duration_min is not None else "-"
        tariff_label = f"{tariff_min}м" if tariff_min is not None else "-"
        status_label = "✅ встал" if r["status"] == "success" else "❌ слетел"
        lines.append(
            f"{status_label} | {r['phone']} | {user} | старт: {start} | "
            f"встал: {stood_time} | слетел: {slip_time} | {duration_label} | "
            f"тариф: {tariff_label} | {mark}"
        )
    return "\n".join(lines)
