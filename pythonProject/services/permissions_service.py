from __future__ import annotations

NEWS_MANAGE_ACTION = "news.manage"


def extend_access_matrix(access_matrix: dict, access_labels: dict, managed_actions: list) -> None:
    """Добавляет право управления новостями, если его ещё нет."""
    access_matrix.setdefault(NEWS_MANAGE_ACTION, {"admin", "moderator"})
    access_labels.setdefault(NEWS_MANAGE_ACTION, "управление новостями Mini App")
    if not any(action == NEWS_MANAGE_ACTION for action, _ in managed_actions):
        managed_actions.append((NEWS_MANAGE_ACTION, "📰 Новости Mini App"))
