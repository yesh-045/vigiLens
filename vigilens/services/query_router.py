EVENT_KEYWORDS = {"fall", "accident", "injury", "alert"}


def route_query(query: str) -> str:
    q = (query or "").lower()
    if any(keyword in q for keyword in EVENT_KEYWORDS):
        return "event"
    return "activity"
