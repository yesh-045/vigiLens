from vigilens.observability import trace

EVENT_KEYWORDS = {"fall", "accident", "injury", "alert"}


@trace(name="route_query")
def route_query(query: str) -> str:
    q = (query or "").lower()
    if any(keyword in q for keyword in EVENT_KEYWORDS):
        return "event"
    return "activity"
