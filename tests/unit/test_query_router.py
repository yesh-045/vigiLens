from vigilens.services.query_router import route_query


class TestRouteQuery:
    def test_event_keywords_route_to_event(self):
        assert route_query("did someone fall") == "event"
        assert route_query("any accident happened?") == "event"
        assert route_query("possible injury detected") == "event"
        assert route_query("raise alert now") == "event"

    def test_non_event_queries_route_to_activity(self):
        assert route_query("person walking near sofa") == "activity"
        assert route_query("what happened in the last hour") == "activity"

    def test_empty_query_defaults_to_activity(self):
        assert route_query("") == "activity"
