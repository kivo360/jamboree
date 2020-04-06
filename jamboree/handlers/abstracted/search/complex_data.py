from jamboree.handlers.default.search import BaseSearchHandler


class ComplexDataSearchHandler(BaseSearchHandler):
    def __init__(self):
        self.requirements = {
            "name": str,
            "category": str,
            "episode": str,
            "loc": "GEO",
            "live": bool,
            "specified": list
        }