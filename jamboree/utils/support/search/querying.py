class text(object):
    @staticmethod
    def exact(term):
        return {
            "type": "TEXT",
            "is_filter": True,
            "values": {
                "term": term,
                "is_exact": True
            }
        }