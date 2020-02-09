from jamboree.base.processors.abstracts import Processor
from jamboree.base.processors.event import JamboreeEvents

class Jamboree(Processor):
    def __init__(self, **kwargs) -> None:
        super().__init__()

        redis_host = kwargs.get("REDIS_HOST", "localhost")
        redis_port = int(kwargs.get("REDIS_PORT", "6379"))
        mongo_host = kwargs.get("MONGO_HOST", "localhost")

        self.event = JamboreeEvents(
            mongodb_host=mongo_host,
            redis_host=redis_host,
            redis_port=redis_port
        )