from redis import Redis
from jamboree.base.processors.abstracts import Processor
from jamboree.base.processors.event import JamboreeEvents
from jamboree.base.processors.files import JamboreeFileProcessor
class Jamboree(Processor):
    def __init__(self, **kwargs) -> None:
        super().__init__()

        redis_host = kwargs.get("REDIS_HOST", "localhost")
        redis_port = int(kwargs.get("REDIS_PORT", "6379"))
        mongo_host = kwargs.get("MONGO_HOST", "localhost")
        rconn = Redis(host=redis_host, port=redis_port)
        # redis.Redis(redis_host, port=redis_port)

        self.event = JamboreeEvents(
            mongodb_host=mongo_host,
            redis_host=redis_host,
            redis_port=redis_port
        )

        # Set the files management here
        self.storage = JamboreeFileProcessor()
        self.storage.rconn = rconn
        self.event.rconn = rconn
        self.event.initialize()
        self.storage.initialize()
        