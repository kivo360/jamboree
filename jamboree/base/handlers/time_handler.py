import uuid
import time
import maya
from jamboree.base.handlers.main_handler import DBHandler
from jamboree import Jamboree

class TimeHandler(DBHandler):
    """ 
        # TimeHandler
        --- 
        The time handler is used to both do simple calculations of time and to maintain the head for what ever calculations we're working on.

        
    """
    def __init__(self):
        super().__init__()
        self.entity = "timeindexing"
        self.required = {
            "episode": str,
            "live": bool,
            "sub_detail": str
        }
        self.micro = 1000
        self.seconds = 0
        self.minutes = 0
        self.hours = 10
        self.days = 0
        self.weeks = 0
        self._head = time.time()
        

    @property
    def lookback_params(self):
        self.load_lookback()
        lookback_dict = {
            "microseconds": self.micro,
            "seconds":self.seconds,
            "minutes":self.minutes,
            "hours":self.hours,
            "days":self.days,
            "weeks":self.weeks
        }
        return lookback_dict

    @property
    def head(self):
        self.load_head()
        return self._head
    
    @head.setter
    def head(self, _head):
        self._head = _head
        self.save_head()

    

    

    def lookback(self, microseconds=1000, seconds=0, minutes=0, hours=10, days=0, weeks=0):
        self.load_lookback()
        if microseconds != 1000:
            self.micro = microseconds
        if seconds != 0:
            self.seconds = seconds
        if minutes != 0:
            self.minutes = minutes
        if hours != 10:
            self.hours = hours
        if days != 0:
            self.days = days
        if weeks != 0:
            self.weeks = weeks
        
        self.save_lookback()

    """ 
        # Index Commands
    """

    def save_head(self):
        alt = {"detail": "head"}
        head_data = {"time": self._head}
        self.set_single(head_data, alt=alt)
    
    def latest_head(self) -> dict:
        alt = {"detail": "head"}
        item = self.get_single(alt=alt)
        return item

    # def 

    def count_headindex(self) -> int:
        alt = {"detail": "headindex"}
        lookback_count = self.count(alt=alt)
        return lookback_count


    def save_headindex(self):
        """ Save monitored assets """
        alt = {"detail": "headindex"}
        self.save({"message": "Head Index"}, alt=alt)

    """ 
        # Lookback Commands
    """


    def count_lookback(self) -> int:
        alt = {"detail": "lookback"}
        lookback_count = self.count(alt=alt)
        return lookback_count


    def latest_lookback(self) -> dict:
        alt = {"detail": "lookback"}
        lookback = self.last(alt=alt)
        return lookback


    def save_lookback(self):
        """ Save monitored assets """
        alt = {"detail": "lookback"}
        self.save(self.lookback_params, alt=alt)
    
    def load_lookback(self):
        count = self.count_lookback()
        if count > 0:
            look = self.latest_lookback()
            self.micro = look.get('microseconds', 1000)
            self.seconds = look.get('seconds', 0)
            self.minutes = look.get('minutes', 0)
            self.hours = look.get('hours', 10)
            self.days = look.get('days', 0)
            self.weeks = look.get('weeks', 0)
    
    def load_head(self):
        if self.count_headindex() > 0:
            head = self.latest_head()
            head_time = head.get('time', time.time())
            self._head = head_time

    def _reset_lookback(self):
        if self.count_lookback() == 0:
            self.save_lookback()

    def _reset_headindex(self):
        if self.count_headindex() == 0:
            self.save_headindex()
            self.save_head()

    def reset(self):
        self._reset_lookback()
        self._reset_headindex()
    
    


if __name__ == "__main__":
    jambo = Jamboree()
    timehandler = TimeHandler()
    timehandler.event = jambo
    timehandler["episode"] = uuid.uuid4().hex
    timehandler["live"] = False
    timehandler['sub_detail'] = "whore"
    timehandler.reset()
    print(maya.MayaDT(timehandler.head))
    timehandler.head = maya.now().subtract(weeks=3, days=9)._epoch
    print(maya.MayaDT(timehandler.head))
