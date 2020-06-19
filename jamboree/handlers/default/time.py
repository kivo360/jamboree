import uuid
import time
import maya
from jamboree.handlers.default import DBHandler
from jamboree import Jamboree
from jamboree import JamboreeNew
from loguru import logger


class TimeHandler(DBHandler):
    """ 
        # TimeHandler
        --- 
        The time handler is used to both do simple calculations of time and to maintain the head.

        The `head` is the current time we're referring to. 

        ```
            time_handler = TimeHandler()
            time_handler['episode'] = "episodeid"
            time_handler['live'] = False
            time_handler.reset()
            current_time:float = time_handler.head
        ```
    """

    def __init__(self):
        super().__init__()
        self.entity = "timeindexing"
        self.required = {
            "episode": str,
            "live": bool,
        }
        self.micro = 1000
        self.seconds = 0
        self.minutes = 0
        self.hours = 10
        self.days = 0
        self.weeks = 0

        self.step_micro = 1000
        self.step_seconds = 0
        self.step_minutes = 0
        self.step_hours = 10
        self.step_days = 0
        self.step_weeks = 0

        self._head = time.time()

    @property
    def looks(self):
        lookback_dict = {
            "microseconds": self.micro,
            "seconds": self.seconds,
            "minutes": self.minutes,
            "hours": self.hours,
            "days": self.days,
            "weeks": self.weeks,
        }
        return lookback_dict

    @property
    def steps(self):
        stepsize_dict = {
            "microseconds": self.step_micro,
            "seconds": self.step_seconds,
            "minutes": self.step_minutes,
            "hours": self.step_hours,
            "days": self.step_days,
            "weeks": self.step_weeks,
        }
        return stepsize_dict

    @property
    def lookback_params(self):
        self.load_lookback()
        lookback_dict = self.looks
        return lookback_dict

    @property
    def stepsize_params(self):
        self.load_stepsize()
        stepsize_dict = self.steps
        return stepsize_dict

    @property
    def head(self):
        if self["episode"] == "live" and self["live"] == True:
            return maya.now()._epoch
        self.load_head()
        return self._head

    @head.setter
    def head(self, _head):

        self._head = _head
        self.save_head()

    @property
    def tail(self):
        head = self.head
        look_params = self.lookback_params
        tail = maya.MayaDT(head).subtract(**look_params)
        return tail._epoch

    def change_lookback(
        self, microseconds=1000, seconds=0, minutes=0, hours=10, days=0, weeks=0
    ):
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

    def change_stepsize(
        self, microseconds=1000, seconds=0, minutes=0, hours=10, days=0, weeks=0
    ):
        self.load_stepsize()
        if microseconds != 1000:
            self.step_micro = microseconds
        if seconds != 0:
            self.step_seconds = seconds
        if minutes != 0:
            self.step_minutes = minutes
        if hours != 10:
            self.step_hours = hours
        if days != 0:
            self.step_days = days
        if weeks != 0:
            self.step_weeks = weeks

        self.save_stepsize()

    """ 
        # Index Commands
    """

    def save_head(self):
        alt = {"detail": "head"}
        # head_data = {"time": self._head}
        self.set_single(self._head, alt=alt, is_serialized=False)

    def latest_head(self) -> dict:
        alt = {"detail": "head"}
        item = self.get_single(alt=alt, is_serialized=False)
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
        params = self.looks
        self.save(params, alt=alt)


    def count_stepsize(self) -> int:
        alt = {"detail": "stepsize"}
        lookback_count = self.count(alt=alt)
        return lookback_count

    def latest_stepsize(self) -> dict:
        alt = {"detail": "stepsize"}
        lookback = self.last(alt=alt)
        return lookback

    def save_stepsize(self):
        """ Save monitored assets """
        alt = {"detail": "stepsize"}
        self.save(self.steps, alt=alt)
    def load_lookback(self):
        count = self.count_lookback()
        if count > 0:
            look = self.latest_lookback()
            self.micro = look.get("microseconds", 1000)
            self.seconds = look.get("seconds", 0)
            self.minutes = look.get("minutes", 0)
            self.hours = look.get("hours", 10)
            self.days = look.get("days", 0)
            self.weeks = look.get("weeks", 0)

    def load_stepsize(self):
        count = self.count_stepsize()
        if count > 0:
            look = self.latest_stepsize()
            self.step_micro = look.get("microseconds", 1000)
            self.step_seconds = look.get("seconds", 0)
            self.step_minutes = look.get("minutes", 0)
            self.step_hours = look.get("hours", 10)
            self.step_days = look.get("days", 0)
            self.step_weeks = look.get("weeks", 0)

    def load_head(self):
        if self.count_headindex() > 0:
            head = self.latest_head()
            if head is None:
                return time.time()
            self._head = float(head.decode("utf-8"))

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

    def step(self) -> None:
        head = self.head
        step_params = self.stepsize_params
        self.head = maya.MayaDT(head).add(**step_params)._epoch
        self.save_head()

    def peak(self) -> float:
        """ Get the time exactly one step ahead"""
        head = self.head
        step_params = self.stepsize_params
        new_head = maya.MayaDT(head).add(**step_params)._epoch
        return new_head

    def peak_far(self) -> float:
        """ 
            # PEAK FAR
            ---
            Peak far into the future. One step head + one lookahead_params 

            Doesn't modify the head
        """
        head = self.head
        step_params = self.stepsize_params
        lookahead_params = self.lookback_params
        new_head = maya.MayaDT(head).add(**step_params).add(**lookahead_params)._epoch
        return new_head

    def peak_back(self) -> float:
        head = self.head
        step_params = self.stepsize_params
        new_head = maya.MayaDT(head).subtract(**step_params)._epoch
        return new_head

    def peak_back_far(self) -> float:
        """ 
            # PEAK FAR
            ---
            Peak far into the future. One step head + one lookahead_params 
        """
        head = self.head
        step_params = self.stepsize_params
        lookahead_params = self.lookback_params
        new_head = (
            maya.MayaDT(head)
            .subtract(**step_params)
            .subtract(**lookahead_params)
            ._epoch
        )
        return new_head

    def peak_back_num(self, n: int = 1) -> float:
        """ 
            # Peak Back Num
            ---
            Peak far into the future. One step head + one lookahead_params

            Parameters
            ----------
                n: {int} - The number of steps backwards. Used to get the information.
        """
        if n < 1:
            raise ValueError("Number n needs to be greater than 1")
        head = self.head
        step_params = self.stepsize_params
        current_position = maya.MayaDT(head)
        for _ in range(n):
            current_position = current_position.subtract(**step_params)
        peak_position = current_position._epoch
        return peak_position

    def peak_back_num_tail(self, n: int = 1) -> float:
        """ 
            # Peak Back Num
            ---
            Peak far into the future. One step head + one lookahead_params

            Parameters
            ----------
                n: {int} - The number of steps backwards. Used to get the information.
        """
        if n < 1:
            raise ValueError("Number n needs to be greater than 1")
        head = self.head
        step_params = self.stepsize_params
        look_params = self.lookback_params
        current_position = maya.MayaDT(head)
        for _ in range(n):
            current_position = current_position.subtract(**step_params)
        peak_position = current_position.subtract(**look_params)._epoch
        return peak_position

    def step_back(self) -> None:
        head = self.head
        step_params = self.stepsize_params
        self.head = maya.MayaDT(head).subtract(**step_params)._epoch
        self.save_head()


if __name__ == "__main__":
    jambo = Jamboree()
    timehandler = TimeHandler()
    timehandler.processor = jambo
    eid = uuid.uuid4().hex
    timehandler["episode"] = eid
    timehandler["live"] = False
    timehandler.reset()
    
    timehandler.head = maya.MayaDT(timehandler.head).subtract(weeks=20, days=9)._epoch
    timehandler.change_stepsize(microseconds=0, days=1, hours=0)
    for _ in range(100):
        logger.warning(maya.MayaDT(timehandler.head))
        timehandler.step()