from jamboree.handlers.processors import DataProcessorsAbstract
import pandas as pd
import numpy as np
import scipy.stats as stats


class DynamicResample(DataProcessorsAbstract):
    def __init__(self, name, **kwargs) -> None:
        self.time_info = {
            "years": 0,
            "months": 0,
            "weeks":0,
            "days": 0,
            "hours": 0,
            "minutes": 0,
            "seconds": 0
        }
        self.time_formatting = {
            "seconds": "S",
            "minutes": "T",
            "hours": "H",
            "days": "D",
            "weeks":"W",
            "months": "M",
            "years": "Y"
        }

        self.base = 0
        super().__init__(name, **kwargs)


    def set_settings(self, **kwargs):
        """ Updates the time information"""
        self.time_info['seconds'] = kwargs.get("seconds",self.time_info['seconds'])
        self.time_info['minutes'] = kwargs.get("minutes",self.time_info['minutes'])
        self.time_info['hours'] = kwargs.get("hours",self.time_info['hours'])
        self.time_info['days'] = kwargs.get("days",self.time_info['days'])
        self.time_info['weeks'] = kwargs.get("weeks",self.time_info['weeks'])
        self.time_info['months'] = kwargs.get("months",self.time_info['months'])
        self.time_info['years'] = kwargs.get("years",self.time_info['years'])
        self.base = kwargs.get("base", self.base)

    def validate_existing_times(self):
        checkable_list = self.time_info.values()
        all_zero = all(x==0 for x in checkable_list)
        if all_zero:
            self.time_info['hours'] = 1

    def generate_time_string(self):
        self.validate_existing_times()
        final_string = ""
        for name, time_amount in self.time_info.items():
            if time_amount == 0:
                continue
            elif time_amount == 1:
                final_string = final_string + self.time_formatting.get(name)
                continue
            
            time_format = self.time_formatting.get(name)
            final_string = final_string + f"{time_amount}{time_format}"
        
        return final_string

    def process(self, data: pd.DataFrame) -> pd.DataFrame:
        if not (isinstance(data, pd.DataFrame)):
            return pd.DataFrame()
        
        if data.empty:
            return pd.DataFrame()
        # Do preprocessing here
        
        dtypes = data.dtypes
        aggregate_command = {}
        for k, v in dtypes.items():
            if np.issubdtype(v, np.number):
                command = {k:'mean'}
                aggregate_command.update(command)
                continue
            else:
                command = {k: lambda x: stats.mode(x)[0]}
                aggregate_command.update(command)
                continue
        
        rule = self.generate_time_string()

        if self.base == 0:
            resampled = data.resample(rule).apply(aggregate_command)
            return resampled
        resampled = data.resample(rule, base=self.base).apply(aggregate_command)
        return resampled

if __name__ == "__main__":
    import pandas_datareader.data as web
    data_msft = web.DataReader('MSFT','yahoo',start='2008/1/1',end='2020/3/8').round(2)
    mrsample = DynamicResample("modin", days=7)

    remicro = mrsample.process(data_msft)