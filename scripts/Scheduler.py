import os
from .TopDownBackScheduler import TopDownBackScheduler
from .TopDownFBScheduler import TopDownFBScheduler
from .BottomUpBackScheduler import BottomUpBackScheduler
from .BottomUpFBScheduler import BottomUpFBScheduler
from .utils import produce_final_schedule

class Scheduler:
    def __init__(self, config, hardcap={}):
        self.output_dir = config["output_dir"]
        self.forecast_years = config["end_year"] - config["start_year"] - 2
        self.hardcap = hardcap  # TODO: include hardcap inside config

    def schedule(self, clean_df, wm_df, alg_name, original_csv, trade=""):
        scheduler = get_scheduler(alg_name)
        sched = scheduler.create_schedule(clean_df, wm_df, forecast_years=self.forecast_years, hardcap=self.hardcap)
        produce_final_schedule(sched, original_csv, os.path.join(self.output_dir, alg_name + trade + "-Final-Schedule" + ".csv"))


def get_scheduler(name):
    if name == "top-down-b":
        scheduler = TopDownBackScheduler()
    elif name == "top-down-fb":
        scheduler = TopDownFBScheduler()
    elif name == "bottom-up-b":
        scheduler = BottomUpBackScheduler()
    elif name == "bottom-up-fb":
        scheduler = BottomUpFBScheduler()
    else:
        raise ValueError
    return scheduler
