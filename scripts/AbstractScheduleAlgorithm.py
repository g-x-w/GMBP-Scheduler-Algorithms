import pandas as pd
import datetime
from abc import ABC, abstractmethod


class AbstractScheduleAlgorithm:
    @abstractmethod
    def create_schedule(self, clean_df, wm_df, forecast_years, hardcap):
        pass


    @staticmethod
    def is_constraints_satisfied(constraints: pd.DataFrame, date: str, scheduled_tasks: list[dict], 
        new_task_hrs: int, hard_capped=False, task_freq=0, add_task=1):
        """
        Function to check whether a constraint has been violated.
        """
        assert constraints.loc[date].any(), "Date not covered by constraints, please check constraints generation process!"
        assert not hard_capped, f"Hard cap constraint too strict for task sequence week frequency {task_freq}!"

        total_scheduled_hrs = sum([task["Hrs"] for task in scheduled_tasks])

        # NOTE: Change this based on how the constraints interface changes
        if total_scheduled_hrs + new_task_hrs > constraints.loc[date]["AllowedHours"] or \
            len(scheduled_tasks) + add_task > constraints.loc[date]["AllowedTasks"]:
            return False
        else:
            return True


    @staticmethod
    def compute_priority_score(task_entry, hardcap):
        """
        Function to compute a priority score for ordering tasks.
        A smaller priority score indicates that it is more crucial to be scheduled first.
        """
        task_sequence_weeks = int(task_entry["TaskSequence_Weeks"])
        task_hrs = int(task_entry["Hrs"])
        delta_weeks = int(task_entry["DeltaWeeks"])
        priority_score = (task_sequence_weeks + 1/task_hrs) / (1 + delta_weeks)

        # Force task to be scheduled if delta weeks is hard capped for the task sequence
        if task_sequence_weeks in hardcap:
            if delta_weeks >= hardcap[task_sequence_weeks]:
                priority_score = -1  
        return priority_score


    @staticmethod
    def convert_date_to_iso(date):
        """
        Function to map a date to the correponding Monday, representing the start of the week based on ISO 8601.
        """
        day_of_week = date.weekday()
        iso_date = date - datetime.timedelta(days=day_of_week)
        iso_date = iso_date.strftime("%Y-%m-%d")
        return iso_date


    @staticmethod
    def check_valid_schedule(sched, wm_df, sched_name):
        weeks_master = wm_df.set_index(["ScheduledWeek"])

        for date in weeks_master.index.values:
            tasks = sched[sched["ScheduledWeek"] == date].to_dict('records')
            assert AbstractScheduleAlgorithm.is_constraints_satisfied(weeks_master, date, tasks, 0, add_task=0), \
                f"Constraint failed for {date} with task_hours {sum([task['Hrs'] for task in tasks])} and number of tasks {len(tasks)}!"
        print(f"{sched_name} passes constraints!")


    @staticmethod
    def check_complete_task_list(task_df, sched, sched_name):
        task_set = set(task_df["DataSource"].values)
        sched_tasks_set = set(sched["DataSource"].values)
        missing_tasks = task_set.difference(sched_tasks_set)

        assert not missing_tasks, f"Schedule {sched_name} is missing tasks {missing_tasks} from task_df!"
        print(f"{sched_name} has no missing tasks!")

    @staticmethod
    def build_base_top_down_schedule(clean_df, forecast_years=10):
        # Currently we go from raw csv -> load_csv() -> clean_dataframe() -> ProcessedData df (tblTasks_London schema)
        # Need to go from ProcessedData df -> build_base_schedule() -> Results df (tblTaskSchedule_London schema)
        # Build base schedule that becomes the object that optimizer iterates on (i.e. iterates b/w blackout and weekly hrs)

        # Current approach limits df obj to <2gb in size, which should be fine????
        # as long as they don't try to forecast for like 100 years in
        # TODO: scalability & space complexity would prefer using df.itertuples method to bypass 2gb df memory limit
        #  but that's a way out there problem and might cost some runtime complexity anyway
        # TODO: df idx column exported under anonymous label, rename to tID and make pKey?

        schedule_list = []
        forecast_end = pd.Timestamp.today() + pd.DateOffset(years=forecast_years)

        for task in clean_df.to_dict('records'):
            tmp = task.copy()
            tmp['Scheduled_Date'] = tmp.pop('ConsolidatedDates')
            tmp['TotalCount'] = 0
            tmp['DeltaWeeks'] = 0
            tmp['TenYearTotal'] = (52*forecast_years) // tmp['TaskSequence_Weeks']       # iteratively calc for accuracy

            while tmp['Scheduled_Date'] < forecast_end:
                tmp['Year'] = tmp['Scheduled_Date'].year
                tmp['Week'] = tmp['Scheduled_Date'].week
                tmp['ScheduledWeek'] = (tmp['Scheduled_Date'] if (weekday := tmp['Scheduled_Date'].weekday()) == 0
                                        else tmp['Scheduled_Date'] - pd.DateOffset(days=weekday))
                tmp['TotalCount'] += 1
                schedule_list.append(tmp.copy())
                tmp['Scheduled_Date'] += pd.DateOffset(weeks=tmp['TaskSequence_Weeks'])

        schedule_df = pd.DataFrame.from_records(schedule_list)
        schedule_df['HardCapped'] = 0

        schedule_df = schedule_df[[
            "Key",
            "DataSource",
            "TaskDescription",
            "TaskSequence",
            "TaskSequence_Weeks",
            "DeltaWeeks",
            "HardCapped",
            "Trade",
            "Hrs",
            "Year",
            "Week",
            "EstimatedLastServiceDate",
            "Scheduled_Date",
            "ScheduledWeek",
            "TenYearTotal",
            "TotalCount"
        ]]

        schedule_df = schedule_df.sort_values(by='Scheduled_Date')

        return schedule_df


    @staticmethod
    def week_helper(week, shift=1):
        week += shift
        if week > 52:
            return 1
        elif week < 1:
            return 52
        else:
            return week