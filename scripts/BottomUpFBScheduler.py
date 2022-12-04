import pandas as pd
import heapq
import datetime
from collections import defaultdict
from .AbstractScheduleAlgorithm import AbstractScheduleAlgorithm


class BottomUpFBScheduler:
    def create_schedule(self, clean_df, wm_df, forecast_years, hardcap={}):
        """
        Idea: treat scheduling as a constraint satisfaction problem and use 
        a backtracking approach to complete the schedule. Assuming no unresolvable conflicts
        (which shouldn't usually occur given the sparsity of tasks), a schedule should always be possible

        Use a greedy approach to propose weeks for each task. 
        
        Benefits of this approach: 
        - May be more easily extendable to more types of constraints (simply need to define a check)
        Drawbacks of this approach:
        - Logic is more convoluted
        - May involve more complex programming structures/concepts

        Meta Algorithm for backward only:
            1. Extract every task in the dataset and compute an associated priority score
            2. Create a hash map containing each task and use the task key as an index. Create an empty schedule which will use iso-standard weeks as indices.
            3. Create a min heap with each task, sorted by the iso-standard week, then priority score, and finally task index.
            4. While the heap is not empty:
            5.      Extract the task associated with the min of the heap (which will be the task with the earliest week and lowest priority score).
            6.      If this this task can be added to the schedule for the given ScheduledWeek:
            7.          Insert a copy of the task into the schedule and create the next occurrence of the task, add back to the heap if it occurs within the forecasted end date.
            8.      else:
            9.          Adjust the ScheduledWeek of the task and increment DeltaWeeks by one.
            10.         Insert the delayed task back into the heap with an updated priority score. 

        Parameters:
        - clean_df: pd.DataFrame. Cleaned input data loaded to Dataframe form.
        - weeks_master: dict[start-of-week-date -> int]. Weeks Master data representing the max number of hours allowed per week.
        - forecast_years: int. Default=10. Adjusts the number of years to forecast into the future.
        """
        weeks_master = wm_df.set_index(["ScheduledWeek"])

        # TODO: Refactor to remove code duplication.
        schedule = defaultdict(list)
        forecast_end = pd.Timestamp.today() + pd.DateOffset(years=forecast_years)

        # Setup heap and map to tasks
        task_heap = []
        task_map = dict()
        for task in clean_df.to_dict('records'):
            tmp = task.copy()
            tmp['ScheduledWeek'] = tmp.pop('ConsolidatedDates')
            tmp['ScheduledWeek'] = tmp['ScheduledWeek'] - pd.DateOffset(days=tmp['ScheduledWeek'].weekday())
            tmp['TotalCount'] = 0
            tmp['DeltaWeeks'] = 0
            priority_score = AbstractScheduleAlgorithm.compute_priority_score(tmp, hardcap)
            iso_week = AbstractScheduleAlgorithm.convert_date_to_iso(tmp['ScheduledWeek'])
            key = tmp["Key"]

            heapq.heappush(task_heap, [iso_week, priority_score, key])
            task_map[key] = tmp
        
        # Generate schedule
        while task_heap:
            date_index, priority_score, key = heapq.heappop(task_heap)
            new_task = task_map[key]

            # Need to make sure new task does not violate constraints
            if AbstractScheduleAlgorithm.is_constraints_satisfied(weeks_master, date_index, schedule[date_index], 
                new_task["Hrs"], hard_capped=bool(priority_score==-1), task_freq=new_task["TaskSequence_Weeks"]):
                # Add the task to the schedule
                new_task["TotalCount"] += 1
                schedule[date_index].append(new_task.copy())

                # Compute next scheduled time, where the delta weeks shift is reset
                next_date = datetime.datetime.strptime(date_index, "%Y-%m-%d") \
                    + pd.DateOffset(weeks=new_task["TaskSequence_Weeks"]) \
                    - pd.DateOffset(weeks=new_task["DeltaWeeks"])

                # Insert task back into heap if there are still more occurences
                if next_date < forecast_end:
                    new_task["ScheduledWeek"] = next_date
                    new_task['DeltaWeeks'] = 0

                    priority_score = AbstractScheduleAlgorithm.compute_priority_score(new_task, hardcap)
                    iso_week = AbstractScheduleAlgorithm.convert_date_to_iso(new_task["ScheduledWeek"])
                    key = new_task["Key"]

                    heapq.heappush(task_heap, [iso_week, priority_score, key])
                        
            # Move task if constraints are violated 
            else: # Look backward: place the task if there is a free space.
                # Check the date that is $DeltaWeeks + 1 backward from the original date
                # E.g. when the task conflicts for the first time, $DeltaWeeks = 0 therefore we check -1 week from the original date, etc.
                past_date = datetime.datetime.strptime(date_index, "%Y-%m-%d") - pd.DateOffset(weeks=2*new_task["DeltaWeeks"] + 1)
                past_date = AbstractScheduleAlgorithm.convert_date_to_iso(past_date)
                if AbstractScheduleAlgorithm.is_constraints_satisfied(weeks_master, date_index, schedule[date_index], 
                    new_task["Hrs"], hard_capped=bool(priority_score==-1), task_freq=new_task["TaskSequence_Weeks"]):
                    # Add the task to the schedule
                    new_task["TotalCount"] += 1
                    new_task["ScheduledWeek"] = past_date
                    new_task["DeltaWeeks"] = -(new_task["DeltaWeeks"] + 1)

                    schedule[past_date].append(new_task.copy())

                    # Compute next scheduled time, where the delta weeks shift is reset
                    next_date = datetime.datetime.strptime(date_index, "%Y-%m-%d") \
                        + pd.DateOffset(weeks=new_task["TaskSequence_Weeks"]) \
                        - pd.DateOffset(weeks=new_task["DeltaWeeks"])

                    # Insert task back into heap if there are still more occurences
                    if next_date < forecast_end:
                        new_task["ScheduledWeek"] = next_date
                        new_task['DeltaWeeks'] = 0

                        priority_score = AbstractScheduleAlgorithm.compute_priority_score(new_task, hardcap)
                        iso_week = AbstractScheduleAlgorithm.convert_date_to_iso(new_task["ScheduledWeek"])
                        key = new_task["Key"]

                        heapq.heappush(task_heap, [iso_week, priority_score, key])

                else: # Look forward: increment ScheduledWeek by one week 
                    new_date = datetime.datetime.strptime(date_index, "%Y-%m-%d") + pd.DateOffset(weeks=1)
                    new_task["ScheduledWeek"] = new_date
                    new_task["DeltaWeeks"] += 1
                    
                    # Recompute priority score with delta weeks adjusted and re-insert into heap
                    priority_score = AbstractScheduleAlgorithm.compute_priority_score(new_task, hardcap)
                    iso_week = AbstractScheduleAlgorithm.convert_date_to_iso(new_task["ScheduledWeek"])
                    key = new_task["Key"]

                    heapq.heappush(task_heap, [iso_week, priority_score, key])

        # Convert schedule to dataframe
        schedule_list = [task for task_list in schedule.values() for task in task_list]
        schedule_df = pd.DataFrame.from_records(schedule_list)

        schedule_df = schedule_df[[
            "Key", "DataSource", "TaskDescription", "TaskSequence", "TaskSequence_Weeks", "Trade", "Hrs", "Year", "Week",
            "EstimatedLastServiceDate", "ScheduledWeek", "TotalCount", "DeltaWeeks"
        ]]
        schedule_df = schedule_df.sort_values(by='ScheduledWeek')
        AbstractScheduleAlgorithm.check_valid_schedule(schedule_df, wm_df, type(self).__name__)
        AbstractScheduleAlgorithm.check_complete_task_list(clean_df, schedule_df, type(self).__name__)
        return schedule_df
