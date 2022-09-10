#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jun 29 12:04:28 2022.

@author: paigehensley
"""

# Required libraries
import concurrent.futures
import openml as oml
import pandas as pd

# Openml.org API key
oml.config.apikey = 'YOUR API KEY HERE'

# Functions


def generate_tables(input_id):
    """Gather run info."""
    run_keys = []
    run_values = []

    run = oml.runs.get_run(input_id)
    run_evals = run.evaluations
    flow_params = run.parameter_settings

    current_runs = [
        {"run info": run, "run id": run.run_id, "flow name": run.flow_name,
         "flow id": run.flow_id, "task type": run.task_type, "task id": run.task_id,
         "setup id": run.setup_id, "dataset_id": run.dataset_id}]

    current_scores = [{measure: value} for measure, value in run_evals.items()]

    j = 1
    k = 1
    for dic in flow_params:
        for key in dic:
            if j % 3 != 0:
                if k == 1:
                    run_keys.append(dic.get(key))
                    k = 2
                elif k == 2:
                    run_values.append(dic.get(key))
                    k = 1
                    j += 1
                else:
                    j += 1

    current_flows = [{key: value} for key, value in zip(run_keys, run_values)]

    results = [current_runs, current_flows, current_scores]
    return results


def gen_file_name(run_num):
    """Generate file name and path based off current run id."""
    name_split = [str(run_num), 'xlsx']
    name = '.'.join(name_split)
    sheet_path_split = ['.', name]
    sheet_path = '/'.join(sheet_path_split)

    return sheet_path


def multiple_dfs(df_list, sheets, spaces, file_path):
    """Write dataframes to xlsx file."""
    writer = pd.ExcelWriter(file_path, engine='xlsxwriter',)
    row = 0
    for dataframe in df_list:
        dataframe.to_excel(writer, sheet_name=sheets, startrow=row, startcol=-1)
        row = row + len(dataframe.index) + spaces
    writer.save()


def gen_run_table(run_info):
    """Generate dataframe of run info."""
    run_table = pd.DataFrame(run_info)
    run_dict = run_info[0]

    run_labels = [pair for pair in run_dict]

    run_table.columns = run_labels

    return run_table


def gen_flow_table(flow_info):
    """Generate table of flow info."""
    return pd.DataFrame(flow_info)


def gen_score_table(score_info):
    """Generate table of score info."""
    return pd.DataFrame(score_info)


def gen_full_run_file(run_id):
    """Execute all other functions to form excel sheet."""
    try:
        runs, flows, scores = generate_tables(run_id)

        current_run_table = gen_run_table(runs)
        current_flow_table = gen_flow_table(flows)
        current_score_table = gen_score_table(scores)

        # list of dataframes
        dfs = [current_run_table, current_flow_table, current_score_table]

        current_path = gen_file_name(run_id)

        # run sheet generation function
        multiple_dfs(dfs, 'Validation', 1, current_path)
    except:
        print('File not found: ', run_id)


# Starter run id and number of runs to find
CURRENT_ID = 10586788
NUM_RUNS = 1

futures = []
i = 0


with concurrent.futures.ThreadPoolExecutor(max_workers=50) as executor:
    while i <= NUM_RUNS:
        current_future = executor.submit(gen_full_run_file, CURRENT_ID)
        futures.append(current_future)

        CURRENT_ID -= 1
        i += 1

concurrent.futures.wait(futures)
print('Completed, ended at run id ', CURRENT_ID)
