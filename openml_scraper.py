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
    current_scores = []
    current_runs = []
    current_flows = []
    j = 1
    k = 1
    run_keys = []
    run_values = []

    run = oml.runs.get_run(input_id)
    run_evals = run.evaluations
    flow_params = run.parameter_settings

    current_runs.append(
        {"run info": run, "run id": run.run_id, "flow name": run.flow_name,
         "flow id": run.flow_id, "task type": run.task_type, "task id": run.task_id,
         "setup id": run.setup_id, "dataset_id": run.dataset_id})

    for measure, value in run_evals.items():
        current_scores.append({measure: value})

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

    flow_dict = dict(zip(run_keys, run_values))
    for key in flow_dict:
        current_flows.append({key: flow_dict.get(key)})

    results = [current_runs, current_flows, current_scores]
    return results


def gen_file_name(run_num):
    """Generate file name and path based off current run id."""
    name_split = [str(run_num), 'xlsx']
    name = '.'.join(name_split)
    sheet_path_split = ['.', name]
    sheet_path = '/'.join(sheet_path_split)
    results = [name, sheet_path]
    return results


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
    run_lables = []
    for pair in run_dict:
        run_lables.append(pair)

    run_table.columns = run_lables

    return run_table


def gen_flow_table(flow_info):
    """Generate table of flow info."""
    flow_table = pd.DataFrame(flow_info)

    return flow_table


def gen_score_table(score_info):
    """Generate table of score info."""
    score_table = pd.DataFrame(score_info)

    return score_table


def gen_full_run_file(run_id):
    """Execute all other functions to form excel sheet."""
    try:
        output = generate_tables(run_id)
        runs = output[0]
        flows = output[1]
        scores = output[2]

        current_run_table = gen_run_table(runs)
        current_flow_table = gen_flow_table(flows)
        current_score_table = gen_score_table(scores)

        # list of dataframes
        dfs = [current_run_table, current_flow_table, current_score_table]

        file_names = gen_file_name(run_id)
        # current_name = file_names[0]
        current_path = file_names[1]

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
