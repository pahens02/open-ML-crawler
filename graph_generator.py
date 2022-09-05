#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Jul 16 17:52:08 2022.

@author: paigehensley
"""

import concurrent.futures
import re
import openml as oml

oml.config.apikey = 'YOUR API KEY HERE'

# Functions


def generate_flow_name(input_id):
    """Gather flow name."""
    run = oml.runs.get_run(input_id)

    current_flow_name = {"flow name": run.flow_name}

    results = current_flow_name
    return results


def pipeline_splitter(input_pipeline):
    """Split pipeline into commands."""
    pipeline_keys = []
    pipeline_values = []
    variable = []
    pipeline = input_pipeline.get('flow name')
    org_commands = pipeline[pipeline.find("(")+1:pipeline.rfind(")")]

    split_commands = re.split('[), , ( ]', org_commands)

    while "" in split_commands:
        split_commands.remove("")

    for command in split_commands:
        holder = command
        key_value_list = holder.split('=')
        if len(key_value_list) > 1:
            pipeline_keys.append(key_value_list[0])
            pipeline_values.append(key_value_list[1])
        else:
            variable.append(key_value_list[0])

    return pipeline_keys, pipeline_values, variable


def gen_graphs(run_id, nth_graph):
    """Generate gspan format graphs from pipeline commands."""
    graph_num = nth_graph
    vertex = 0
    weight = 1
    executions = 0
    full_graph = []
    graph_vertexes = []
    graph_edges = []

    flow_name = generate_flow_name(run_id)
    flow_components = pipeline_splitter(flow_name)
    flow_keys = flow_components[0]
    flow_values = flow_components[1]
    flow_variables = flow_components[2]

    graph_line_1 = ' '.join(['t #', str(graph_num)])
    graph_lable = graph_line_1
    while vertex < int(len(flow_keys)):
        if executions < 2:
            graph_line_2 = ' '.join(['v', flow_keys[vertex], flow_values[vertex]])
            graph_vertexes.append(graph_line_2)
            vertex += 1
            executions += 1
        if executions == 2 or vertex != 0:
            graph_line_3 = ' '.join(['e', flow_keys[vertex-2],
                                     flow_keys[vertex-1], str(weight)])
            executions = 0
            graph_edges.append(graph_line_3)

    if len(flow_variables) != 0:
        graph_line_2 = ' '.join(['v', flow_variables[0], 'variable'])
        graph_vertexes.append(graph_line_2)
        vertex += 1
        executions += 1

        graph_line_3 = ' '.join(['e', flow_keys[(len(flow_keys)-1)],
                                 flow_variables[0], str(weight)])
        executions = 0
        graph_edges.append(graph_line_3)

    full_graph.append(graph_lable)
    full_graph.append(graph_vertexes)
    full_graph.append(graph_edges)

    return full_graph


def gen_file_name(size, run_num):
    """Generate file name based off dataset size and ending run ID."""
    name_join = '_'.join(['dataset_of', str(size), 'ending_at', str(run_num)])
    add_type = '.'.join([name_join, 'txt'])

    return add_type


def write_to_txt(dataset, file_name):
    """Write graphs to txt file."""
    line_count = 0
    start = True
    with open(file_name, 'w') as f:
        for item in dataset:
            line_count = 0
            for line in item:
                if line_count == 0:
                    if start is not True:
                        f.write('\n')
                        start = False
                    f.write(line)
                    f.write('\n')
                    line_count += 1
                else:
                    for command in line:
                        f.write(command)
                        f.write('\n')
                        f.write('\n')
                        line_count += 1


def gen_full_dataset(run_id, iteration):
    """Execute all other functions to form dataset."""
    current_graph = gen_graphs(run_id, iteration)
    graphs.append(current_graph)


def gen_file(iteration, dataset, run_num):
    """Execute all functions to generate txt file."""
    name = gen_file_name(iteration-1, run_num)
    write_to_txt(dataset, name)


# Starter run id and number of runs to find
CURRENT_ID = 10561788
NUM_RUNS = 10

CURRENT_ITERATION = 1
futures = []
graphs = []
i = 0


with concurrent.futures.ThreadPoolExecutor(max_workers=100) as executor:
    while i < NUM_RUNS:
        current_future = executor.submit(gen_full_dataset, CURRENT_ID, CURRENT_ITERATION)
        futures.append(current_future)

        CURRENT_ID -= 100
        i += 1
        CURRENT_ITERATION += 1

concurrent.futures.wait(futures)

gen_file(CURRENT_ITERATION, graphs, CURRENT_ID)
print('Completed, ended at run id', CURRENT_ID)
