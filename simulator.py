'''
HPC scheduler simulator
'''
import argparse
import datetime
import pandas as pd
import sys

from algorithms.algorithm_factory import AlgorithmFactory

def purify_input(logs_df):
    logs_df = logs_df.loc[(logs_df['ctime'] <= logs_df['start']) & (logs_df['start'] <= logs_df['end'])].reset_index()
    logs_df = logs_df.drop_duplicates()
    
    return logs_df

def run(algorithm, start, end):
    record_df = pd.DataFrame(columns=['jobid','ctime', 'start','end','nhosts'])
    current_time = start
    next_enqueue_job = algorithm.get_next_enqueue_job()
    next_run_job = algorithm.get_next_run_job()
    next_terminate_job_id = algorithm.get_next_terminate_job_id()
    while current_time <= end and next_enqueue_job:
        if next_run_job and algorithm.is_available_nodes_for_job(next_run_job):
            # start job
            algorithm.start_job(next_run_job, current_time)
            if not next_terminate_job_id: next_terminate_job_id = algorithm.get_next_terminate_job_id()
            next_run_job = algorithm.get_next_run_job()
        else:
            if not next_terminate_job_id or next_enqueue_job.create_time <= (current_time + algorithm.find_run_job_by_id(next_terminate_job_id).remained_running_time):
                # enqueue job
                algorithm.adjust_remained_running_time(next_enqueue_job.create_time - current_time)
                current_time = next_enqueue_job.create_time
                algorithm.enqueue_job(next_enqueue_job)
                next_enqueue_job = algorithm.get_next_enqueue_job()
                if not next_run_job: next_run_job = algorithm.get_next_run_job()
            else:
                # terminate job
                next_terminate_job = algorithm.find_run_job_by_id(next_terminate_job_id)
                algorithm.terminate_job(next_terminate_job)
                algorithm.adjust_remained_running_time(next_terminate_job.remained_running_time)
                current_time = current_time + next_terminate_job.remained_running_time
                record_df = record_df.append({'jobid': next_terminate_job.job_id,
                                              'jobname': next_terminate_job.job_name,
                                              'ctime': next_terminate_job.create_time,
                                              'start': next_terminate_job.start_time,
                                              'end': current_time,
                                              'nhosts': len(next_terminate_job.used_nodes)}, ignore_index=True)
        next_terminate_job_id = algorithm.get_next_terminate_job_id() 
            
                
    return record_df

def check_input_file_format(df):
    missing_columns = []
    if 'jobname' not in df.columns: missing_columns.append('jobname')
    if 'ctime' not in df.columns: missing_columns.append('ctime')
    if 'start' not in df.columns: missing_columns.append('start')
    if 'end' not in df.columns: missing_columns.append('end')
    if 'nhosts' not in df.columns: missing_columns.append('nhosts')
    
    if len(missing_columns) > 0:
        raise ValueError('Missing columns {}'.format(','.join(missing_columns)))
   
def main(argv):
    parser = argparse.ArgumentParser(description=__doc__)

    parser.add_argument('--nnodes', type=int, help='number of nodes', required=True)
    parser.add_argument('--input', type=str, help='input log file', required=True)
    parser.add_argument('--start', type=str, help='simulator start time [yyyy-mm-dd HH:MM:SS]', required=True)
    parser.add_argument('--end', type=str, help='simulator end time[yyyy-mm-dd HH:MM:SS]', required=True)
    parser.add_argument('--algorithm', type=str, choices=['fcfs', 'rr'], help='schedular algorithm', default='fcfs')
    parser.add_argument('--result-file', type=str, help='result file location and name', required=True)
    parser.add_argument('--algorithm-extra', type=str, help='algorithm specific parameters')
    
    args = parser.parse_args(argv[1:])
    
    # initialize node pool
    node_pool = {}
    for i in range(args.nnodes):
        node_pool['b{}'.format(str(i))] = 0
    
    # read logs as dataframe
    logs_df = pd.read_csv(args.input)
    check_input_file_format(logs_df)
    logs_df = purify_input(logs_df)
    
    epoch_start = int(datetime.datetime.strptime(args.start, '%Y-%m-%d %H:%M:%S').strftime('%s'))
    epoch_end = int(datetime.datetime.strptime(args.end, '%Y-%m-%d %H:%M:%S').strftime('%s'))
    algorithm = AlgorithmFactory.get_algorithm(args.algorithm, args.nnodes, logs_df, epoch_start, epoch_end)
    if args.algorithm_extra:
        items = args.algorithm_extra.split(',')
        params = {}
        for i in items:
            content = i.split('=')
            params[content[0]] = content[1]
        algorithm.extra_settings(**params)
    algorithm.get_init_state(epoch_start, logs_df)
    
    result = run(algorithm, epoch_start, epoch_end)
    result.to_csv(args.result_file, index=False)
        
if __name__ == '__main__':
    sys.exit(main(sys.argv))