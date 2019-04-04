'''
Round Robin
'''
import uuid
import copy

from algorithms.algorithm import Algorithm
from job.job import Job

class RR(Algorithm):
    
    def get_init_state(self, current_timestamp, df):
        # get queued jobs
        queued_df = df.loc[(df['ctime'] >= current_timestamp) & (df['start'] < current_timestamp)].copy()
        queued_df = queued_df.reset_index()
        tmp_queue_list = []
        for index, row in queued_df.iterrows():
            job_id = str(uuid.uuid4())
            job_args = {'create_time': row['ctime'],
                        'job_name': row['jobname'],
                        'start_time': None,
                        'total_run_time': row['end'] - row['start'],
                        'required_n_nodes': int(row['nhosts']),
                        'used_nodes': [],
                        'remained_running_time': None,
                        'job_id': job_id}
            num_full_slices = int(row['end'] - row['start']) / int(self.quantum)
            last_slice = int(row['end'] - row['start']) % int(self.quantum)
            
            tmp_list = []
            job_args['remained_running_time'] = self.quantum
            for i in range(num_full_slices):
                tmp_list.append(Job(**job_args))
            if num_full_slices == 0 or last_slice > 0:
                job_args['remained_running_time'] = last_slice
                tmp_list.append(Job(**job_args))
            tmp_queue_list.append(tmp_list)
        
        if len(tmp_queue_list) > 0:
            for i in range(max(tmp_queue_list, key=len)):
                for j in tmp_queue_list:   
                    if i < len(tmp_queue_list[j]):
                        self.waiting_queue.append(tmp_queue_list[j][i])
        else:
            self.waiting_queue = []
        
        # get running jobs
        run_df = df.loc[(df['start'] >= current_timestamp) & (df['end'] < current_timestamp)].copy()
        run_df = run_df.reset_index()
        for index, row in run_df.iterrows():
            job_args = {'create_time': row['ctime'],
                        'job_name': row['jobname'],
                        'start_time': row['start'],
                        'total_run_time': row['end'] - row['start'],
                        'required_n_nodes': int(row['nhosts']),
                        'used_nodes': [],
                        'remained_running_time': row['end'] - current_timestamp}
            available_nodes = Algorithm.get_available_nodes(self)
            if len(available_nodes) > job_args['required_n_nodes']:
                job_args['used_nodes'] = available_nodes[0:job_args['required_n_nodes']]
                self.running_list.append(Job(**job_args))    
                Algorithm.use_nodes(self, job_args['used_nodes'])

    def extra_settings(self, quantum):
        self.quantum = int(quantum)
        
    def enqueue_job(self, job):
        num_full_slices = int(job.total_run_time) / int(self.quantum)
        last_slice = int(job.total_run_time) % int(self.quantum)
        tmp_list = []
        for i in range(num_full_slices):
            tmp_job = copy.copy(job)
            tmp_job.remained_running_time = self.quantum
            tmp_list.append(tmp_job)
        if num_full_slices == 0 or last_slice > 0:
            tmp_job = copy.copy(job)
            tmp_job.remained_running_time = last_slice
            tmp_list.append(tmp_job)
                
        existing_job_id = set()
        new_waiting_queue = []
        for j in self.waiting_queue:
            if j.job_id in existing_job_id:
                if len(tmp_list) > 0:
                    new_waiting_queue.append(tmp_list.pop(0))
                existing_job_id = set()
            existing_job_id.add(j.job_id)
            new_waiting_queue.append(j)
        if len(tmp_list) > 0:
            new_waiting_queue = new_waiting_queue + tmp_list
        
        self.waiting_queue = new_waiting_queue
        
    
    def get_next_run_job(self):
        if len(self.waiting_queue) == 0:
            return None
        else:
            # job must be run in order
            peek_next_run_job = self.waiting_queue[0]
            for job in self.running_list:
                if job.job_id == peek_next_run_job.job_id:
                    return None
            return self.waiting_queue.pop(0)
    
    def get_next_enqueue_job(self):
        if self.log_queue.empty():
            return None
        else:
            return self.log_queue.get()
        
    def get_next_terminate_job_id(self):
        if len(self.running_list) == 0:
            return None
        else:
            next_terminate_job = min(self.running_list, key=lambda x: x.remained_running_time)
            return next_terminate_job.job_id
    
    def start_job(self, job, start_time):
        available_nodes = Algorithm.get_available_nodes(self)
        job.used_nodes = available_nodes[0:job.required_n_nodes]
        job.start_time = start_time
        Algorithm.use_nodes(self, job.used_nodes)
        self.running_list.append(job)
        
    def terminate_job(self, job):
        for i, o in enumerate(self.running_list):
            if o.job_id == job.job_id and o.create_time == job.create_time:
                del self.running_list[i]
                break
        Algorithm.free_nodes(self, job.used_nodes)
        
        