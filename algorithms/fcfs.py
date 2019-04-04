'''
First Come First Serve
'''
from algorithms.algorithm import Algorithm
from job.job import Job

class FCFS(Algorithm):
    
    def get_init_state(self, current_timestamp, df):
        # get queued jobs
        queued_df = df.loc[(df['ctime'] >= current_timestamp) & (df['start'] < current_timestamp)].copy()
        queued_df = queued_df.reset_index()
        for index, row in queued_df.iterrows():
            job_args = {'create_time': row['ctime'],
                        'job_name': row['jobname'],
                        'start_time': None,
                        'total_run_time': row['end'] - row['start'],
                        'required_n_nodes': int(row['nhosts']),
                        'used_nodes': [],
                        'remained_running_time': row['end'] - row['start']}
            self.waiting_queue.append(Job(**job_args))
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

    def extra_settings(self):
        Algorithm.extra_settings(self)
        
    def enqueue_job(self, job):
        job.remained_running_time = job.total_run_time
        self.waiting_queue.append(job) 
    
    def get_next_run_job(self):
        if len(self.waiting_queue) == 0:
            return None
        else:
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
            if o.job_id == job.job_id:
                del self.running_list[i]
                break
        Algorithm.free_nodes(self, job.used_nodes)
        
        