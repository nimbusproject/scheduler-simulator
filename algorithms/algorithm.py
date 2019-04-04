'''
Parent algorithm class
'''
import logging
import Queue

from abc import abstractmethod
from job.job import Job

class Algorithm:
    
    def __init__(self, num_nodes, log_df, epoch_start, epoch_end):
        self.log_queue = Queue.Queue()
        filtered_df = log_df.loc[(log_df['ctime'] >= epoch_start) & (log_df['ctime'] < epoch_end)].copy()
        filtered_df = filtered_df.reset_index().sort_values('ctime')
        for index, row in filtered_df.iterrows():
            job_args = {'create_time': row['ctime'],
                        'job_name': row['jobname'],
                        'start_time': None,
                        'total_run_time': row['end'] - row['start'],
                        'required_n_nodes': int(row['nhosts']),
                        'used_nodes': [],
                        'remained_running_time': row['end'] - row['start']}
            self.log_queue.put(Job(**job_args))
        self.waiting_queue = []
        self.running_list = []
        self.node_pool = {}
        for i in range(num_nodes):
            self.node_pool['b{}'.format(str(i))] = 0
        
    def get_available_nodes(self):
        return [key for key in self.node_pool.keys() if self.node_pool[key] == 0]   
    
    def free_nodes(self, nodes):
        for node in nodes:
            if self.node_pool[node] == 0:
                logging.error('Freeing node {}, but it is not in use.'.format(node))
            self.node_pool[node] = 0
    
    def use_nodes(self, nodes):
        for node in nodes:
            if self.node_pool[node] == 1:
                logging.error('Grabbing node {}, but it is already in use.'.format(node))
            self.node_pool[node] = 1 
    
    def is_available_nodes_for_job(self, job):
        return len(self.get_available_nodes()) >= job.required_n_nodes
    
    def adjust_remained_running_time(self, adjust_time):
        for idx, job in enumerate(self.running_list):
            job.remained_running_time = job.remained_running_time - adjust_time
            self.running_list[idx] = job 
            
    def find_run_job_by_id(self, job_id):
        job = None
        for i, o in enumerate(self.running_list):
            if o.job_id == job_id:
                job = o
                break
        return job
    
    @abstractmethod
    def extra_settings(self):
        pass
        
    @abstractmethod
    def get_init_state(self):
        pass
    
    @abstractmethod
    def enqueue_job(self):
        pass
    
    @abstractmethod
    def get_next_run_job(self):
        pass
    
    @abstractmethod
    def get_next_enqueue_job(self):
        pass
    
    @abstractmethod
    def get_next_terminate_job_id(self):
        pass
    
    @abstractmethod
    def start_job(self):
        pass
    
    @abstractmethod
    def terminate_job(self):
        pass
