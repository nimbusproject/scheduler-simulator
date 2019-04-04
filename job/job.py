'''
Job
'''
import uuid

class Job:
    def __init__(self, create_time, job_name, total_run_time, required_n_nodes, 
                 start_time = None, used_nodes = [], remained_running_time = None, job_id = None):
        self.create_time = create_time
        self.job_name = job_name
        self.job_id = job_id
        if not job_id: self.job_id = str(uuid.uuid4())
        self.start_time = start_time
        self.total_run_time = total_run_time
        self.required_n_nodes = required_n_nodes
        self.used_nodes = used_nodes
        self.remained_running_time = remained_running_time