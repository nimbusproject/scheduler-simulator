'''
Algorithm Factory

Select which parser class to use
'''
from fcfs import FCFS
from rr import RR

class AlgorithmFactory:
    
    @staticmethod
    def get_algorithm(algorithm_name, num_nodes, log_df, epoch_start, epoch_end):
        if algorithm_name == 'fcfs':
            return FCFS(num_nodes, log_df, epoch_start, epoch_end)
        elif algorithm_name == 'rr':
            return RR(num_nodes, log_df, epoch_start, epoch_end)
        else:
            raise RuntimeError("Unknown algorithm {}!".format(algorithm_name))
