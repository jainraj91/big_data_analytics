########################################
## Template Code for Big Data Analytics
## assignment 1 - part I, at Stony Brook Univeristy
## Fall 2017


import sys
from abc import ABCMeta, abstractmethod
from multiprocessing import Process, Manager
from pprint import pprint
import numpy as np
from random import random


##########################################################################
##########################################################################
# PART I. MapReduce

class MyMapReduce:  # [TODO]
    __metaclass__ = ABCMeta

    def __init__(self, data, num_map_tasks=5, num_reduce_tasks=3):  # [DONE]
        self.data = data  # the "file": list of all key value pairs
        self.num_map_tasks = num_map_tasks  # how many processes to spawn as map tasks
        self.num_reduce_tasks = num_reduce_tasks  # " " " as reduce tasks

    ###########################################################
    # programmer methods (to be overridden by inheriting class)

    @abstractmethod
    def map(self, k, v):  # [DONE]
        print("Need to override map")

    @abstractmethod
    def reduce(self, k, vs):  # [DONE]
        print("Need to override reduce")

    ###########################################################
    # System Code: What the map reduce backend handles

    #mapTask will return (partition_number, (key value)) tuple
    def mapTask(self, data_chunk, namenode_m2r):  # [DONE]
        # runs the mappers and assigns each k,v to a reduce task
        for (k, v) in data_chunk:
            # run mappers:
            mapped_kvs = self.map(k, v)
            # assign each kv pair to a reducer task
            for (k, v) in mapped_kvs:
                namenode_m2r.append((self.partitionFunction(k), (k, v)))

    #partition function will return a hash function according to the following logic
    #1. if value is string, take first character ascii and return its mod
    #2. else if value is numeric, simply take its mod and return
    def partitionFunction(self, k):  # [DONE]

        try:
            c = ord(k[0].lower())
        except:
            c = k
        return c % self.num_reduce_tasks
    # given a key returns the reduce task to send it

    # take the values in the form (k,v) and return (k, (v1,v2,v3)
    def reduceTask(self, kvs, namenode_fromR):

        mapToGroup = {}

        for k, v in kvs :
            if k in mapToGroup :                #if the key is already present in map, then merge their arrays
                current_value = mapToGroup[k]
                current_value.append(v)
                mapToGroup[k] = current_value
            else :                               #else we insert the key into the map
                mapToGroup[k] = [v]

        for k in mapToGroup :
            result = (self.reduce(k, mapToGroup[k]))

            if result is not None :
                namenode_fromR.append(result)


    def runSystem(self):  # [TODO]
        # runs the full map-reduce system processes on mrObject

        # the following two lists are shared by all processes
        # in order to simulate the communication
        # [DONE]
        namenode_m2r = Manager().list()  # stores the reducer task assignment and
        # each key-value pair returned from mappers
        # in the form: [(reduce_task_num, (k, v)), ...]
        namenode_fromR = Manager().list()  # stores key-value pairs returned from reducers
        # in the form [(k, v), ...]

        # divide up the data into chunks accord to num_map_tasks, launch a new process
        # for each map task, passing the chunk of data to it.
        # hint: if chunk contains the data going to a given maptask then the following
        #      starts a process
        #      p = Process(target=self.mapTask, args=(chunk,namenode_m2r))
        #      p.start()
        #  (it might be useful to keep the processes in a list)
        processes = {}
        for i in range(self.num_map_tasks) :
            chunk = []
            # divided the whole data into the chunks using following logic
            # if remainder of data number and num_map_task == num_map_task number
            # e.g. chunk[0] will contain data[0],data[3],data[6]... because [0,3,6...] all mod 3(total num_task) gives 0
            for j in range(len(self.data)):
                if j % self.num_map_tasks == i:
                    chunk.append(self.data[j])

            # join map task processes back
            p = Process(target=self.mapTask, args=(chunk, namenode_m2r))
            processes[i] = p
            processes.get(i).start()
            processes.get(i).join()

        # print output from map tasks
        print("namenode_m2r after map tasks complete:")
        pprint(sorted(list(namenode_m2r)))

        # launch the reduce tasks as a new process for each.
        to_reduce_task = [[] for i in range(self.num_reduce_tasks)]
        reduce_processes = {}

        #making different reduce tasks
        for k, v in namenode_m2r :
            to_reduce_task[k].append(v)

        # "send" each key-value pair to its assigned reducer by placing each
        # into a list of lists, where to_reduce_task[task_num] = [list of kv pairs]
        for i in range(self.num_reduce_tasks):
            p = Process(target=self.reduceTask, args=(to_reduce_task[i], namenode_fromR))
            reduce_processes[i] = p
            reduce_processes.get(i).start()
            reduce_processes.get(i).join()

        # print output from reducer tasks
        print("namenode_m2r after reduce tasks complete:")
        pprint(sorted(list(namenode_fromR)))

        return namenode_fromR


##########################################################################
##########################################################################
##Map Reducers:

class WordCountMR(MyMapReduce):  # [DONE]
    # the mapper and reducer for word count
    def map(self, k, v):  # [DONE]
        counts = dict()
        for w in v.split():
            w = w.lower()  # makes this case-insensitive
            try:  # try/except KeyError is just a faster way to check if w is in counts:
                counts[w] += 1
            except KeyError:
                counts[w] = 1
        return counts.items()

    def reduce(self, k, vs):  # [DONE]
        return (k, np.sum(vs))


class SetDifferenceMR(MyMapReduce):
    # contains the map and reduce function for set difference
    # Assume that the mapper receives the "set" as a list of any primitives or comparable objects

    # map function will convert (key value) into (value, key) e.g (R, {apple,blueberry}) -> [(apple->R),(blueberry->R)
    def map(self, k, v):  # [DONE]
        counts = dict()

        for value in v:     #loop over all the values in a list corresponding to a key k
            counts[value] = k

        return counts.items()

    # Reduce function will work in following way
    # 1. Reduce function will get a key value in the form of (key, [v1,v2,v3,v4...]) e.g. (apple, [R,R,R,S,S])
    # 2. This function will then return difference of count of R and S corresponding to each Key

    def reduce(self, k, vs):

        no_of_r = 0   #to keep track of number of R corresponding to each key
        no_of_s = 0   #to keep track of number of R corresponding to each key

        for v in vs :
            if v == 'R' :
                no_of_r = no_of_r + 1
            elif v == 'S' :
                no_of_s += 1

        if no_of_r - no_of_s > 0 : #if the difference of r and s is  positive, return difference
            # diff = no_of_r - no_of_s
            return k
        else :          #else return none
            return None


##########################################################################
##########################################################################


if __name__ == "__main__":  # [DONE: Uncomment peices to test]
    ###################
    ##run WordCount:
    data = [(1, "The horse raced past the barn fell"),
            (2, "The complex houses married and single soldiers and their families"),
            (3, "There is nothing either good or bad, but thinking makes it so"),
            (4, "I burn, I pine, I perish"),
            (5, "Come what come may, time and the hour runs through the roughest day"),
            (6, "Be a yardstick of quality."),
            (7, "A horse is the projection of peoples' dreams about themselves - strong, powerful, beautiful"),
            (8,
             "I believe that at the end of the century the use of words and general educated opinion will have altered so much that one will be able to speak of machines thinking without expecting to be contradicted."),
            (9, "The car raced past the finish line just in time."),
            (10, "Car engines purred and the tires burned.")]
    mrObject = WordCountMR(data, 4, 3)
    mrObject.runSystem()

    ####################
    ##run SetDifference
    # (TODO: uncomment when ready to test)
    print("\n\n*****************\n Set Difference\n*****************\n")
    data1 = [('R', ['apple', 'orange', 'pear', 'blueberry']),
		 ('S', ['pear', 'orange', 'strawberry', 'fig', 'tangerine'])]
    data2 = [('R', [x for x in range(50) if random() > 0.5]),
		    ('S', [x for x in range(50) if random() > 0.75])]
    mrObject = SetDifferenceMR(data1, 2, 2)
    mrObject.runSystem()
    mrObject = SetDifferenceMR(data2, 2, 2)
    mrObject.runSystem()

