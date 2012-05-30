
import random
import time
import unittest
import os
from openmdao.main.api import Assembly,Component,set_as_top
from openmdao.main.resource import ResourceAllocationManager as RAM
from openmdao.lib.drivers.api import CaseIteratorDriver
from openmdao.lib.casehandlers.api import CaseArray
from openmdao.lib.datatypes.api import Float,Dict,List
from openmdao.lib.casehandlers.api import DumpCaseRecorder,ListCaseRecorder
from mpiallocator.mpiallocator import MPI_Allocator

class Sleeper(Component):
    t = Float(0,iotype="in",units="s")
    tnew = Float(0,iotype="in",units="s")

    resource = List(iotype="in")

    def __init__(self,*args,**kwargs):
        super(Sleeper, self).__init__()
        for k, v in kwargs.iteritems():
            if hasattr(self,k):
                setattr(self,k,v)

    def execute(self):
        time.sleep(self.t)
        print 'dir',self.resource
        name = str(self.name)+str(self.t)+'.dat'
        f = open(name, 'w')
        f.write(str(self.t))
        self.tnew = self.t + 100 


class Sleepers(Assembly):

    
    resource = List(iotype="in")

    def configure(self):
        self.add('CaseIter',CaseIteratorDriver())
        self.CaseIter.sequential = False
        self.CaseIter.extra_resources = {'min_cpus':4,'allocator':'test'} 
        self.add('sleeper',Sleeper())
        self.CaseIter.workflow.add('sleeper') 
        t=[random.randint(1,5) for r in xrange(10)]
        cases={'sleeper.t':t}
        
        self.total_time = sum(cases['sleeper.t'])
        self.CaseIter.iterator = CaseArray(obj=cases)
        self.driver.workflow.add('CaseIter')
        self.CaseIter.case_outputs = ['sleeper.tnew']
        dumpoutfile = open('cases.out', 'w',0)
        self.CaseIter.recorders = [DumpCaseRecorder(dumpoutfile),ListCaseRecorder()]

        self.sleeper.resource = self.resource

    def _pre_execute(self,force=True):
        super(Sleepers, self)._pre_execute(force=force)
        self.sleeper.resource = self.resource


    def _post_run(self):
        for c in self.CaseIter.recorders[1].get_iterator():
            print c
        print 'Total "CPU" time',self.total_time,self._case_id 


class MPIallocatorTestCase(unittest.TestCase):

    def setUp(self):

        nodes = []
        for i in range(12):
            nodes.append('g-0%02d'%i)

        # start the fake MPI_Allocator 
        self.cluster=MPI_Allocator(name='test',machines=nodes)
        # add it to to the RAM
        RAM.add_allocator(self.cluster)
        
    def tearDown(self):
        pass
        
    def test_CaseIter(self): 
        sleep_assembly=set_as_top(Sleepers())
        t0 = time.time()
        sleep_assembly.run()
        
if __name__ == "__main__":
    unittest.main()
