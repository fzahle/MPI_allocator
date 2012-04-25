
import random
import time
import unittest
import os
from openmdao.main.api import Assembly,Component,set_as_top
from openmdao.main.resource import ResourceAllocationManager as RAM
from openmdao.lib.drivers.api import CaseIteratorDriver
from openmdao.lib.casehandlers.api import CaseArray
from openmdao.lib.datatypes.api import Float
from openmdao.lib.casehandlers.api import DBCaseRecorder,ListCaseRecorder
from mpiallocator.mpiallocator import MPI_Allocator

class Sleeper(Component):
    t = Float(0,iotype="in",units="s")
    tnew = Float(0,iotype="in",units="s")

    def __init__(self,*args,**kwargs):
        super(Sleeper, self).__init__()
        for k, v in kwargs.iteritems():
            if hasattr(self,k):
                setattr(self,k,v)

        super(Sleeper, self).__init__()

    def execute(self):
        time.sleep(self.t)
        name = str(self.name)+str(self.t)+'.dat'
        f = open(name, 'w')
        f.write(str(self.t))
        self.tnew = self.t + 100 


class Sleepers(Assembly):

    def configure(self):
        self.add('CaseIter',CaseIteratorDriver())
        self.CaseIter.sequential = False
        self.CaseIter.extra_resources = {'min_cpus':4,'allocator':'test'} 
        self.add('sleeper',Sleeper())
        self.CaseIter.workflow.add('sleeper') 
        # can't seem to add sleeper.t to cases unless I make a passthrough...
        self.create_passthrough('sleeper.t')
        cases = dict(t=[random.randint(0,5) for r in xrange(10)])
        self.CaseIter.iterator = CaseArray(obj=cases)
        self.driver.workflow.add('CaseIter')
        # why doesn't this show up in the recorder's output?
        self.CaseIter.case_outputs = ['sleeper.tnew']
        self.CaseIter.recorders = [ListCaseRecorder()]

    def _post_run(self):
        for c in self.CaseIter.recorders[0].get_iterator():
            print c


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
