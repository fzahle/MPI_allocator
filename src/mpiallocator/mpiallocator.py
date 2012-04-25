__all__ = ['MPI_allocator']

import random
import time
import os
import socket
import sys
import threading
import time
import traceback
import commands
import logging

from openmdao.main.mp_support import OpenMDAO_Manager, register
from openmdao.main.resource import FactoryAllocator, \
                                   HOME_DIRECTORY, WORKING_DIRECTORY
from openmdao.main.rbac import get_credentials, set_credentials, rbac


class MPI_Allocator(FactoryAllocator):
    """
    A resource allocator for jobs running in an MPI environment 
    such as Torque or PBS.
    """

    _MPI = True 

    def __init__(self,name='MPI_Allocator',machines=None, accounting_id='no-default-set',
                 authkey=None, allow_shell=True):
        super(MPI_Allocator, self).__init__(name, authkey, allow_shell)

        self.accounting_id = accounting_id
        self.machines = machines

        command = 'echo $PBS_VERSION'
        PBS_VERSION=commands.getoutput(command)

        self.job_id = 0

        if not 'TORQUE' in PBS_VERSION:
            print 'Warning: This is not an mpi environment'
            # really only used for testing
            MPI_Allocator._MPI = False


        # get job information from PBS environment variables
        if machines is None:
            self.machines = []
            self.machines=commands.getoutput('cat $PBS_NODEFILE').split('\n')

        self.n_cpus = 0
        self.workers=[]
        for host in self.machines:
            print 'appending node',host,'to allocator'
            self.workers.append({'hostname':host,'state':1}) 
            self.n_cpus+=1
        self.max_cpus = len(self.workers)

    def configure(self, cfg):
        """
        Configure allocator from :class:`ConfigParser` instance.
        Normally only called during manager initialization.

        cfg: :class:`ConfigParser`
            Configuration data is located under the section matching
            this allocator's `name`.

        Allows modifying `accounting_id` and factory options.
        """
        super(PBS_Allocator, self).configure(cfg)
        if cfg.has_option(self.name, 'accounting_id'):
            self.accounting_id = cfg.get(self.name, 'accounting_id')
            self._logger.debug('    accounting_id: %s', self.accounting_id)

    @rbac('*')
    def max_servers(self, resource_desc):
        """
        Return the maximum number of servers which could be deployed for
        `resource_desc`.  

        resource_desc: dict
            Description of required resources.
        """
        return (10000, {})
        retcode, info = self.check_compatibility(resource_desc)
        if retcode != 0:
            return (0, info)
        elif 'min_cpus' in resource_desc:
            # get available cpu's from the allocator
           #self.n_cpus = self._qstat()

            return (self.max_cpus / resource_desc['min_cpus'], {})
        else:
            return (self.max_cpus, {})

    @rbac('*')
    def time_estimate(self, resource_desc):
        """
        Return ``(estimate, criteria)`` indicating how well this resource
        allocator can satisfy the `resource_desc` request.  The estimate will
        be:

        - >0 for an estimate of walltime (seconds).
        -  0 for no estimate.
        - -1 for no resource at this time.
        - -2 for no support for `resource_desc`.

        The returned criteria is a dictionary containing information related
        to the estimate, such as hostnames, load averages, unsupported
        resources, etc.

        resource_desc: dict
            Description of required resources.
        """

        hostnames=[]
        if 'name' in resource_desc:
            if resource_desc['name'] != self.name:
                return (-2,hostnames) 
                
        
        if 'min_cpus' in resource_desc:
           n_cpus = resource_desc['min_cpus'] 

        if self._qstat() < n_cpus:
            return (-1,hostnames)

        nh = 0
        for host in self.workers:
            if nh == n_cpus: break
            if host['state'] == 1:
                hostnames.append(host['hostname'])
                nh+=1

        criteria = {
            'hostnames':hostnames,
        }

        return (nh,criteria)

    def check_compatibility(self, resource_desc):
        """
        Check compatibility with resource attributes.

        resource_desc: dict
            Description of required resources.

        Returns ``(retcode, info)``. If Compatible, then `retcode` is zero
        and `info` is empty. Otherwise `retcode` will be -2 and `info` will
        be a single-entry dictionary whose key is the incompatible key in
        `resource_desc` and value provides data regarding the incompatibility.
        """
        retcode, info = \
            super(MPI_Allocator, self).check_compatibility(resource_desc)
        if retcode != 0:
            return (retcode, info)

        for key in info:
            value = resource_desc[key]
            if key == 'localhost':
                if value:
                    return (-2, {key: 'requested local host'})
            elif key == 'min_cpus':
                self.n_cpus = self._qstat()
                if self.n_cpus < value:
                    return (-2, {'min_cpus': 'want %s, have %s'
                                             % (value, self.n_cpus)})
            elif key == 'max_cpus':
                pass
            else:
                return (-2, {key: 'unrecognized key'})
        return (0, {})

    @rbac('*')
    def deploy(self,name, resource_desc,criteria):
        """ 
        Deploy a server suitable for `resource_desc`.
        Returns a proxy to the deployed server.

        name: string
            Name for server.

        resource_desc: dict
            Description of required resources.

        criteria: dict
            The dictionary returned by :meth:`time_estimate`.
        """

        print 'allocating hosts',criteria['hostnames']
        for i,worker in enumerate(self.workers):
            for host in criteria['hostnames']:
                if worker['hostname'] == host:
                    worker['state'] = 0


        credentials = get_credentials()
        allowed_users = {credentials.user: credentials.public_key}
        try:
            server = self.factory.create(typname='', allowed_users=allowed_users,
                                       name=name)

            # overwrite the server's host list with the assigned hosts
            server.host = criteria['hostnames']
            return server

        # Shouldn't happen...
        except Exception as exc:  #pragma no cover
            self._logger.error('create failed: %r', exc)
            return None
 

    def release(self,server):
        """
        """
        print 'releasing hosts',server.host 
        for worker in self.workers:
            for host in server.host:
                if host == worker['hostname']:
                    worker['state'] = 1
            
    def _qstat(self):
        """check status of the workers and return number of free nodes"""
        free=0
        for i in range(len(self.workers)):
            free+=self.workers[i]['state']

        return free
             

    def shutdown(self):
        """ todo: shut down MPIallocator cluster """
        pass



if __name__ == '__main__':
    pass
