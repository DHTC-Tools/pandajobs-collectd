#!/usr/bin/env python

import collectd, string, time, pycurl
from StringIO import StringIO

# list of computing elements for which to query
computingelements = []

def config_pandajob_plugin(config):
    # Get computing elements from config block
    # usage: ComputingElement "<queue>"
    for line in config.children:
        if line.key=="ComputingElement":
            computingelements.append(line.values[0])

def init_pandajob_plugin():
    collectd.debug("init")

def read_panda_jobs(input_data=None):

    for queue in computingelements:

        metrics = {}

        timestamp = int(time.time())

        # Get json output from http://bigpanda.cern.ch
        buffer = StringIO()
        c = pycurl.Curl()
        c.setopt(c.URL, "http://bigpanda.cern.ch/jobs/?hours=1&fields=pandaid,modificationhost,jobstatus&computingelement=" + queue)
        c.setopt(c.WRITEFUNCTION, buffer.write)
        c.setopt(c.HTTPHEADER, ["Accept: application/json"])
        c.setopt(c.HTTPHEADER, ["Content-Type: application/json"])
        c.perform()
        alljobs = buffer.getvalue().split("}, {")
        buffer.close()
        c.close()

        # Process json output
        for job in alljobs:
            p = job.split()

            # Get host site
            try:
                job_hostsite = p[p.index('"modificationhost":')+1]
                if "uct2" in job_hostsite:
                    job_hostsite = "uct2"
                elif "iu.edu" in job_hostsite:
                    job_hostsite = "iut2"
                elif ("golub" in job_hostsite) | ("taub" in job_hostsite):
                    job_hostsite = "uiuc"
                else:
                    job_hostsite = job_hostsite[1:-1].split("@")[1].split(".")[-2]
                if (job_hostsite not in metrics):
                    metrics[job_hostsite] = {'pending':0, 'defined':0, 'waiting':0, \
                       'assigned':0, 'throttled':0, 'activated':0, 'sent':0, 'starting':0, \
                       'running':0, 'holding':0, 'merging':0, 'transferring':0, 'finished':0, \
                       'failing':0, 'failed':0, 'cancelled':0, }
            except ValueError:
                job_hostsite = "undefined"


            # Increment appropriate job status
            try:
                job_status = p[p.index('"jobstatus":')+1][1:-2].lower() 
                metrics[job_hostsite][job_status] += 1
            except ValueError:
                job_status = "undefined"

        # Send the data back to collectd
        for hostsite in metrics:
            for status in metrics[hostsite]:
                val = collectd.Values(type='gauge', host='pandajobs', plugin=queue, plugin_instance=hostsite, time=timestamp, type_instance=status)
                val.dispatch(values=[metrics[hostsite][status]])

# Collectd register callback functions
collectd.register_config(config_pandajob_plugin)
collectd.register_init(init_pandajob_plugin)
collectd.register_read(read_panda_jobs, 3600)  # 3600: only poll once an hour
