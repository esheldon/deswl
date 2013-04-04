from deswl import generic
import desdb

class I3MEScripts(generic.GenericScripts):
    def __init__(self, run, **keys):
        super(I3MEScripts,self).__init__(run)

        # we set timeout much longer than expected time per
        # the time per is the mean plus one standard deviation
        # so should itself be plenty

        # number of objects per job
        nper=self.rc['nper']
        time_per_object=30 # assuming 10 images in stack
        self.seconds_per = nper*time_per_object

        # buffer quite a bit.  Note the above estimate
        # should already be way too big, so this is fine
        self.timeout=5*self.seconds_per

        # don't put status, meta, or log here, they will get
        # over-written
        self.filetypes={'raw':{'ext':'txt'},
                        'clean':{'ext':'txt'}}


    def get_flists(self):
        """
        im3shape is slow so we use the chunking functionality
        """
        return self.get_flists_by_tile()

    def get_script(self, fdict):
        rc=self.rc
        load='module load im3shape/%s'
        load = load % rc['IM3SHAPE_VERS']


        # note only the {key} are set at this time
        text = """#!/bin/bash
#PBS -q serial
#PBS -l nodes=1:ppn=1
#PBS -l walltime=3:00:00
#PBS -N %(job_name)s
#PBS -j oe
#PBS -o %(pbslog)s
#PBS -V
#PBS -A des

# stdout goes to the log file
%(load)s

meds_file=%(meds)s
raw_file=%(raw)s
clean_file=%(clean)s

timeout=%(timeout)d

status_file=%(status)s
log_file=%(log)s

export OMP_NUM_THREADS=1

echo "host: $(hostname)" > $log_file

command="
    echo hello >> $log_file
"

echo "time-seconds: $SECONDS" >> $log_file

mess="writing status $exit_status to:
    $status_file"
echo $mess >> $log_file

echo "$exit_status" > "$status_file"
exit $exit_status
        \n"""

        # now interpolate the rest
        allkeys={}
        allkeys['load'] = load
        for k,v in fdict.iteritems():
            if k not in ['input_files','output_files']:
                allkeys[k] = v
        for k,v in fdict['input_files'].iteritems():
            allkeys[k] = v
        for k,v in fdict['output_files'].iteritems():
            allkeys[k] = v

        allkeys['pbslog']=fdict['script']+'.pbslog'

        job_name='%s-%s' % (fdict['tilename'],fdict['band'])
        allkeys['job_name']=job_name

        text = text % allkeys
        return text

