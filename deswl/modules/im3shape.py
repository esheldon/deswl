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

%(load_modules_func)s

function run_im3shape() {
    # stdout goes to the log file
    meds_file=%(meds)s
    raw_file=%(raw)s
    clean_file=%(clean)s

    timeout=%(timeout)d


    export OMP_NUM_THREADS=1


    command="
        echo hello
    "

    $command >> $log_file
    exit_status=$?
    echo "time-seconds: $SECONDS" >> $log_file
    
    return $exit_status
}

log_file=%(log)s
status_file=%(status)s

echo "host: $(hostname)" > $log_file

load_modules
exit_status=$?

if [[ $exit_status == "0" ]]; then
    run_im3shape
    exit_status=$?
fi

mess="writing status $exit_status to:
    $status_file"
echo $mess >> $log_file

echo "$exit_status" > "$status_file"
exit $exit_status
        \n"""

        lmodfunc=generic.get_load_modules_func()
        lmodfunc=lmodfunc % {'load_modules':load}

        # now interpolate the rest
        allkeys={}
        allkeys['load_modules_func'] = lmodfunc
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

