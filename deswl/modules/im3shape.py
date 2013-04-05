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

        # assuming 10 images in stack
        time_per_object=30
        self.seconds_per = nper*time_per_object

        # buffer quite a bit.  Note the above estimate
        # should already be way too big, so this is fine
        self.timeout=5*self.seconds_per

        # don't put status, meta, or log here, they will get
        # over-written
        self.filetypes={'raw':{'ext':'txt'},
                        'clean':{'ext':'txt'}}

        self.module_uses=\
            ['/project/projectdirs/des/wl/desdata/users/cogs/usr/modulefiles']
        self.modules=['im3shape']
        self.commands=self._get_commands()

    def get_flists(self):
        return self.get_flists_by_tile()

    def get_job_name(self, fd):
        job_name='%s-%s' % (fd['tilename'],fd['band'])
        return job_name


    def _get_commands(self):
        """
        timeout and log_file are defined on entry
        """

        commands="""
    tilename=%(tilename)s
    meds_file=%(meds)s
    start=%(start)d
    nobj=%(nobj)d
    raw=%(raw)s
    clean=%(clean)s

    export OMP_NUM_THREADS=1
    timeout $timeout $IM3SHAPE_DIR/launch_im3shape.sh ${tilename} ${meds_file} ${start} ${nobj} ${raw} ${clean} >> $log_file

    exit_status=$?
    
    return $exit_status
        """

        return commands







    def get_script_old(self, fdict):
        rc=self.rc
        load='module load im3shape/%s'
        load = load % rc['IM3SHAPE_VERS']

        load="echo > /dev/null"


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
    meds_file=%(meds)s
    raw_file=%(raw)s
    clean_file=%(clean)s
    timeout=%(timeout)d

    export OMP_NUM_THREADS=1

    command="
        timeout $timeout echo hello
    "

    $command >> $log_file
    exit_status=$?
    
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
echo "time-seconds: $SECONDS" >> $log_file

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

