from deswl import generic
import desdb

class GMixMCMCMaster(generic.GenericScripts):
    def __init__(self, run, **keys):
        super(GMixMCMCMaster,self).__init__(run)

        # we set timeout much longer than expected time per
        # the time per is the mean plus one standard deviation
        # so should itself be plenty

        # number of objects per job
        nper=self.rc['nper']

        # assuming 10 images in stack
        time_per_object=20.0

        # seconds per job
        self.seconds_per = nper*time_per_object

        # buffer a lot
        self.timeout=self.seconds_per

        # don't put status, meta, or log here, they will get
        # over-written
        self.filetypes={'mcmc':{'ext':'fits'}}

        # we just put nsetup_ess in the commands
        self.module_uses=None
        self.modules=None

    def get_flists(self, **keys):
        return self.get_flists_by_tile(**keys)

    def get_job_name(self, fd):
        job_name='%s-%s-%d' % (fd['tilename'],fd['band'],fd['start'])
        job_name=job_name.replace('DES','')
        return job_name


    def _get_command_template(self, have_detrun=False):
        """
        For condor these are the arguments

        mcmc is the output file type
        """
        t="%(meds)s %(start)s %(end)s %(mcmc)s %(log)s"
        return t

    def _get_master_script_template(self):
        """
        timeout and log_file are defined on entry
        """
        
        commands="""#!/bin/bash

function go {
    hostname

    gmvers="%(version)s"
    module unload gmix_image && module load gmix_image/work
    module unload ngmix && module load ngmix/work
    module unload psfex-ess && module load psfex-ess/work
    module unload meds && module load meds/work
    module unload gmix_meds && module load gmix_meds/$gmvers


    confname="%(config)s.yaml"
    conf="$GMIX_MEDS_DIR/share/config/$confname"

    python -u $GMIX_MEDS_DIR/bin/gmix-fit-meds     \\
            --obj-range $start,$end                \\
            --work-dir $tmpdir                     \\
            $conf $meds_file $out_file
    
    exit_status=$?

}

#nsetup_ess
source ~/.bashrc

if [ $# -lt 5 ]; then
    echo "error: meds_file start end out_file"
    exit 1
fi

# this can be a list
meds_file="$1"
start="$2"
end="$3"
out_file="$4"
log_file="$5"

if [[ -n $_CONDOR_SCRATCH_DIR ]]; then
    tmpdir=$_CONDOR_SCRATCH_DIR
else
    tmpdir=$TMPDIR
fi

outdir=$(dirname $out_file)
mkdir -p $outdir

lbase=$(basename $log_file)
tmplog="$tmpdir/$lbase"

go &> "$tmplog"
cp "$tmplog" "$log_file"

exit $exit_status\n"""

        return commands

