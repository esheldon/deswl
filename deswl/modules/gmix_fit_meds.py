from deswl import generic
import desdb

class GMixFitMEScripts(generic.GenericScripts):
    def __init__(self, run, **keys):
        super(GMixFitMEScripts,self).__init__(run)

        # we set timeout much longer than expected time per
        # the time per is the mean plus one standard deviation
        # so should itself be plenty

        # number of objects per job
        nper=self.rc['nper']

        # assuming 10 images in stack
        # 5 is the worst I saw, so let's double it
        time_per_object=10.0

        # seconds per job
        self.seconds_per = nper*time_per_object

        # buffer a lot
        self.timeout=self.seconds_per

        # don't put status, meta, or log here, they will get
        # over-written
        self.filetypes={'lmfit':{'ext':'fits'}}

        # we just put nsetup_ess in the commands
        self.module_uses=None
        self.modules=None

        self.commands=self._get_commands()

    def get_flists(self):
        return self.get_flists_by_tile()

    def get_job_name(self, fd):
        job_name='%s-%s-%d' % (fd['tilename'],fd['band'],fd['start'])
        job_name=job_name.replace('DES','')
        return job_name


    def _get_commands(self):
        """
        timeout and log_file are defined on entry
        """

        commands="""
    nsetup_ess
    gmvers=%(version)s

    # need to make these configurable.  Can put in a single
    # module gmix_meds_run
    module unload gmix_image && module load gmix_image/work
    module unload psfex && module load psfex/work
    module unload meds && module load meds/work
    module unload gmix_meds && module load gmix_meds/$gmvers

    meds_file="%(meds)s"
    out_file="%(lmfit)s"
    start=%(start)d
    end=%(end)d

    confname=gfmeds-%(config)s.yaml

    conf=$GMIX_MEDS_DIR/share/config/$confname

    $GMIX_MEDS_DIR/bin/gmix-fit-meds            \\
            --obj-range $start,$end             \\
            ${conf} ${meds_file} ${out_file}

    exit_status=$?
    return $exit_status
        """

        return commands
