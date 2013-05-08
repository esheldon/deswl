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

        module='im3shape/%s' % self.rc['IM3SHAPE_VERS']
        self.modules=[module]
        self.commands=self._get_commands()

    def get_flists(self):
        return self.get_flists_by_tile()

    def get_job_name(self, fd):
        job_name='%s-%s-%d' % (fd['tilename'],fd['band'],fd['start'])
        job_name=job_name.replace('DES','')
        return job_name


    def _get_commands(self):
        commands="""
    tilename=%(tilename)s
    meds_file=%(meds)s
    start=%(start)d
    nobj=%(nobj)d
    raw=%(raw)s
    clean=%(clean)s

    export OMP_NUM_THREADS=1
    $IM3SHAPE_DIR/launch_im3shape.sh ${tilename} ${meds_file} ${start} ${nobj} ${raw} ${clean}

    exit_status=$?
    
    return $exit_status
        """

        return commands

