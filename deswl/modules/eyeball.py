from deswl import generic
import desdb

class EyeballScripts(generic.GenericScripts):
    def __init__(self, run, **keys):
        super(EyeballScripts,self).__init__(run)

        # 5 minutes is plenty
        self.timeout=5*60*60

        # about a factor of 4 larger than what we expect
        self.seconds_per = 20

        # don't put status, meta, or log here, they will get
        # over-written
        # using optional typename to allow different filetypes to have
        # same type name but different extensions
        self.filetypes={'mosaic_jpg':  {'typename':'mosaic', 'ext':'jpg'},
                        'mosaic_fits': {'typename':'mosaic', 'ext':'fits.fz'},
                        'field_jpg2':  {'typename':'field2', 'ext':'jpg'},
                        'field_jpg4':  {'typename':'field4', 'ext':'jpg'}}

        # don't need this, will just load my normal modules
        self.module_uses=None
        self.modules=None
        self.commands=self._get_commands()

    def get_flists(self):
        return self.get_flists_by_ccd()

    def get_job_name(self, fd):
        job_name='%s-%02d' % (fd['expname'],fd['ccd'])
        job_name=job_name.replace('DECam_','')
        return job_name


    def _get_commands(self):
        """
        timeout and log_file are defined on entry
        """
        
        # indent because this is written within the function
        commands="""
    nsetup_ess

    image=%(image)s
    bkg=%(bkg)s
    cat=%(cat)s
    mosaic_fits=%(mosaic_fits)s
    mosaic_jpg=%(mosaic_jpg)s
    field_jpg2=%(field_jpg2)s
    field_jpg4=%(field_jpg4)s

    timeout $timeout python $EYEBALLER_DIR/bin/make-se-eyeball.py \\
            ${image} ${bkg} ${cat} \\
            ${mosaic_fits} ${mosaic_jpg} \\
            ${field_jpg2} ${field_jpg4} 2>&1 >> $log_file

    exit_status=$?
    
    return $exit_status
        """

        return commands








