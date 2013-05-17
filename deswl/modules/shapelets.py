from deswl import generic
import desdb

"""
_se_patterns={'stars':'%(run)s-%(expname)s-%(ccd)02d-stars.fits',
              'psf':'%(run)s-%(expname)s-%(ccd)02d-psf.fits',
              'fitpsf':'%(run)s-%(expname)s-%(ccd)02d-fitpsf.fits',
              'shear':'%(run)s-%(expname)s-%(ccd)02d-shear.fits',
              'stat':'%(run)s-%(expname)s-%(ccd)02d-stat.yaml',
              'log':'%(run)s-%(expname)s-%(ccd)02d.log'}
"""


class ShapeletsSEScripts(generic.GenericScripts):
    """
    to create and write the script files, which hold the command
    to run, input/output file lists, and other metadata.
    """
    def __init__(self, run, **keys):
        super(ShapeletsSEScripts,self).__init__(run)

        # don't put status, meta, or log here, they will get
        # over-written
        self.filetypes={'stars':{'ext':'fits'},
                        'psf':{'ext':'fits'},
                        'fitpsf':{'ext':'fits'},
                        'shear':{'ext':'fits'}}
        
        # we set timeout much longer than expected time per
        # the time per is the mean plus one standard deviation
        # so should itself be plenty
        self.seconds_per = 250 #
        self.timeout=15*60 # 15 minutes -> 900


    def get_flists(self, **keys):
        return self.get_flists_by_ccd(**keys)

    def get_script(self, fdict):
        rc=self.rc
        shapelets_load='module unload shapelets && module load shapelets/%s'
        shapelets_load = shapelets_load % rc['SHAPELETS_VERS']

        wl_config='$SHAPELETS_DIR/etc/wl.config'
        wl_config_desdm='$SHAPELETS_DIR/etc/wl_desdm.config'

        wl_config_local='$DESWL_DIR/share/config/%(fileclass)s/%(wl_config)s'
        wl_config_local=wl_config_local % self.rc

        # note only the {key} are set at this time
        text = """#!/bin/bash
#PBS -q serial
#PBS -l nodes=1:ppn=1
#PBS -l walltime=30:00
#PBS -N %(job_name)s
#PBS -j oe
#PBS -o %(pbslog)s
#PBS -V
#PBS -A des

%(load_modules_func)s

function run_shapelets() {

    wl_config=%(wl_config)s
    wl_config_desdm=%(wl_config_desdm)s
    wl_config_local=%(wl_config_local)s

    image=%(image)s
    cat=%(cat)s
    stars=%(stars)s
    fitpsf=%(fitpsf)s
    psf=%(psf)s
    shear=%(shear)s
    timeout=%(timeout)d


    export OMP_NUM_THREADS=1


    for prog in findstars measurepsf measureshear; do
        echo "running $prog" >> $log_file

        timeout $timeout $SHAPELETS_DIR/bin/$prog  \\
            $wl_config            \\
            +$wl_config_desdm     \\
            +$wl_config_local     \\
            image_file=$image     \\
            cat_file=$cat         \\
            stars_file=$stars     \\
            fitpsf_file=$fitpsf   \\
            psf_file=$psf         \\
            shear_file=$shear     \\
            output_dots=false     \\
                2>&1 >> $log_file

        exit_status=$?
        if [[ $exit_status != "0" ]]; then
            echo "error running $prog: $err" >> $log_file
            return $exit_status
        fi
    done

    return 0
}


log_file=%(log)s
status_file=%(status)s

echo "host: $(hostname)" > $log_file

load_modules
exit_status=$?

if [[ $exit_status == "0" ]]; then
    run_shapelets
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
        allkeys['wl_config'] = wl_config
        allkeys['wl_config_desdm'] = wl_config_desdm
        allkeys['wl_config_local'] = wl_config_local
        for k,v in fdict.iteritems():
            if k not in ['input_files','output_files']:
                allkeys[k] = v
        for k,v in fdict['input_files'].iteritems():
            allkeys[k] = v
        for k,v in fdict['output_files'].iteritems():
            allkeys[k] = v

        allkeys['pbslog']=fdict['script']+'.pbslog'

        job_name='%s-%02d' % (fdict['expname'],fdict['ccd'])
        job_name=job_name.replace('decam-','')
        job_name=job_name.replace('DECam_','')
        allkeys['job_name']=job_name

        text = text % allkeys
        return text

