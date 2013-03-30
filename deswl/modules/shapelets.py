import os
from sys import stderr
import desdb
import deswl
from deswl import generic
import esutil as eu
import desdb

"""
_se_patterns={'stars':'%(run)s-%(expname)s-%(ccd)02d-stars.fits',
              'psf':'%(run)s-%(expname)s-%(ccd)02d-psf.fits',
              'fitpsf':'%(run)s-%(expname)s-%(ccd)02d-fitpsf.fits',
              'shear':'%(run)s-%(expname)s-%(ccd)02d-shear.fits',
              'stat':'%(run)s-%(expname)s-%(ccd)02d-stat.yaml',
              'log':'%(run)s-%(expname)s-%(ccd)02d.log'}
"""

# don't put status, meta, or log here, they will get
# over-written
SE_FILETYPES={'stars':{'ext':'fits'},
              'psf':{'ext':'fits'},
              'fitpsf':{'ext':'fits'},
              'shear':{'ext':'fits'}}

SE_TIMEOUT=15*60 # 15 minutes

class ShapeletsSEScripts(generic.GenericScripts):
    """
    to create and write the "config" files, which hold the command
    to run, input/output file lists, and other metadata.
    """
    def __init__(self, run, **keys):
        super(ShapeletsSEScripts,self).__init__(run)

    def write(self):
        """
        Write all config files for expname/ccd
        """
        super(ShapeletsSEScripts,self).write_byccd()

    def get_config_data(self):
        if self.config_data is not None:
            return self.config_data

        desdata=desdb.files.get_des_rootdir()
        flists = desdb.files.get_red_info_by_release(self.rc['dataset'],
                                                     self.rc['band'],
                                                     desdata=desdata)

        for fd in flists:
            expname=fd['expname']
            ccd=fd['ccd']

            fd['run'] = self['run']
            fd['cat_url']=fd['image_url'].replace('.fits.fz','_cat.fits')
            fd['input_files'] = {'image':fd['image_url'],
                                 'cat':fd['cat_url']}
            fd['output_files']=self.get_output_filenames(expname=expname,
                                                         ccd=ccd)

            fd['timeout'] = SE_TIMEOUT


        self.config_data = flists
        return flists

    def get_output_filenames(self, **keys):
        expname=keys['expname']
        ccd=keys['ccd']
        fdict={}
        for ftype in SE_FILETYPES:
            ext=SE_FILETYPES[ftype]['ext']
            fdict[ftype] = self._df.url(type='wlpipe_se_gen',
                                        run=self['run'],
                                        expname=expname,
                                        ccd=ccd,
                                        filetype=ftype,
                                        ext=ext)
        return fdict


    def get_script(self, fdict):
        rc=self.rc
        shapelets_load = 'module unload shapelets && module load shapelets/%s' % rc['SHAPELETS_VERS']

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
#PBS -o %(job_file)s.pbslog
#PBS -V
#PBS -A des

# If all goes as planned, there will be no output
# from this at all
%(shapelets_load)s

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

status_file=%(status)s
log_file=%(log)s

export OMP_NUM_THREADS=1

echo "host: $(hostname)" > $log_file

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
        break
    fi
done

echo "time-seconds: $SECONDS" >> $log_file

mess="writing status $exit_status to:
    $status_file"
echo $mess >> $log_file

echo "$exit_status" > "$status_file"
exit $exit_status
        \n"""

        # now interpolate the rest
        allkeys={}
        allkeys['shapelets_load'] = shapelets_load
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

        allkeys['job_file']=fdict['script']

        job_name='%s-%02d' % (fdict['expname'],fdict['ccd'])
        job_name=job_name.replace('decam-','')
        job_name=job_name.replace('DECam_','')
        allkeys['job_name']=job_name

        text = text % allkeys
        return text

