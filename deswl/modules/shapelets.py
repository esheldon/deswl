import os
from sys import stderr
import desdb
import deswl
from deswl import generic
import esutil as eu
import desdb

_output_patterns={'stars':'%(run)s-%(expname)s-%(ccd)02d-stars.fits',
                  'psf':'%(run)s-%(expname)s-%(ccd)02d-psf.fits',
                  'fitpsf':'%(run)s-%(expname)s-%(ccd)02d-fitpsf.fits',
                  'shear':'%(run)s-%(expname)s-%(ccd)02d-shear.fits',
                  'qa':'%(run)s-%(expname)s-%(ccd)02d-qa.fits',
                  'stat':'%(run)s-%(expname)s-%(ccd)02d-stat.yaml',
                  'log':'%(run)s-%(expname)s-%(ccd)02d.log'}

def get_se_filename(type, run, expname, ccd):
    if type not in _output_patterns:
        raise ValueError("type '%s' not found in patterns" % type)
    pattern=_output_patterns[type]
    return deswl.generic.genurl(pattern, 'shapelets', run, expname, ccd=ccd)

class ShapeletsSEConfig(generic.GenericConfig):
    """
    to create and write the "config" files, which hold the command
    to run, input/output file lists, and other metadata.
    """
    def __init__(self,run, **keys):
        super(ShapeletsSEConfig,self).__init__(run)

    def write(self):
        """
        Write all config files for expname/ccd
        """
        super(ShapeletsSEConfig,self).write_byccd()

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
            fd['output_files']=\
                generic.generate_filenames(_output_patterns,
                                           'shapelets',
                                           self['run'],
                                           expname,
                                           ccd=ccd)


            fd['command'] = self.get_command(fd)
            fd['timeout'] = 15*60 # fifteen minute timeout

            script_file=self.get_script_file(fd)
            fd['script'] = script_file

        self.config_data = flists
        return flists

        """
        odict={}
        for flist in flists:
            expname=flist['expname']

            odict[expname] = []
            # one for each CCD
            for fd in flist:
                ccd=fd['ccd']

                fdict={}
                fdict['run'] = self['run']
                fdict['expname'] = expname
                fdict['band'] = fd['band']
                fdict['ccd'] = ccd
                fdict['input_files'] = \
                    {'image':fd['image_url'],'cat':fd['cat_url']}
                fdict['output_files']=\
                    generic.generate_filenames(_output_patterns,
                                               'shapelets',
                                               self['run'],
                                               expname,
                                               ccd=ccd)
                
                fdict['command'] = self.get_command(fdict)
                fdict['timeout'] = 15*60 # fifteen minute timeout

                script_file=self.get_script_file(fd)
                fdict['script'] = script_file
                odict[expname].append(fdict)
        self.config_data = odict 
        return odict
        """

    def get_script_file(self, fdict):
        script_file=deswl.files.get_se_script_path(self['run'],
                                                   fdict['expname'],
                                                   fdict['ccd'])
        return script_file

    def get_command(self, fdict):
        script_file=self.get_script_file(fdict)
        return 'time bash %s' % script_file


    def get_script(self, fdict):
        rc=self.rc
        shapelets_load = 'module unload shapelets && module load shapelets/%s' % rc['SHAPELETS_VERS']

        wl_config='$SHAPELETS_DIR/etc/wl.config'
        wl_config_desdm='$SHAPELETS_DIR/etc/wl_desdm.config'

        wl_config_local='$DESWL_DIR/share/config/%(fileclass)s/%(wl_config)s'
        wl_config_local=wl_config_local % self.rc

        # note only the {key} are set at this time
        text = """#!/bin/bash
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

export OMP_NUM_THREADS=1

for prog in findstars measurepsf measureshear; do
    echo "running $prog"
    $SHAPELETS_DIR/bin/$prog  \\
        $wl_config            \\
        +$wl_config_desdm     \\
        +$wl_config_local     \\
        image_file=$image     \\
        cat_file=$cat         \\
        stars_file=$stars     \\
        fitpsf_file=$fitpsf   \\
        psf_file=$psf         \\
        shear_file=$shear     \\
        output_dots=false
    err=$?
    if [[ $err != "0" ]]; then
        echo "error running $prog: $err"
        exit $err
    fi
done
echo "time-seconds: $SECONDS"
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


        text = text % allkeys
        return text

