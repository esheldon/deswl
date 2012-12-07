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

class ShapeletsSEConfig(generic.GenericConfig):
    """
    to create and write the "config" files, which hold the command
    to run, input/output file lists, and other metadata.
    """
    def __init__(self,run):
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
        expdict = desdb.files.get_red_info_byexp(self.rc['dataset'],
                                                 self.rc['band'],
                                                 desdata=desdata)
        
        odict={}
        for expname,flist in expdict.iteritems():
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
                odict[expname].append(fdict)

        self.config_data = odict 
        return odict

    def get_command(self, fdict):
        rc=self.rc
        shapelets_load = 'module unload shapelets && module load shapelets/%s' % rc['SHAPELETS_VERS']
        deswl_load = 'module unload deswl && module load deswl/%s' % rc['DESWL_VERS']

        wl_config='$DESWL_DIR/share/config/%(fileclass)s/%(wl_config)s'
        wl_config=wl_config % self.rc

        # note only the {key} are set at this time
        command = """
source ~esheldon/.bashrc

{deswl_load}
{shapelets_load}

wl_config={wl_config}
image=%(image)s
cat=%(cat)s
stars=%(stars)s
fitpsf=%(fitpsf)s
psf=%(psf)s
shear=%(shear)s
export OMP_NUM_THREADS=1
$SHAPELETS_DIR/bin/fullpipe $wl_config \
    image_file=$image     \
    cat_file=$cat         \
    stars_file=$stars     \
    fitpsf_file=$fitpsf   \
    psf_file=$psf         \
    shear_file=$shear

        \n"""

        command = command.format(deswl_load=deswl_load,
                                 shapelets_load=shapelets_load,
                                 wl_config=wl_config)
        return command

