import os
from sys import stderr
import deswl
from deswl import generic
import esutil as eu
import desdb

_patterns={'am':'%(run)s-%(expname)s-%(ccd)02d-am.fits',
           'stat':'%(run)s-%(expname)s-%(ccd)02d-stat.yaml',
           'log':'%(run)s-%(expname)s-%(ccd)02d.log'}

class AMConfig(generic.GenericConfig):
    """
    to create and write the "config" files, which hold the command
    to run, input/output file lists, and other metadata.
    """
    def __init__(self,run, **keys):
        super(AMConfig,self).__init__(run)

    def write(self):
        """
        Write all config files for expname/ccd
        """
        super(AMConfig,self).write_byccd()

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
                    generic.generate_filenames(_patterns,
                                               'am',
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
        wl_load = deswl.files._make_load_command('wl',rc['wlvers'])
        esutil_load = deswl.files._make_load_command('esutil', rc['esutilvers'])
        espy_load = deswl.files._make_load_command('espy', rc['espyvers'])

        command = """
source ~esheldon/.bashrc

{esutil_load}
{wl_load}
{espy_load}

image=%(image)s
cat=%(cat)s
am=%(am)s
python $ESPY_DIR/des/bin/admom-des.py $image $cat $am\n"""

        command = command.format(esutil_load=esutil_load,
                                 wl_load=wl_load,
                                 espy_load=espy_load)
        return command

