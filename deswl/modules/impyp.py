import os
from sys import stderr
import deswl
from deswl import generic
import esutil as eu

_impyp_patterns={'shear':'%(run)s-%(expname)s-%(ccd)02d-shear.dat',
                 'stat':'%(run)s-%(expname)s-%(ccd)02d-stat.yaml',
                 'log':'%(run)s-%(expname)s-%(ccd)02d.log'}



class ImpypConfig(dict):
    """
    to create and write the "config" files, which hold the command
    to run, input/output file lists, and other metadata.

    We just use deswl.files.ShearFiles to generate filenames and then put in
    the special output_files and input_files sub-dicts.
    """
    def __init__(self,run, **keys):
        self['run'] = run
        # this has serun in it
        self.rc = deswl.files.Runconfig(self['run'])

        self.config_data=None

    def write(self):
        """
        Write all config files for expname/ccd
        """
        all_fd = self.get_config_data()
        i=1
        ne=62*len(all_fd)
        for expname,fdlist in all_fd.iteritems():
            # now by ccd
            for fd in fdlist:
                config_file=deswl.files.se_config_path(self['run'],
                                                       fd['expname'],
                                                       ccd=fd['ccd'])
                if (i % 1000) == 0:
                    print >>stderr,"Writing config (%d/%d) %s" % (i,ne,config_file)
                eu.ostools.makedirs_fromfile(config_file)
                eu.io.write(config_file, fd)
                i += 1


    def get_config_data(self):
        if self.config_data is not None:
            return self.config_data

        rf=deswl.files.ShearFiles(self.rc['serun'])
        expdict = rf.get_flist(by_expname=True)
        
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
                fdict['input_files'] = {'image':fd['image'],'stars':fd['stars']}
                fdict['output_files']=\
                    generic.generate_filenames(_impyp_patterns,
                                               'impyp',
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
        impyp_load = deswl.files._make_load_command('impyp', rc['impypvers'])

        command = """
source ~esheldon/.bashrc

{esutil_load}
{wl_load}
{impyp_load}

image=%(image)s
stars=%(stars)s
shear=%(shear)s
python $IMHOME/bnl_impyp.py $image $stars $shear\n"""

        command = command.format(esutil_load=esutil_load,
                                 wl_load=wl_load,
                                 impyp_load=impyp_load)
        return command







'''
def impyp_dir(run, expname, **keys):
    rc=deswl.files.Runconfig()

    fileclass=rc.run_types['impyp']['fileclass']
    rundir=deswl.files.run_dir(fileclass, run, **keys)
    dir=os.path.join(rundir, expname)
    return dir

def impyp_url(run, expname, ccd, ftype, **keys):
    if ftype not in _impyp_patterns:
        raise ValueError("bad impyp ftype: '%s'" % ftype)

    basename=_impyp_patterns[ftype] % {'run':run,
                                    'expname':expname,
                                    'ccd':int(ccd)}

    dir = impyp_dir(run, expname, **keys)

    url = os.path.join(dir, basename)
    return url

def generate_impyp_filenames(run, expname, ccd, **keys):
    """
    Output filenames for impyp
    """
    fdict={}

    # output file names
    for ftype in _impyp_patterns:
        #name= impyp_url(run, expname, ccd, ftype, **keys)
        name=generic.genurl(_impyp_patterns[ftype],
                            'impyp',run, expname, ccd=ccd, **keys)
        fdict[ftype] = name

    return fdict

'''
