from sys import stdout,stderr
import platform
import os
import re
import esutil as eu
from esutil import json_util
from esutil.ostools import path_join, getenv_check

import pprint
import deswl
import desdb
from desdb.files import expand_desvars

# for separating file elements
file_el_sep = '-'


def get_proc_environ(extra=None):
    e={}
    e['DESWL_DIR']=getenv_check('DESWL_DIR')
    e['DESWL_VERS']=getenv_check('DESWL_VERS')

    e['DESDB_DIR']=getenv_check('DESDB_DIR')
    e['DESDB_VERS']=getenv_check('DESDB_VERS')

    e['ESUTIL_DIR']=getenv_check('ESUTIL_DIR')
    e['ESUTIL_VERS']=getenv_check('ESUTIL_VERS')

    e['DES_FILE_LISTS']=getenv_check('DES_FILE_LISTS')

    e['PYTHON_VERS']=deswl.get_python_version()

    if extra:
        for k in extra:
            e[k] = getenv_check(k)
    return e

class Runconfig(dict):
    def __init__(self, run=None):
        
        self.run=run

        self.run_types={}
        self.run_types['sse'] = {'name':'sse', 'fileclass': 'shapelets'}
        self.run_types['sme'] = {'name':'sme', 'fileclass': 'shapelets'}

        # these are "external" codes.  The only places we explicitly work with
        # them is in this class, otherwise operations specific to these are are
        # in {type}.py or something similar
        self.run_types['impyp'] = {'name':'impyp', 'fileclass': 'impyp'}
        self.run_types['am']    = {'name':'am',    'fileclass': 'am'}

        if run is not None:
            self.load(run)

        """
        self.se_executables = ['findstars','measurepsf','measureshear']

        self.se_filetypes = ['stars', 'stars1','stars2',
                             'psf','psf1','psf2',
                             'fitpsf','fitpsf1','fitpsf2',
                             'shear','shear1','shear2',
                             'qa','stat','debug']
        self.se_fext       = {'stars':  '.fits', 
                              'stars1':  '.fits', 
                              'stars2':  '.fits', 
                              'psf':    '.fits',
                              'psf1':    '.fits',
                              'psf2':    '.fits',
                              'fitpsf': '.fits', 
                              'fitpsf1': '.fits', 
                              'fitpsf2': '.fits', 
                              'shear':  '.fits',
                              'shear1':  '.fits',
                              'shear2':  '.fits',
                              'qa':     '.dat',
                              'debug':'.dat',
                              'stat':   '.json',
                              'checkpsf':'.rec'}
        self.se_collated_filetypes = {'badlist':'json',
                                      'goodlist':'json',
                                      'gal':'fits'}
        self.me_collated_filetypes = {'badlist':'json', 'goodlist':'json'}


        self.me_filetypes = ['multishear','qa','stat','debug']
        self.me_fext       = {'multishear':'.fits',
                              'qa':'.dat',
                              'stat':'.json',
                              'debug':'.dat'}


        self.me_executables = ['multishear']
        """

    def get_basedir(self, checkout=False):
        """
        checkout is for when we are generating the file
        """
        if checkout:
            dir=getenv_check('DESWL_CHECKOUT')
            dir=os.path.join(dir,'runconfig')
        else:
            dir=getenv_check('DESWL_DIR')
            dir=os.path.join(dir,'share','runconfig')
        return dir

    def getpath(self, run=None, checkout=False):
        if run is None:
            if self.run is None:
                raise ValueError("Either send run= keyword or "
                                 "load a runconfig")
            else:
                run=self.run
        rdir = self.get_basedir(checkout=checkout)
        return path_join(rdir, run+'-config.json')
    

    def generate_new_runconfig_name(self, run_type, band, test=False, old=False):
        """
        generates a new name like se013i, checking against names in
        the runconfig directory
        """

        if run_type not in self.run_types:
            mess="Unknown run type: '%s'.  Must be one "+\
                 "of: (%s)" % (run_type, ', '.join(self.run_types))
            raise ValueError(mess)

        if run_type == 'me':
            if test:
                starti=3
            else:
                starti=1
        elif run_type == 'se':
            if test:
                starti=13
            else:
                starti=1
        else:
            starti=1

        run_name=self._run_name_from_type_number(run_type, band, starti, test=test)

        fullpath=self.getpath(run=run_name, checkout=True)

        i=starti
        while os.path.exists(fullpath):
            i+=1
            run_name=self._run_name_from_type_number(run_type, band, i, test=test)
            fullpath=self.getpath(run=run_name, checkout=True)

        return run_name

    def _run_name_from_type_number(self, run_type, band, number, test=False):
        name='%(type)s%(num)03i%(band)s' % {'type':run_type,
                                            'num':number,
                                            'band':band}
        if test:
            name += 't'
        return name


    def generate_new_runconfig(self, 
                               run_type, 
                               dataset, 
                               band,
                               config=None,
                               run_name=None,
                               test=False, 
                               dryrun=False, 
                               comment=None,
                               **extra):
        """
        generate a new runconfig file

        You should load all the appropriate modules before running this so that
        the environment matches that which will be used in the run.

        parameters
        ----------
        run_type: string
            e.g. 'se' or 'me' 'impyp' 'am'
        dataset: string
            e.g. 'dr012'
        band: string
            'g','r','i','z','Y'
        config: string path, optional
            The location of a config file for this run/code.

            E.g.  'wldc6b-v2.config'

        run_name: string, optional
            If not sent, will be generated.
        test: bool, optional
            If true, this is a test run. Generated names will be like
            se008t
        dryrun: bool, optional
            If true, just show what would have been written to the file.

        comment: string, optional
            Add an additional comment.

        """

        # me runs depend on se runs
        if run_type in ['sme','impyp']:
            serun=extra.get('serun',None)
            if serun is None:
                raise RuntimeError("You must send serun=something for run "
                                   "type '%s'"  % run_type)
            # make sure serun exists by reading the config
            tmp=Runconfig(serun)



        if run_name is None:
            run_name=self.generate_new_runconfig_name(run_type, band, test=test)

        fileclass = self.run_types[run_type]['fileclass']

        # software versions.  Default to whatever is in our environment

        runconfig={'run':run_name, 
                   'run_type':run_type,
                   'band':band,
                   'fileclass': fileclass,
                   'dataset':dataset}

        if run_type in ['sme','sse']:
            if config is None:
                raise ValueError("Send config for run type '%s'" % run_type)
            runconfig['wl_config'] = config

        env_keys=self.get_required_env_keys(run_type)
        env=get_proc_environ(extra=env_keys)
        for k in env:
            runconfig[k] = env[k]
 
        if comment is not None:
            runconfig['comment'] = comment

        for e in extra:
            if e not in runconfig:
                runconfig[e] = extra[e]

        pprint.pprint(runconfig)

        fullpath=self.getpath(run=run_name,checkout=True)
        stdout.write('Writing to file: %s\n' % fullpath)
        eu.ostools.makedirs_fromfile(fullpath)
        if not dryrun:
            json_util.write(runconfig, fullpath)
            stdout.write("Don't forget to check in the file!\n")
        else:
            stdout.write(" .... dry run, skipping file write\n")


    def get_required_env_keys(self, run_type):
        """
        Get the list of environment variables that must exist

        These are in addition to the defaults
        """
        keys=[]
        if run_type in ['sme','sse']:
            # need DIR since we don't set PATH
            keys += ['TMV_VERS',
                     'SHAPELETS_VERS','SHAPELETS_DIR']

        if run_type=='am':
            keys += ['ADMOM_VERS','ESPY_VERS','ESPY_DIR']
        if run_type=='impyp':
            keys += ['IMPYP_VERS']

        return keys

    def verify(self):
        """

        verify current setup against the run config, make sure all versions and
        such are the same.  This does not check the data set or or
        serun for merun runconfigs.

        """

        env_keys=self.get_required_env_keys()
        vdict = get_proc_environ(extra=env_keys)


        for type in vdict:
            vers=self[type]
            cvers=vdict[type]
            if vers != cvers:
                # check for possiblity we are using a local install of trunk
                # declared with -r
                # note, when making the pbs files we automaticaly expand
                # trunk to ~/exports/{prodname}-work so putting in the -r
                # still is usually not necessary
                if cvers == 'trunk' and vers.find('-r') == -1:
                    raise ValueError("current %s '%s' does not match "
                                     "runconfig '%s'" % (type,cvers,vers))


    def load(self, run):
        config = self.read(run)
        for k in config:
            self[k] = config[k]
        self.run=run

    def read(self, run):
        name=self.getpath(run=run)
        if not os.path.exists(name):
            mess="runconfig for '%s' not found: %s\n" % (run, name)
            raise RuntimeError(mess)
        runconfig = json_util.read(name)
        return runconfig


def ftype2fext(ftype_input):
    ftype=ftype_input.lower()

    if ftype == 'fits' or ftype == 'fit':
        return 'fits'
    elif ftype == 'rec' or ftype == 'pya':
        return 'rec'
    elif ftype == 'json':
        return 'json'
    elif ftype == 'yaml':
        return 'yaml'
    elif ftype == 'xml':
        return 'xml'
    elif ftype == 'eps':
        return 'eps'
    elif ftype == 'ps':
        return 'ps'
    elif ftype == 'png':
        return 'png'
    else:
        raise ValueError("Don't know about '%s' files" % ftype)


def run_dir(fileclass, run, **keys):
    rootdir=desdb.files.get_des_rootdir(**keys)
    dir=path_join(rootdir, fileclass, run)
    return dir
 

def filetype_dir(fileclass, run, filetype, **keys):
    rundir=run_dir(fileclass, run, **keys)
    return os.path.join(rundir, filetype)

def exposure_dir(fileclass, run, filetype, expname, **keys):
    ftdir=filetype_dir(fileclass, run, filetype, **keys)
    return os.path.join(ftdir, expname)

def red_image_path(run, expname, ccd, **keys):
    fz=keys.get('fz',True)
    check=keys.get('check',False)

    fileclass='red'
    filetype='red'
    expdir = exposure_dir(fileclass, run, filetype, expname, **keys)
    imagename = '%s_%02i.fits' % (expname, int(ccd))
    basic_path = os.path.join(expdir, imagename)

    if check:
        # check both with and without .fz
        path=basic_path
        if not os.path.exists(path):
            path += '.fz'
            if not os.path.exists(path):
                raise RuntimeError("SE image not found: %s(.fz)\n" % basic_path)
    else:
        if fz:
            path=basic_path + '.fz'
        
    return path

def red_cat_path(run, expname, ccd, **keys):
    check=keys.get('check',False)

    fileclass='red'
    filetype='red'
    expdir = exposure_dir(fileclass, run, filetype, expname, **keys)
    imagename = '%s_%02i_cat.fits' % (expname, int(ccd))
    path = os.path.join(expdir, imagename)

    if check:
        if not os.path.exists(path):
            raise RuntimeError("SE catalog not found: %s\n" % path)
        
    return path




def extract_image_exposure_names(flist):
    allinfo={}
    for imfile in flist:
        info=get_info_from_path(imfile, 'red')
        allinfo[info['expname']] = info['expname']

    # list() of for py3k
    return list( allinfo.keys() )


def tile_dir(coaddrun, **keys):
    """
    Coadds are different than other output files:  The fileclass dir is 
    where the files are!
    """
    fileclass='coadd'
    filetype='coadd'
    tiledir=filetype_dir(fileclass, coaddrun, filetype, **keys)
    return tiledir

def coadd_image_path(coaddrun, tilename, band, **keys):
    """
    More frustration:  the path element for "coaddrun"
    contains the tilename but in principle it can be arbitrary, so 
    we can't simply pass in pieces:  coaddrun is treated independently

    If check=True, fz will be also be tried if not already set to True
    """
    fz=keys.get('fz',True)
    check=keys.get('check',False)

    tiledir=tile_dir(coaddrun, **keys)
    fname=tilename+'_'+band+'.fits'
    basic_path=path_join(tiledir, fname)
    if check:
        # check both with and without .fz
        path=basic_path
        if not os.path.exists(path):
            path += '.fz'
            if not os.path.exists(path):
                stdout.write("coadd image not found: %s(.fz)\n" % basic_path)
                return None
    else:
        if fz:
            path=basic_path + '.fz'
    return path

def coadd_cat_path(catalogrun, tilename, band, **keys):
    """
    More frustration:  the path element for "catalogrun"
    contains the tilename but in principle it can be arbitrary, so 
    we can't simply pass in pieces:  catalogrun is treated independently

    Note often catalogrun is the same as the coaddrun
    """
    check=keys.get('check',False)

    tiledir=tile_dir(catalogrun, **keys)
    fname=tilename+'_'+band+'_cat.fits'
    path=path_join(tiledir, fname)
    if check:
        if not os.path.exists(path):
            stdout.write("coadd catalog not found: %s(.fz)\n" % path)
            return None
    return path


_MOHR_MAP={'stars':'shpltall', 
           'psf':'shpltpsf', 
           'fitpsf':'psfmodel', 
           'shear':'shear'}
def mohrify_name(name):
    if name in _MOHR_MAP:
        return _MOHR_MAP[name]
    else:
        return name



# names and directories for  weak lensing processing
# se means single epoch
# me means multi-epoch

def se_dir(run, expname, **keys):
    rc=Runconfig()

    fileclass=rc.run_types['sse']['fileclass']
    rundir=run_dir(fileclass, run, **keys)
    dir=path_join(rundir, expname)
    return dir

def se_basename(serun, expname, ccd, ftype, **keys):

    fext=keys.get('fext',None)

    if fext is None:
        rc=Runconfig()
        fext = rc.se_fext.get(ftype,'.fits')

    el=[expname, '%02i' % int(ccd), ftype]

    el=[serun]+el

    basename=file_el_sep.join(el) + fext
    return basename



def se_url(serun, expname, ccd, ftype, **keys):
    """
    name=se_url(expname,ccd,ftype,serun=None,fext=None,fs=_default_fs,dir=None)
    Return the SE output file name for the given inputs
    """

    dir = se_dir(serun, expname, **keys)
    name = se_basename(serun, expname, ccd, ftype, **keys)
    path=path_join(dir,name)
    return path


def se_read(serun, expname, ccd, ftype, **keys):

    fpath=se_url(serun, expname, ccd, ftype, **keys)

    return eu.io.read(fpath, **keys)

def generate_se_filenames(serun, expname, ccd, **keys):
    fdict={}

    rc=deswl.files.Runconfig()

    split=keys.get('split',False)

    # output file names
    for ftype in rc.se_filetypes:
        if not split:
            if ftype[-1] == '1' or ftype[-1] == '2':
                continue
        name= se_url(serun, expname, ccd, ftype, **keys)
        fdict[ftype] = name

    return fdict

def coldir_open(serun):
    import columns
    cdir=coldir(serun)

    cols = columns.Columns(cdir)
    return cols
    

def coldir(run, fits=False, suffix=''):
    dir = collated_dir(run)
    if fits:
        dir=os.path.join(dir,run+suffix+'-fits')
    else:
        dir=os.path.join(dir,run+suffix+'.cols')
    return dir



def se_test_dir(serun, subdir=None):
    rc=Runconfig(run)
    fileclass=rc['fileclass']
    dir=run_dir(fileclass,serun)
    dir = path_join(dir, 'test')
    if subdir is not None:
        dir = path_join(dir, subdir)

    return dir

def se_test_path(serun, subdir=None, extra=None, fext='fits'):
    """
    e.g. se_test_path('se011it', subdir=['checksg', 'decam--22--44-i-11'],
                        extra='%02d' % ccd, fext='eps')
    """

    fname = [serun]
    if subdir is not None:
        if isinstance(subdir,(list,tuple)):
            fname += list(subdir)
        else:
            fname += [subdir]

    # extra only goes on the name
    if extra is not None:
        if isinstance(extra,(list,tuple)):
            fname += list(extra)
        else:
            fname += [str(extra)]

    fname='-'.join(fname)
    fname += '.'+fext

    dir = se_test_dir(serun, subdir=subdir)
    outpath = path_join(dir,fname)

    return outpath



def collated_dir(run):
    rc=Runconfig(run)
    fileclass=rc['fileclass']
    dir=run_dir(fileclass,run,fs='nfs')
    dir = path_join(dir, 'collated')
    return dir


def collated_path(run, 
                  objclass, 
                  ftype=None, 
                  delim=None):

    rc=Runconfig(run)
    fileclass=rc['fileclass']

    fname=[run,objclass]
    fname='-'.join(fname)

    # determine the file type
    if ftype is None:
        raise  ValueError("implement file type determination")
        
    # determine the extension
    fext = eu.io.ftype2fext(ftype)

    fname += '.'+fext

    dir = collated_dir(run)
    outpath = path_join(dir,fname)

    return outpath

def collated_read(serun, 
                  objclass, 
                  ftype=None, 
                  delim=None,
                  dir=None,
                  ext=0,
                  header=False,
                  rows=None,columns=None,fields=None,
                  norecfile=False, verbose=False):

    fpath=collated_path(serun, objclass, ftype=ftype, delim=delim)

    return eu.io.read(fpath, header=header, 
                          rows=rows, columns=columns, fields=fields,
                          norecfile=norecfile, verbose=verbose, 
                          ext=ext) 



# 
# This is a coadd info list that also has 'srclist'
def coadd_info_dir(dataset):
    desfiles_dir=getenv_check('DES_FILE_LISTS')
    desfiles_dir = path_join(desfiles_dir,dataset)
    return desfiles_dir


def coadd_info_url(dataset, srclist=False):
    if srclist:
        name=[dataset,'coadd','srclist']
    else:
        name=[dataset,'coadd','info']
    name = '-'.join(name)+'.pyobj'
    tdir=coadd_info_dir(dataset)
    return path_join(tdir, name)

_coadd_info_cache={'dataset':None,
                   'data':None,
                   'url':None,
                   'has_srclist':None}

def coadd_info_read(dataset, srclist=False, geturl=False):

    if (_coadd_info_cache['dataset'] == dataset 
            and _coadd_info_cache['has_srclist']==srclist):
        stdout.write('Re-using coadd info cache\n')
        url=_coadd_info_cache['url']
        data=_coadd_info_cache['data']
    else:
        url=coadd_info_url(dataset,srclist=srclist)
        print 'Reading coadd info:',url
        data=eu.io.read(url)

        _coadd_info_cache['dataset'] = dataset
        _coadd_info_cache['data'] = data
        _coadd_info_cache['url'] = url
        _coadd_info_cache['has_srclist'] = srclist

    if geturl:
        return data, url
    else:
        return data

def coadd_info_select(dataset, ids=None, tiles=None, bands=None, srclist=False):
    """
    Select from the input id or tile list.  Id and tile can be scalar
    or list/tupe.
    
    id is faster since that is they key, but keys by tile will be added as you
    go.

    """

    if ids is not None:
        return coadd_info_select_ids(dataset, ds, srclist=srclist)
    elif tiles is not None:
        return coadd_info_select_tile(dataset, tiles, bands=None, srclist=srclist)
    else:
        raise ValueError("send either id= or tiles=")

def coadd_info_select_ids(dataset, ids, srclist=False):

    cinfo=coadd_info_read(dataset, srclist=srclist)
    if isinstance(ids,(list,tuple)): 
        output=[]
        for id in ids:
            ci=_extract_coadd_info_id(cinfo, id)
            output.append(ci)
    else:
        output = _extract_coadd_info_id(cinfo, ids)

    return output

def _extract_coadd_info_id(cinfo, id):
    try:
        ci = cinfo[id]
    except KeyError:
        msg='id %s not found in coadd info for dataset %s'
        msg=msg % (id,_coadd_info_cache['dataset'])
        raise KeyError(msg)

    return ci

def coadd_info_select_tile(dataset, tiles, bands=None, srclist=False):

    cinfo=coadd_info_read(dataset, srclist=srclist)
    if isinstance(tiles,(list,tuple)): 
        output=[]
        for tile in tiles:
            ci=_extract_coadd_info_tile(cinfo, tile, bands=bands)
            output.append(ci)
    else:
        output = _extract_coadd_info_tile(cinfo, tiles)

    return output

def _extract_coadd_info_tile(cinfo, tile):
    for key in cinfo:
        ci=cinfo[key]
        if ci['tilename'] == tile:
            # also cache this tilename as key
            cinfo[tile] = ci
            return ci

    msg='tile %s not found in coadd info for dataset %s'
    msg=msg % (tile,_coadd_info_cache['dataset'])
    raise ValueError(msg)

def _extract_coadd_info_tile_band(cinfo, tile, bands):
    if not isinstance(bands,(type,list)):
        bands=[bands]

    for key in cinfo:
        ci=cinfo[key]
        for band in bands:
            if ci['tilename'] == tile and ci['band'] == band:
                # also cache this tilename-band as key
                tk='%s-%s' % (tile,band)
                cinfo[tk] = ci
                return ci

    msg='tile %s band %s not found in coadd info for dataset %s band %s'
    msg=msg % (tile,band,_coadd_info_cache['dataset'])
    raise ValueError(msg)

#
# multishear output files
#

def me_dir(merun, tilename, **keys):
    rc=Runconfig()

    fileclass=rc.run_types['me']['fileclass']
    rundir=run_dir(fileclass, merun, **keys)
    wldir=os.path.join(rundir, tilename)
    return wldir

def me_basename(tilename, band, ftype, **keys):
    """
    basename for multi-epoch weak lensing output files
    """

    merun=keys.get('merun',None)
    extra=keys.get('extra',None)
    fext=keys.get('fext',None)

    if fext is None:
        rc=Runconfig()
        if ftype in rc.me_fext:
            fext=rc.me_fext[ftype]
        else:
            fext='.fits'

    name=[tilename,band,ftype]
    
    if merun is not None:
        name=[merun] + name

    if extra is not None:
        name.append(extra)

    name=file_el_sep.join(name)+fext
    return name

def me_url(merun, tilename, band, ftype, **keys):
    """
    Return a multi-epoch shear output file name
    """

    dir = me_dir(merun, tilename, **keys)

    name = me_basename(tilename, band, ftype, **keys)
    name = path_join(dir, name)
    return name

def generate_me_output_urls(merun, tilename, band, **keys):
    """
    This is the files output by multishear
    """

    fdict={}

    rc=deswl.files.Runconfig()

    # output file names
    for ftype in rc.me_filetypes:
        name= me_url(merun, tilename, band, ftype, **keys)
        fdict[ftype] = name


    return fdict


#
# generates the input image/catalog urls using the database. Generates
# the single epoch # list url using me_seinputs_url
#
# gets the wl config from the runconfig
#
# gets the output files from generate_me_output_urls
#
class MultishearFiles:
    """
    Generate the input file names, output file names, and condor file names
    """
    def __init__(self, merun, fs=None, conn=None):
        if fs is None:
            fs=desdb.files.get_default_fs()
        self.merun=merun
        self.rc=Runconfig(self.merun)
        self.fs=fs
        if conn is None:
            import desdb
            self.conn=desdb.Connection()
        else:
            self.conn=conn


    def get_flist(self):
        query="""
        select
            id,tilename,band
        from
            %(release)s_files
        where
            filetype='coadd'
            and band='%(band)s'\n""" % {'release':self.rc['dataset'],
                                        'band':self.rc['band']}
        print query
        res=self.conn.quick(query)
        if len(res) == 1:
            raise ValueError("Found no coadds for release %s "
                             "band %s" % (self.rc['dataset'],self.rc['band']))

        print 'getting all the file sets'
        all_fdicts=[]
        for r in res:
            print '%s-%s' % (r['tilename'],r['band'])
            id=r['id']
            fdict=self.get_files(id=id)
            all_fdicts.append(fdict)
        return all_fdicts

    def get_files(self, 
                  id=None, 
                  tilename=None):
        """
        Call with
            f=get_files(id=)
        or
            f=get_files(tilename=)
        """
        import desdb

        # first the coadd image and catalog
        c=desdb.files.Coadd(id=id, 
                            band=self.rc['band'], 
                            dataset=self.rc['dataset'], 
                            tilename=tilename, 
                            fs=self.fs,
                            conn=self.conn)
        c.load()

        # now get the output files
        outfiles=generate_me_output_urls(self.merun,
                                         c['tilename'], 
                                         self.rc['band'], 
                                         fs=self.fs)

        files={}
        files['wl_config'] = os.path.expandvars(self.rc['wl_config'])
        files['image'] = c['image_url']
        files['cat'] = os.path.expandvars(c['cat_url'])
        files['id'] = int(c['image_id'])
        files['tilename'] = c['tilename']

        srclist = me_seinputs_url(self.merun, c['tilename'], self.rc['band'])
        files['srclist'] = os.path.expandvars(srclist)

        for k in outfiles:
            files[k] = outfiles[k]

        return files

#
#
# these are lists of srcfile,shearfile,fitpsffile
# for input to multishear
#

def me_seinputs_dir(merun):
    rc=Runconfig(merun)
    d=os.path.join('$DES_FILE_LISTS',
                   rc['dataset'],
                   'multishear-inputs-%s' % merun)
    return d

def me_seinputs_url(merun, tilename, band):
    d=me_seinputs_dir(merun)

    inputs=[tilename,band,merun,'inputs']
    inputs='-'.join(inputs)+'.dat'
    return path_join(d, inputs)

class MultishearSEInputs:
    """
    This is the single epoch inputs, use MultishearInputs to just generate
    the input file names for multishear.
    """
    def __init__(self, merun, conn=None, fs=None):
        if fs is None:
            fs=desdb.files.get_default_fs()
        self.merun=merun
        self.rc=Runconfig(self.merun)
        self.fs=fs

        if conn is None:
            import desdb
            self.conn=desdb.Connection()
        else:
            self.conn=conn


    def generate_all_inputs(self):
        """
        Generate all inputs for the input merun
        """
        query="""
        select
            id
        from
            %(release)s_files
        where
            filetype='coadd'
            and band='%(band)s'\n""" % {'release':self.rc['dataset'],
                                       'band':self.rc['band']}
        res=self.conn.quick(query)
        if len(res) == 1:
            raise ValueError("Found no coadds for release %s "
                             "band %s" % (self.rc['dataset'],self.rc['band']))

        for r in res:
            id=r['id']
            self.generate_inputs(id=id)
       
    def generate_inputs(self, id=None, tilename=None):
        import desdb
        if tilename is None and id is None:
            raise ValueError("Send tilename or id")
        if id is None:
            id=self.get_id(tilename)
        elif tilename is None:
            tilename=self.get_tilename(id)

        c=desdb.files.Coadd(id=id, conn=self.conn, fs=self.fs)
        c.load(srclist=True)

        url=self.get_url(tilename=tilename)
        url=os.path.expandvars(url)
        d=os.path.dirname(url)
        if not os.path.exists(d):
            os.makedirs(d)

        print 'writing me inputs:',url
        with open(url,'w') as fobj:
            for s in c.srclist:
                names=generate_se_filenames(self.rc['serun'],
                                            s['expname'],
                                            s['ccd'],
                                            fs=self.fs)

                line = '%s %s %s\n' % (s['url'],names['shear'],names['fitpsf'])
                fobj.write(line)

    def get_url(self, id=None, tilename=None):
        """
        Get the location of the single-epoch input list for the input id or
        tilename.
        """
        
        if tilename is None and id is None:
            raise ValueError("Send tilename or id")
        if tilename is None:
            tilename=self.get_tilename(id)
        url=me_seinputs_url(self.merun, tilename, self.rc['band'])
        return url

    def get_tilename(self, id):
        query="""
        select
            tilename
        from
            coadd
        where
            id=%(id)d\n""" % {'id':id}

        res=self.conn.quick(query)
        if len(res) != 1:
            raise ValueError("Expected a single match for id "
                             "%s, found %s" % (id,len(res)))
        return res[0]['tilename']

    def get_id(self, tile):
        query="""
        select
            id
        from
            %(release)s_files
        where
            filetype='coadd'
            and tilename='%(tile)s'
            and band='%(band)s'\n""" % {'tile':tile,
                                        'band':self.rc['band'],
                                        'release':self.rc['dataset']}
        res=self.conn.quick(query)
        if len(res) != 1:
            raise ValueError("Expected a single match for tilename "
                             "%s, found %s" % (tile,len(res)))
        return res[0]['id']

class HDFSSrclist:
    def __init__(self, orig_url):
        import tempfile
        self.orig_url=orig_url
        
        self.hdfs_files=[]

        bname=os.path.basename(self.orig_url)
        self.scratch_dir=desdb.files.get_scratch_dir()
        self.name = tempfile.mktemp(prefix='hdfs-', suffix='-'+bname, dir=self.scratch_dir)

    def stage(self):
        """
        Stage the files to local disk and write a new file with these filenames
        """

        if not os.path.exists(self.scratch_dir):
            os.makedirs(self.scratch_dir)

        self.hdfs_files=[]
        print >>stderr,'working through original:',self.orig_url
        with open(self.orig_url) as orig, open(self.name,'w') as new:
            lines=orig.readlines()
            nl=len(lines)
            for i,line in enumerate(lines,1):
                image,shear,fitpsf = line.split()

                t={}
                t['image'] = eu.hdfs.HDFSFile(image,verbose=True)
                t['shear'] = eu.hdfs.HDFSFile(shear,verbose=True)
                t['fitpsf'] = eu.hdfs.HDFSFile(fitpsf,verbose=True)

                self.hdfs_files.append(t)

                new.write('%s %s %s\n' % (t['image'].localfile,
                                          t['shear'].localfile,
                                          t['fitpsf'].localfile))

                print >>stderr,'%s/%s' % (i,nl)
                for k in t:
                    stderr.write('    ')
                    t[k].stage()

    def cleanup(self):
        """
        run cleanup on all staged files and cleanup temporary srclist file
        """

        if os.path.exists(self.name):
            print >>stderr,"Cleaning up temp srclist file:",self.name
            os.remove(self.name)

        for fd in self.hdfs_files:
            for k in ['image','shear','fitpsf']:
                fd[k].cleanup()

    def __enter__(self):
        return self
    def __exit__(self, exception_type, exception_value, traceback):
        self.cleanup()
    def __del__(self):
        self.cleanup()



class MultishearCondorJob(dict):
    """
    You most certainly want to run the script
        generate-me-condor merun
    Instead of use this directly
    """
    def __init__(self, merun, 
                 files=None,
                 id=None, 
                 tilename=None, 
                 nthread=None, 
                 dir=None,
                 conn=None,
                 verbose=False,
                 dryrun=False):
        """
        Note merun implies a dataset which, combined with tilename,
        implies the band.

        Or send files= (get_files from MultishearFiles) for quicker 
        results, no sql calls required.
        """
        if id is None and tilename is None and files is None:
            raise ValueError("Send id= or tilename= or files=")

        self.rc=Runconfig(merun)
        self['dataset'] = self.rc['dataset']
        self['band'] = self.rc['band']

        self['run'] = merun
        self['nthread'] = nthread
        self['verbose']=verbose
        self['dryrun']=dryrun

        if files is not None:
            self['config'] = files
        else:
            mfobj=MultishearFiles(merun, conn=conn)
            self['config'] = mfobj.get_files(id=id, tilename=tilename,dir=dir)

        self['id'] = self['config']['id']
        self['tilename'] = self['config']['tilename']

        #self['config']['nodots']=True
        self['config']['merun']=merun

        self['config_file']=me_config_path(self['run'],self['tilename'],self['band'])
        self['script_file']=me_script_path(self['run'],self['tilename'],self['band'])
        self['condor_file']=me_condor_path(self['run'],self['tilename'],self['band'])

    def write_condor(self):
        self.make_condor_dir()

        print "writing to condor submit file:",self['condor_file']
        text = self.condor_text()
        if self['verbose'] or self['dryrun']:
            print text 
        if not self['dryrun']:
            with open(self['condor_file'],'w') as fobj:
                fobj.write(text)
        else:
            print "this is just a dry run" 

    def write_script(self):
        self.make_condor_dir()

        print "writing to condor script file:",self['script_file']
        text = self.script_text()
        if self['verbose'] or self['dryrun']:
            print text 
        if not self['dryrun']:
            with open(self['script_file'],'w') as fobj:
                fobj.write(text)

            print "changing mode of file to executable"
            os.popen('chmod 755 '+self['script_file'])
        else:
            print "this is just a dry run" 


    def write_config(self):
        """
        This is the config meaning the list of files and parameters
        for input to multishear, not the wl config
        """
        self.make_condor_dir()

        if self['verbose'] or self['dryrun']:
            print "writing to config file:",self['config_file']

        if not self['dryrun']:
            with open(self['config_file'],'w') as fobj:
                eu.json_util.write(self['config'], fobj)

    def condor_text(self):
        condor_dir = self.condor_dir()
        script_base = me_script_base(self['tilename'],self['band'])
        condor_text="""
Universe        = parallel
Notification    = Error
GetEnv          = True
Notify_user     = esheldon@bnl.gov
+Experiment     = "astro"
Requirements    = (CPU_Experiment == "astro") && (TotalSlots == 12 || TotalSlots == 8)
Initialdir      = {condor_dir}

Executable      = {script_base}.sh
Output          = {script_base}.out
Error           = {script_base}.err
Log             = {script_base}.log

Queue\n""".format(condor_dir=condor_dir, script_base=script_base)

        return condor_text


    def script_text(self):
        rc=self.rc
        wl_load = _make_load_command('wl',rc['deswl_vers'])
        tmv_load = _make_load_command('tmv',rc['tmvvers'])
        esutil_load = _make_load_command('esutil', rc['esutil_vers'])
     
        thread_text=''
        if self['nthread'] is not None:
            thread_text='export OMP_NUM_THREADS=%s' % self['nthread']

        script_text="""#!/bin/bash
source ~astrodat/setup/setup.sh
source ~astrodat/setup/setup-modules.sh
module load use.own
{tmv_load}
{wl_load}
{esutil_load}
module load desfiles

{thread_text}
multishear-run -c {config_file}
""".format(wl_load=wl_load, 
           tmv_load=tmv_load, 
           esutil_load=esutil_load,
           thread_text=thread_text,
           config_file=os.path.basename(self['config_file']))

        return script_text

    def condor_dir(self):
        return condor_dir(self['run'])

    def make_condor_dir(self):
        condor_dir = self.condor_dir()
        if not os.path.exists(condor_dir):
            print 'making output dir',condor_dir
            os.makedirs(condor_dir)


class MultishearWQJob(dict):
    """
    You most certainly want to run the script
        generate-me-wq merun
    Instead of use this directly
    """
    def __init__(self, merun, 
                 files=None,
                 id=None, 
                 tilename=None, 
                 nthread=None, 
                 fs=None,
                 conn=None,
                 groups=None,
                 verbose=False):
        """
        Note merun implies a dataset which, combined with tilename,
        implies the band.

        Or send files= (get_files from MultishearFiles) for quicker 
        results, no sql calls required.
        """
        if fs is None:
            fs=desdb.files.get_default_fs()
        if id is None and tilename is None and files is None:
            raise ValueError("Send id= or tilename= or files=")

        self.rc=Runconfig(merun)
        self['dataset'] = self.rc['dataset']
        self['band'] = self.rc['band']

        self['run'] = merun
        self['nthread'] = nthread
        self['verbose']=verbose

        self['fs'] = fs

        if files is not None:
            self['config'] = files
        else:
            mfobj=MultishearFiles(merun, conn=conn, fs=self['fs'])
            self['config'] = mfobj.get_files(id=id, tilename=tilename)

        self['id'] = self['config']['id']
        self['tilename'] = self['config']['tilename']

        #self['config']['nodots']=True
        self['config']['merun']=merun

        self['job_file']=me_wq_path(self['run'],self['tilename'])
        self['check_job_file']=self['job_file'].replace('.yaml','-check.yaml')
        self['config_file']=me_config_path(self['run'],self['tilename'])
        self['log_file']=os.path.basename(self['job_file']).replace('yaml','out')


        if groups is None:
            groups = '[new,new2]'
        else:
            groups = '['+groups+']'
        self['groups'] = groups

    def write(self):
        self.write_config()
        self.write_job_file()
        self.write_job_file(check=True)

    def write_job_file(self, check=False):
        if check:
            job_file=self['check_job_file']
            text = self.check_job_file_text()
        else:
            job_file=self['job_file']
            text = self.job_file_text()
        eu.ostools.makedirs_fromfile(job_file)

        if self['verbose']:
            print >>stderr,"writing to wq job file:",job_file
        with open(job_file,'w') as fobj:
            fobj.write(text)


    def write_config(self):
        """
        This is the config meaning the list of files and parameters
        for input to multishear, not the wl config
        """
        import yaml
        eu.ostools.makedirs_fromfile(self['config_file'])

        if self['verbose']:
            print "writing to config file:",self['config_file']

        with open(self['config_file'],'w') as fobj:
            for k in self['config']:
                # I want it a bit prettier so writing my own, but will have
                # to be careful
                kk = k+': '
                val = self['config'][k]
                if isinstance(val,bool):
                    if val:
                        val='true'
                    else:
                        val='false'
                fobj.write('%-15s %s\n' % (kk,val))
            #yaml.dump(self['config'], fobj)


    def job_file_text(self):
        job_name=self['tilename'] + '-'+self['band']
        groups = self['groups']

        rc=self.rc
        wl_load = _make_load_command('wl',rc['deswl_vers'])
        tmv_load = _make_load_command('tmv',rc['tmvvers'])
        esutil_load = _make_load_command('esutil', rc['esutil_vers'])
     
        thread_text='\n'
        if self['nthread'] is not None:
            thread_text='\nexport OMP_NUM_THREADS=%s' % self['nthread']

        text="""
command: |
    source /opt/astro/SL53/bin/setup.hadoop.sh
    source ~astrodat/setup/setup.sh
    source ~/.dotfiles/bash/astro.bnl.gov/modules.sh
    {esutil_load}
    {tmv_load}
    {wl_load}
    {thread_text}
    multishear-run {config_file} &> {log_file}

group: {groups}
mode: bynode
priority: low
job_name: {job_name}\n""".format(wl_load=wl_load, 
                                 esutil_load=esutil_load,
                                 tmv_load=tmv_load, 
                                 thread_text=thread_text,
                                 config_file=os.path.basename(self['config_file']),
                                 log_file=self['log_file'],
                                 groups=groups, 
                                 job_name=job_name)

        return text



    def check_job_file_text(self):

        job_name=self['tilename'] + '-'+self['band']
        rc=self.rc

        esutil_load = _make_load_command('esutil', rc['esutil_vers'])
        wl_load = _make_load_command('wl',rc['deswl_vers'])
        tmv_load = _make_load_command('tmv',rc['tmvvers'])

        stat=self['config']['stat']
        cmd="""


        """.format(stat=stat,job_name=job_name).strip()

        text = """
command: |
    source /opt/astro/SL53/bin/setup.hadoop.sh
    source ~astrodat/setup/setup.sh
    source ~/.dotfiles/bash/astro.bnl.gov/modules.sh
    {esutil_load}
    {tmv_load}
    {wl_load}

    stat={stat}
    chk={job_name}-check.json
    err={job_name}-check.err
    shear-check-one $stat 1> $chk 2> $err

priority: low
job_name: {job_name}\n""".format(esutil_load=esutil_load,
                                   tmv_load=tmv_load,
                                   wl_load=wl_load,
                                   stat=stat,
                                   job_name=job_name)


        return text





class ShearFiles(dict):
    def __init__(self, serun, conn=None, fs=None):
        if fs is None:
            fs=desdb.files.get_default_fs()
        self['run'] = serun
        self['fs'] = fs

        self.rc = Runconfig(self['run'])
        self['dataset'] = self.rc['dataset']

        self.conn=conn
        if self.conn is None:
            self.conn=desdb.Connection()

        self.expnames = None


    def get_expnames(self):
        if self.expnames is None:
            import desdb
            self.expnames = desdb.files.get_expnames(self.rc['dataset'],
                                                     self.rc['band'])
        return self.expnames

    def get_flist(self, by_expname=False):
        """

        Get the red info from the database and add in the file info for the wl
        outputs

        If by_expname=True, return a dict keyed by exposure name

        """

        import desdb
        infolist=desdb.files.get_red_info(self.rc['dataset'],self.rc['band'])
        for info in infolist:
            # rename to be consistent
            image=info.pop('image_url')
            cat=info.pop('cat_url')
            info['image'] = image
            info['cat'] = cat

            fdict=deswl.files.generate_se_filenames(self['run'],
                                                    info['expname'],
                                                    info['ccd'],
                                                    fs=self['fs'])
            info['wl_config'] = self.rc['wl_config']
            for k,v in fdict.iteritems():
                info[k] = v

            for k,v in info.iteritems():
                if isinstance(v,basestring):
                    info[k] = expand_desvars(v, fs=self['fs'])
            # expand environment variables
            for k,v in info.iteritems():
                if isinstance(v,basestring):
                    info[k] = os.path.expandvars(v)


            info['serun'] = self['run']

        if by_expname:
            d={}
            for info in infolist:
                expname=info['expname']
                el=d.get(expname,None)
                if el is None:
                    d[expname] = [info]
                else:
                    # should modify in place
                    el.append(info)
                    if len(el) > 62:
                        pprint.pprint(el)
                        raise ValueError("%s grown beyond 62, why?" % expname)

            return d
        else:
            return infolist

    def convert_to_hdfs(self, infolist):
        """
        Modifies in place
        """
        nfs_root=desdb.files.get_des_rootdir()
        hdfs_root=hdfs_rootdir()
        for info in infolist:
            for k,v in info.iteritems():
                if isinstance(v,basestring):
                    info[k] = v.replace(nfs_root,hdfs_root)

    def get_flist_old(self):
        expnames=self.get_expnames()
        flist = []
        for expname in expnames:
            for ccd in xrange(1,62+1):
                fdict=deswl.files.generate_se_filenames(self['run'],expname,ccd)
                fdict['expname'] = expname
                fdict['ccd'] = ccd
                flist.append(fdict)
        return flist



class SEWQJob(dict):
    def __init__(self, serun, files, **keys):
        """
        For now just take files for a given ccd or list of 62 ccds

        Files should be an element from ShearFiles get_flist(), which returns a
        list of dicts
        """

        if not isinstance(files,list):
            files=[files]
        nf=len(files)
        if nf != 1 and nf != 62:
            print >>stderr,'files[0]:',files[0]
            raise ValueError("Expect either 1 or 62 inputs, got %s" % nf)

        self['serun'] = serun
        self['keys'] = keys
        self['type'] = keys.get('type','fullpipe')
        self['verbose'] = keys.get('verbose',False)
        self['debug_level'] = keys.get('debug_level',-1)
        self['groups'] = keys.get('groups',None)

        for c in files:
            c['serun'] = self['serun']
            c['type'] = self['type']
            c['debug_level'] = self['debug_level']

        self['flist'] = files

    def write(self):
        """
        Always call this when doing multiple ccds
        """
        import copy
        # config files get written individually
        keys=copy.deepcopy(self['keys'])
        nf=len(self['flist'])
        if nf > 1:
            keys['verbose'] = False
        for fdict in self['flist']:
            sj=SEWQJob(self['serun'],fdict, **keys)
            sj._write_config()

        self._write_job_file()
        self._write_job_file(check=True)

    def _write_job_file(self, check=False):
        if check:
            job_file=self._get_job_filename(check=True)
            text = self._check_job_file_text()
        else:
            job_file=self._get_job_filename()
            text = self._job_file_text()

        eu.ostools.makedirs_fromfile(job_file)

        if self['verbose']:
            print "writing to wq job file:",job_file

        with open(job_file,'w') as fobj:
            fobj.write(text)


    def _job_file_text(self):

        groups = self['groups']
        expname=self['flist'][0]['expname']

        rc=Runconfig(self['serun'])
        wl_load = _make_load_command('wl',rc['deswl_vers'])
        tmv_load = _make_load_command('tmv',rc['tmvvers'])
        esutil_load = _make_load_command('esutil', rc['esutil_vers'])
     
        if len(self['flist']) == 62:
            job_name=expname
        else:
            job_name='%s-%02i' % (expname,self['flist'][0]['ccd'])

        cmd=[]
        for fd in self['flist']:
            config_file=se_config_path(self['serun'],
                                       fd['expname'],
                                       type=self['type'],
                                       ccd=fd['ccd'])
            config_file=os.path.basename(config_file)

            # When doing a full exposure, our job file will be lower in the
            # tree from both config and the log file
            if len(self['flist']) == 62:
                config_file='byccd/'+config_file

            log_file=config_file.replace('-config.yaml','.out')

            c="shear-run %s &> %s" % (config_file,log_file)
            cmd.append(c)

        cmd='\n    '.join(cmd) 

        if groups is None:
            groups=''
        else:
            groups='group: [' + groups +']'

        text = """
command: |
    source /opt/astro/SL53/bin/setup.hadoop.sh
    source ~astrodat/setup/setup.sh
    source ~/.dotfiles/bash/astro.bnl.gov/modules.sh
    %(esutil_load)s
    %(tmv_load)s
    %(wl_load)s

    export OMP_NUM_THREADS=1
    %(cmd)s

%(groups)s
priority: low
job_name: %(job_name)s\n""" % {'esutil_load':esutil_load,
                               'tmv_load':tmv_load,
                               'wl_load':wl_load,
                               'cmd':cmd,
                               'groups':groups,
                               'job_name':job_name}


        return text

    def _check_job_file_text(self):

        expname=self['flist'][0]['expname']

        rc=Runconfig(self['serun'])
        esutil_load = _make_load_command('esutil', rc['esutil_vers'])
        wl_load = _make_load_command('wl',rc['deswl_vers'])
        tmv_load = _make_load_command('tmv',rc['tmvvers'])

        if len(self['flist']) == 62:
            job_name='chk-%s' % expname
        else:
            job_name='chk-%s-%02i' % (expname,self['flist'][0]['ccd'])

        if len(self['flist']) == 1:
            fd=self['flist'][0]
            stat=fd['stat']
            ccd=fd['ccd']
            chk="byccd/%(expname)s-%(ccd)02i-check.json" % (expname,ccd)
            err="byccd/%(expname)s-%(ccd)02i-check.err" % (expname,ccd)
            cmd="shear-check-one %s 1> %s 2> %s" % (stat,chk,err)
        else:

            fd=self['flist'][0]
            stat0=fd['stat']
            rep='%02i-stat.json' % fd['ccd']
            stat=stat0.replace(rep,'$i-stat.json')
            #print 'expname:',expname,'rep:',rep
            #print stat0,'\n',stat

            cmd="""
    for i in `seq -w 1 62`; do
        stat=%(stat)s
        chk=byccd/%(expname)s-$i-check.json
        err=byccd/%(expname)s-$i-check.err
        echo $chk
        shear-check-one $stat 1> $chk 2> $err
    done""" % {'stat':stat,'expname':expname}

            cmd=cmd.strip()


        text = """
command: |
    source /opt/astro/SL53/bin/setup.hadoop.sh
    source ~astrodat/setup/setup.sh
    source ~/.dotfiles/bash/astro.bnl.gov/modules.sh
    %(esutil_load)s
    %(tmv_load)s
    %(wl_load)s

    %(cmd)s

priority: low
job_name: %(job_name)s\n""" % {'esutil_load':esutil_load,
                               'tmv_load':tmv_load,
                               'wl_load':wl_load,
                               'cmd':cmd,
                               'job_name':job_name}


        return text


    def _write_config(self):
        """
        This is the config meaning the list of files and parameters
        for input to multishear, not the wl config
        """
        import yaml

        if len(self['flist']) > 1:
            raise ValueError("Only call write_config when working on single ccd")

        fd=self['flist'][0]
        config_file=se_config_path(self['serun'],
                                   fd['expname'],
                                   type=self['type'],
                                   ccd=fd['ccd'])

        eu.ostools.makedirs_fromfile(config_file)

        if self['verbose']:
            print "writing to config file:",config_file

        with open(config_file,'w') as fobj:
            for k in fd:
                # I want it a bit prettier so writing my own, but will have to
                # be careful
                kk = k+': '
                val = fd[k]
                if isinstance(val,bool):
                    if val:
                        val='true'
                    else:
                        val='false'
                fobj.write('%-15s %s\n' % (kk,val))


    def _get_job_filename(self, check=False):
        if len(self['flist']) == 1:
            files=self['flist'][0]
            job_file= se_wq_path(self['serun'],
                                 files['expname'],
                                 type=self['type'],
                                 ccd=files['ccd'])
        else:
            files=self['flist'][0]
            job_file= se_wq_path(self['serun'],
                                 files['expname'],
                                 type=self['type'])
        if check:
            job_file=job_file.replace('.yaml','-check.yaml')
        return job_file

def _make_load_command(modname, vers):
    """
    convention is that trunk is exported to ~/exports/modname-work
    """
    load_command = \
        "module unload {modname} && module load {modname}".format(modname=modname)
    if vers == 'trunk':
        load_command += '/work'
    else:
        load_command += '/%s' % vers

    return load_command




#
# old pbs scripts for running a tile. These will be used by condor
#



def get_pbs_dir(run, subdir=None):
    rootdir=desdb.files.get_des_rootdir()
    outdir=path_join(rootdir,'pbs',run)
    if subdir is not None:
        outdir=path_join(outdir, subdir)
    outdir=os.path.expanduser(outdir)
    return outdir

def get_mpibatch_pbs_file(run):
    d=get_pbs_dir(run)
    f='%s-mpibatch.pbs' % run
    f=os.path.join(d,f)
    return f

def get_mpibatch_check_pbs_file(run):
    d=get_pbs_dir(run)
    f='%s-check-mpibatch.pbs' % run
    f=os.path.join(d,f)
    return f


# no longer used
"""
def get_mpibatch_cmds_file(run):
    d=get_pbs_dir(run)
    f='%s-mpibatch-cmds.txt' % run
    f=os.path.join(d,f)
    return f
"""
def get_exp_mpibatch_pbs_file(run, expname):
    d=get_pbs_dir(run, subdir='byexp')
    f='%s-mpibatch.pbs' % expname
    f=os.path.join(d,f)
    return f

"""
def get_exp_mpibatch_cmds_file(run,expname):
    d=get_pbs_dir(run, subdir='byexp')
    f='%s-mpibatch-cmds.txt' % expname
    f=os.path.join(d,f)
    return f
"""


def get_me_pbs_name(tilename, band):
    pbsfile=[tilename,band]
    pbsfile='-'.join(pbsfile)+'.pbs'
    return pbsfile
    
def get_me_pbs_path(merun, tilename, band):
    pbsdir=get_pbs_dir(merun)
    pbsfile=get_me_pbs_name(tilename,band)
    pbsfile=path_join(pbsdir, pbsfile)
    return pbsfile


def get_se_pbs_name(expname, typ='fullpipe', ccd=None):
    pbsfile=[expname]

    if typ != 'fullpipe':
        pbsfile.append(typ)

    if ccd is not None:
        pbsfile.append('%02i' % int(ccd))
    pbsfile='-'.join(pbsfile)+'.pbs'
    return pbsfile
    
def get_se_pbs_path(serun, expname, typ='fullpipe', ccd=None):
    if ccd is not None:
        subdir='byccd'
    else:
        subdir='byexp'

    pbsdir=get_pbs_dir(serun, subdir=subdir)
    pbsfile=get_se_pbs_name(expname, typ=typ, ccd=ccd)
    pbsfile=path_join(pbsdir, pbsfile)
    return pbsfile


def get_se_config_path(run, expname, typ='fullpipe', ccd=None):
    f=get_se_pbs_path(run, expname, typ=typ, ccd=ccd)
    f=f[0:f.rfind('.')]+'-config.yaml'
    return f

def get_se_log_path(run, expname, typ='fullpipe', ccd=None):
    f=get_se_pbs_path(run, expname, typ=typ, ccd=ccd)
    f=f[0:f.rfind('.')]+'.log'
    return f


def get_se_script_path(run, expname, typ='fullpipe', ccd=None):
    f=get_se_pbs_path(run, expname, typ=typ, ccd=ccd)
    f=f[0:f.rfind('.')]+'.sh'
    return f

def get_se_mpiscript_path(run, expname, typ='fullpipe', ccd=None):
    """
    For calling deswl-run from mpi workers. Sets up environment.
    """
    f=get_se_pbs_path(run, expname, typ=typ, ccd=ccd)
    f=f[0:f.rfind('.')]+'-mpi.sh'
    return f




#
# new wq stuff
#

def wq_dir(run, subdir=None):
    outdir=path_join('~','des-wq',run)
    if subdir is not None:
        outdir=path_join(outdir, subdir)
    outdir=os.path.expanduser(outdir)
    return outdir


def se_wq_path(run, expname, type='fullpipe', ccd=None):
    wqfile=[expname]

    if type != 'fullpipe':
        wqfile.append(type)

    if ccd is not None:
        wqfile.append('%02i' % int(ccd))

    wqfile='-'.join(wqfile)+'.yaml'

    if ccd is not None:
        subdir='byccd'
    else:
        subdir=None

    d = wq_dir(run, subdir=subdir)

    wqpath=path_join(d, wqfile)
    return wqpath

def se_config_path(run, expname, type='fullpipe', ccd=None):
    """
    This is the file with all the paths and parameters,
    not a wl_config
    """
    
    f=se_wq_path(run,expname,type=type,ccd=ccd)
    f=f[0:f.rfind('.')]+'-config.yaml'
    return f



def me_wq_path(run, tilename):
    rc = Runconfig(run)
    band = rc['band']


    f=[tilename,band]
    f='-'.join(f)+'.yaml'

    d = wq_dir(run)

    wqpath=path_join(d, f)
    return wqpath

def me_config_path(merun, tilename):
    """
    This is the file with all the paths and parameters,
    not a wl_config
    """
    
    f=me_wq_path(merun, tilename)
    f=f[0:f.rfind('.')]+'-config.yaml'
    return f



#
# condor submit files and bash scripts

def condor_dir(run, subdir=None):
    outdir=path_join('~','condor','wl',run)
    if subdir is not None:
        outdir=path_join(outdir, subdir)
    outdir=os.path.expanduser(outdir)
    return outdir


def me_condor_name(tilename, band):
    condorfile=[tilename,band]
    condorfile='-'.join(condorfile)+'.condor'
    return condorfile
    
def me_condor_path(merun, tilename, band):
    condordir=condor_dir(merun)
    condorfile=me_condor_name(tilename,band)
    condorfile=path_join(condordir, condorfile)
    return condorfile


def se_condor_name(expname, typ='fullpipe', ccd=None):
    condorfile=[expname]

    if typ != 'fullpipe':
        condorfile.append(typ)

    if ccd is not None:
        condorfile.append('%02i' % int(ccd))
    condorfile='-'.join(condorfile)+'.condor'
    return condorfile
    
def se_condor_path(serun, expname, typ='fullpipe', ccd=None):
    if ccd is not None:
        subdir='byccd'
    else:
        subdir=None

    condordir=condor_dir(serun, subdir=subdir)
    condorfile=se_condor_name(expname, typ=typ, ccd=ccd)
    condorfile=path_join(condordir, condorfile)
    return condorfile



def me_script_base(tilename, band):
    script_parts=[tilename,band]
    script_base='-'.join(script_parts)
    return script_base

def me_script_name(tilename, band):
    script_base = me_script_base(tilename, band)
    script_name = script_base+'.sh'
    return script_name
    
def me_script_path(merun, tilename, band):
    scriptdir=condor_dir(merun)
    scriptfile=me_script_name(tilename,band)
    scriptfile=path_join(scriptdir, scriptfile)
    return scriptfile




def se_script_base(expname, typ='fullpipe', ccd=None):
    script_parts=[expname]

    if typ != 'fullpipe':
        script_parts.append(typ)

    if ccd is not None:
        script_parts.append('%02i' % int(ccd))
    script_base='-'.join(script_parts)
    return script_base

def se_script_name(expname, typ='fullpipe', ccd=None):
    script_base = se_script_base(expname, typ=typ, ccd=ccd)
    script_name = script_base+'.sh'
    return script_name
    
def se_script_path(serun, expname, typ='fullpipe', ccd=None):
    if ccd is not None:
        subdir='byccd'
    else:
        subdir=None

    scriptdir=condor_dir(serun, subdir=subdir)
    scriptfile=se_script_name(expname, typ=typ, ccd=ccd)
    scriptfile=path_join(scriptdir, scriptfile)
    return scriptfile












def get_info_from_path(filepath, fileclass):
    """
    This is a more extensive version get_info_from_image_path.  Also works for
    the cat files.  If fileclass='coadd' then treated differently
    """

    odict = {}
    # remove the extension
    dotloc=filepath.find('.')
    
    odict['ext'] = filepath[dotloc:]
    root=filepath[0:dotloc]



    DESDATA=getenv_check('DESDATA')
    if DESDATA[-1] == os.sep:
        DESDATA=DESDATA[0:len(DESDATA)-1]

    endf = filepath.replace(DESDATA, '')


    odict['filename'] = filepath
    odict['root'] = root
    odict['DESDATA'] = DESDATA


    fsplit = endf.split(os.sep)


    if fileclass == 'red':
        nparts = 6
        if len(fsplit) < nparts:
            tmp=(len(fsplit), nparts, filepath)
            mess="Found too few file elements, %s instead of %s: '%s'\n" % tmp
            raise ValueError(mess)

        if fsplit[1] != 'red' or fsplit[3] != 'red':
            mess="In file path %s expected 'red' in positions "+\
                "1 and 3 after DESDATA=%s" % (filepath,DESDATA)
            raise ValueError(mess)

        odict['redrun'] = fsplit[2]
        odict['expname'] = fsplit[4]
        odict['basename'] = fsplit[5]

        base=os.path.basename(odict['root'])

        if base[-4:] == '_cat':
            base=base[:-4]

        # last 3 bytes are _[ccd number]
        odict['ccd']=int(base[-2:])
        base=base[0:len(base)-3]

        # next -[repeatnum].  not fixed length so we must go by the dash
        dashloc=base.rfind('-')
        odict['repeatnum'] = base[dashloc+1:]
        base=base[0:dashloc]

        # now is -[band]
        dashloc=base.rfind('-')
        band=base[-1]
        odict['band'] = band
        base=base[0:dashloc]
        
        # now we have something like decam--24--11 or decam--24--9, so
        # just remove the decam-
        # might have to alter this some time if they change the names
        if base[0:6] != 'decam-':
            raise ValueError('Expected "decam-" at beginning')
        odict['pointing'] = base.replace('decam-','')

        
    elif fileclass == 'coadd':
        nparts = 5
        if len(fsplit) < nparts:
            mess= 'Found too few file elements, '+\
                '%s instead of %s: "%s"\n' % (len(fsplit), nparts, filepath)
            raise ValueError(mess)

        if fsplit[1] != 'coadd' or fsplit[3] != 'coadd':
            mess="In file path %s expected 'red' in positions "+\
                "1 and 3 after DESDATA=%s" % (filepath,DESDATA)
            raise ValueError(mess)


        odict['coaddrun'] = fsplit[2]
        odict['basename'] = fsplit[4]

        tmp = os.path.basename(root)
        if tmp[-3:] == 'cat':
            odict['coaddtype'] = 'cat'
            tmp = tmp[0:-4]
        else:
            odict['coaddtype'] = 'image'

        odict['band'] = tmp[-1]

        last_underscore = tmp.rfind('_')
        if last_underscore == -1:
            mess='Expected t find an underscore in basename of %s' % filepath
            raise ValueError(mess)
        odict['tilename'] = tmp[0:last_underscore]

    
    return odict


# utilities
def replacedir(oldfile, newdir):
    if oldfile is not None:
        fbase=os.path.basename(oldfile)
        newf = os.path.join(newdir, fbase)
        return newf
    else:
        return None


_FITS_EXTSTRIP=re.compile( '\.fits(\.fz)?$' )
def remove_fits_extension(fname):
    """
    See if the pattern exists and replace it with '' if it does
    """
    return _FITS_EXTSTRIP.sub('', fname)




