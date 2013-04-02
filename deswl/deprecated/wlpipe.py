"""
    deprecated

    This module is no longer used




    vim: set ft=python:
    Order of doing things for SE image processing

        I now use the database to figure out what SE files are available
        for a given *release*.  The release dr012 corresponds to DC6b.

        Use ~/python/desdb/bin/get-release-runs to get a list of runs
        and then use wget-des/wget-des-parallel to download them.

        Generate md5sums using ~/shell_scripts/checksum-files.  Then verify against 
          https://desar.cosmology.illinois.edu/DESFiles/desardata/Release/{release}/MD5SUM
        Here release is all caps.  Use 
            ~/python/des/bin/checksum-compare.py 
            or 
            ~/python/des/src/checksum-compare (compile it)
        for the comparison.




        # create a run name and its configuration
            rc=deswl.files.Runconfig()
            rc.generate_new_runconfig('se', dataset, band, wl_config)

        Note I've started setting wl_config here, instead of using what is
        under etc. This allows us to use the same version of code but run with
        a different configuration.  TODO: Move these into GIT!
        e.g. wl01.config
        
        You can send test=True and dryrun=True also

        # create the wq job files and submit script
        generate-se-wq se014it

        # after running you can check the results by submitting
        # the -check*yaml wq parallel check scripts, followed
        # by the check-reduce.py 

        # the reduced files written are these.
        deswl.files.collated_path(serun, 'goodlist')
        deswl.files.collated_path(serun, 'badlist')

        # collation also happens in parallel.
        # use this to generate the wq scripts which
        # go under ~/des-wq/run/collate
        ~/python/des/bin/make-collate-wq.py

        # it will also write a script for combining these
        # called {run}-combine.py  Don't forget to clean up
        # the sub-collated versions

        # then make the fits versions, and html using
        ~/python/des/bin/collate2cols.py --fits run
        ~/python/des/bin/collate2cols.py --html run


    For coadds, using multishear:
        First generate a run with a dataset/band/serun.  This will
        generate an merun and define everything we need.

            rc=deswl.files.Runconfig()
            rc.generate_new_runconfig('me', dataset, band, wl_config, serun=, test=)

        This script will then generate the single epoch input images and
        shear/fitpsf lists for input to each tile processing

            generate-me-seinputs merun

        
        This will generate the wq job files

            generate-me-wq merun

        # the rest is the same as for SE
"""

import sys
from sys import stdout,stderr
import traceback
import platform
import os
import glob
import subprocess
import time
import copy
import signal
import re
import datetime
import shutil
import pprint
import logging

import deswl

import esutil
import esutil as eu
from esutil.ostools import path_join, getenv_check
from esutil import json_util
from esutil.misc import ptime


class ImageProcessor(dict):
    def __init__(self, fdict):
        """
        fdict should include input and *all* output files, even
        those you won't use for this executable type.

        Should include 'type', e.g. 'fullpipe', 'findstars','measurepsf','measureshear' 
        """
        for k,v in fdict.iteritems():
            self[k]=v
        self['executable']= self['type']
        self['timeout'] = 2*60*60 # two hours

        self.use_hdfs=False
        if self['image'][0:4] == 'hdfs':
            self.use_hdfs=True

        self.setup_files()

    def run(self):
        try:
            print >>stderr,os.uname()[1]

            if self.use_hdfs:
                self.hdfs_stage()

            eu.ostools.makedirs_fromfile(self['qa'])
            command=self.get_command()
            # stdout will go to the qafile.
            # stderr is not directed

            stderr.write("opening qa file for output (stdout): %s\n" % self.outf['qa'])
            with open(self.outf['qa'],'w') as qafile:
                stderr.write("running command: \n%s\n" % '  \\\n\t'.join(command))
                exit_status, oret, eret = eu.ostools.exec_process(command,
                                                                  timeout=self['timeout'],
                                                                  stdout_file=qafile,
                                                                  stderr_file=None)

            self['exit_status'] = exit_status
            print >>stderr,'exit_status:',exit_status

            self.write_status()
        finally:
            if self.use_hdfs:
                self.hdfs_put()
                self.hdfs_cleanup()

        print >>stderr,'Done'


    def get_command(self):
        command=[self['executable'],
                 self['wl_config'],
                 'image_file='+self.inf['image'],
                 'cat_file='+self.inf['cat'],
                 'stars_file='+self.outf['stars'],
                 'fitpsf_file='+self.outf['fitpsf'],
                 'psf_file='+self.outf['psf'],
                 'shear_file='+self.outf['shear'] ]

        if 'output_dots' in self:
            if not self['output_dots']:
                command.append('output_dots=false')
        if 'output_info' in self:
            if not self['output_info']:
                command.append('output_info=false')

        if 'serun' in self:
            command.append('wlserun=%s' % self['serun'])

        if 'debug_level' in self:
            if self['debug_level'] >= 0:
                command.append('debug_file=%s' % self.outf['debug'])
                command.append('verbose=%s' % self['debug_level'])

        return command

    def write_status(self):
        """
        Add a bunch of new things to self and write self out as the stat file
        """
        print >>stderr,'writing status file:',self.outf['stat']
        eu.io.write(self.outf['stat'],self)

    def hdfs_stage(self):
        self.hdfs_inf['image'].stage()
        self.hdfs_inf['cat'].stage()

    def hdfs_cleanup(self):
        for k,v in self.hdfs_inf.iteritems():
            v.cleanup()

    def hdfs_put(self):
        for k,v in self.hdfs_outf.iteritems():
            if os.path.exists(v.localfile):
                # clobber existing files
                # performs cleanup
                v.put(force=True)

    def setup_files(self):
        inf={}
        outf={}
        in_types=['image','cat']
        out_types = ['qa','stat','stars','fitpsf','psf','shear','debug']
        if self.use_hdfs:
            hdfs_inf={}
            hdfs_outf={}

            for k in in_types:
                hdfs_inf[k] = eu.hdfs.HDFSFile(self[k],verbose=True)
                inf[k] = hdfs_inf[k].localfile
            for k in out_types:
                hdfs_outf[k] = eu.hdfs.HDFSFile(self[k],verbose=True)
                outf[k] = hdfs_outf[k].localfile

            self.hdfs_inf  = hdfs_inf
            self.hdfs_outf = hdfs_outf
        else:
            for k in in_types:
                inf[k] = self[k]
            for k in out_types:
                outf[k] = self[k]

        self.inf  = inf
        self.outf = outf


class CoaddTileProcessor(dict):
    """
    Very basic: Takes a dictionary "config" with at least
        wl_config
        image
        cat
        srclist
        multishear
        qa
        stat
    runs multishear on the specified files.  Note config here is different than
    the run config or the wl config, it's just a dict with enough info to
    process the image.

    If merun= is in the dict, it will be sent as wlmerun= and written
    to the header.

    If  nodots is in the dict, and True, then output_dots=false is sent.

    If debug_file is present the debug_file= is sent and verbose is set.  If
    debug is present verbose will have that value, otherwise 0 (same as no
    debug).

    Anything in config will be also be written to the stat file. 

    """
    def __init__(self, fdict):
        import desdb
        for k in fdict:
            self[k] = fdict[k]

        self['executable']= 'multishear'
        self['timeout'] = 2*60*60 # two hours

        self.use_hdfs=False
        if self['image'][0:4] == 'hdfs':
            self.use_hdfs=True

        self.setup_files()

        self.scratch_dir=desdb.files.get_scratch_dir()

    def run(self):
        try:
            print >>stderr,os.uname()[1]

            if self.use_hdfs:
                self.hdfs_stage()

            eu.ostools.makedirs_fromfile(self['qa'])
            command=self.get_command()
            # stdout will go to the qafile.
            # stderr is not directed

            stderr.write("opening qa file for output (stdout): %s\n" % self.outf['qa'])
            with open(self.outf['qa'],'w') as qafile:
                stderr.write("running command: \n%s\n" % '  \\\n\t'.join(command))
                exit_status, oret, eret = eu.ostools.exec_process(command,
                                                                  timeout=self['timeout'],
                                                                  stdout_file=qafile,
                                                                  stderr_file=None)

            self['exit_status'] = exit_status
            print 'exit_status:',exit_status

            self.write_status()
        finally:
            if self.use_hdfs:
                self.hdfs_put()
                self.hdfs_cleanup()

        print >>stderr,'Done'

    def get_command(self):

        command=[self['executable'],
                 self['wl_config'],
                 'coadd_srclist='+self.inf['srclist'], # srclist always on nfs
                 'coaddimage_file='+self.inf['image'],
                 'coaddcat_file='+self.inf['cat'],
                 'multishear_file='+self.outf['multishear'],
                 'multishear_sky_method=NEAREST']


        if 'output_dots' in self:
            if not self['output_dots']:
                command.append('output_dots=false')
        if 'output_info' in self:
            if not self['output_info']:
                command.append('output_info=false')

        if 'merun' in self:
            command.append('wlmerun=%s' % self['merun'])

        if 'debug_level' in self:
            if self['debug_level'] >= 0:
                command.append('debug_file=%s' % self.outf['debug'])
                command.append('verbose=%s' % self['debug_level'])

        if 'multishear_section_size' in self:
            command.append('multishear_section_size=%s' % self['multishear_section_size'])

        return command

    def write_status(self):
        """
        Add a bunch of new things to self and write self out as the stat file
        """
        print >>stderr,'writing status file:',self.outf['stat']
        eu.io.write(self.outf['stat'],self)


    def hdfs_stage(self):
        self.hdfs_inf['image'].stage()
        self.hdfs_inf['cat'].stage()
        self.hdfs_srclist.stage()

    def hdfs_cleanup(self):
        for k,v in self.hdfs_inf.iteritems():
            v.cleanup()

        self.hdfs_srclist.cleanup()

    def hdfs_put(self):
        for k,v in self.hdfs_outf.iteritems():
            if os.path.exists(v.localfile):
                # clobber existing files.
                # performs cleanup
                v.put(force=True)


    def setup_files(self):
        inf={}
        outf={}

        in_types=['image','cat']
        out_types = ['qa','stat','multishear','debug']
        if not os.path.exists(self.scratch_dir):
            os.makedirs(self.scratch_dir)

        if self.use_hdfs:
            hdfs_srclist=deswl.files.HDFSSrclist(self['srclist'])
            inf['srclist'] = hdfs_srclist.name

            hdfs_inf={}
            hdfs_outf={}

            for k in in_types:
                hdfs_inf[k] = eu.hdfs.HDFSFile(self[k],verbose=True)
                inf[k] = hdfs_inf[k].localfile
            for k in out_types:
                hdfs_outf[k] = eu.hdfs.HDFSFile(self[k],verbose=True)
                outf[k] = hdfs_outf[k].localfile

            self.hdfs_inf  = hdfs_inf
            self.hdfs_outf = hdfs_outf
            self.hdfs_srclist = hdfs_srclist
        else:
            inf['srclist'] = self['srclist']
            for k in in_types:
                inf[k] = self[k]
            for k in out_types:
                outf[k] = self[k]

        self.inf  = inf
        self.outf = outf



#
# deprecated
#

ERROR_SE_UNKNOWN=2**0
ERROR_SE_MISC=2**1
ERROR_SE_MISSING_FILE=2**2
ERROR_SE_LOAD_CONFIG=2**3
ERROR_SE_LOAD_FILES=2**4
ERROR_SE_FINDSTARS=2**5
ERROR_SE_MEASURE_PSF=2**6
ERROR_SE_MEASURE_SHEAR=2**7
ERROR_SE_SPLIT_STARS=2**8
ERROR_SE_IO=2**9
ERROR_SE_SET_LOG=2**10

'''
class ExposureProcessorOld:
    def __init__(self, expname, **keys):
        """
        Send serun= or dataset= to disambiguate

        We don't load anything in the constructor because we
        want to have state available and always write the
        status file.
        """
        self.all_types = ['stars','psf','shear','split']

        # things in stat will get written to the status QA
        # file
        self.stat = {}
        
        self.stat['expname']   = expname
        self.stat['serun']          = keys.get('serun',None)
        self.stat['dataset']        = keys.get('dataset',None)
        self.stat['config']         = keys.get('config',None)
        self.stat['nodots']         = keys.get('nodots',False)

        if self.stat['dataset'] is None and self.stat['serun'] is None:
            raise ValueError("send either dataset or serun")

        DESDATA = deswl.files.des_rootdir()
        self.stat['DESDATA'] = DESDATA

        self.stat['error']        = 0
        self.stat['error_string'] = ''

        self.stat['hostname'] = platform.node()
        self.stat['date'] = datetime.datetime.now().strftime("%Y-%m-%d-%X")

        self.runconfig = None

        self.logger = logging.getLogger('ExposureProcessor')
        log_level = keys.get('log_level',logging.ERROR)
        self.logger.setLevel(log_level)

        self.verbose=0

    def process_all_ccds(self, **keys):
        t0=time.time()
        for ccd in xrange(1,62+1):
            self.process_ccd(ccd, **keys)
            stdout.flush()
        ptime(time.time() - t0, format='Time for all 62 ccds: %s\n')
        stdout.flush()


    def process_ccd(self, ccd, **keys):
        t0=time.time()

        stdout.write('-'*72 + '\n')

        self.stat['error']        = 0
        self.stat['error_string'] = ''

        self.stat['types'] = keys.get('types',self.all_types)

        # set file names in self.stat
        self.set_output_filenames(ccd,clear=True)

        self.stat['ccd'] = ccd


        # ok, now we at least have the status file name to write to
        stdout.write("host: %s\n" % platform.node())
        stdout.write("ccd:  %02d\n" % ccd)

        self.setup()

        image,cat = self.get_image_cat(ccd)

        self.stat['image'] = image
        self.stat['cat'] = cat

        self.make_output_dir()

        # interacting with C++.  We want to catch certain errors
        # and just log them by type.  This is because internally
        # just throw char* since defining our own exceptions in
        # there is a pain
        try:
            self.load_images()
            self.load_catalog()
            self.set_log()
            self._process_ccd_types(self.stat['types'])
        except:
            if self.stat['error'] == 0:
                # this was an unexpected error
                self.stat['error'] = ERROR_SE_UNKNOWN
                self.stat['error_string'] = "Unexpected error: '%s'" % traceback.format_exc()
            else:
                # we caught this exception already and set an
                # error state.  Just proceed and write the status file
                pass
        self.write_status()
        if self.stat['copyroot']:
            self.copy_root()
        ptime(time.time() - t0, format='Time to process ccd: %s\n')

    def set_output_filenames(self, ccd, clear=False):

        fdict=deswl.files.generate_se_filenames(self.stat['serun'],
                                                self.stat['expname'],
                                                ccd)
        for k in fdict:
            fdict[k] = os.path.expandvars(fdict[k])
        self.stat['output_files'] = fdict

        # if outdir was none, we picked a proper dir
        if self.stat['outdir'] is None:
            self.stat['outdir'] = os.path.dirname(fdict['stars'])

        if clear:
            for k in fdict:
                if os.path.exists(fdict[k]):
                    print 'Removing existing file:',fdict[k]
                    os.remove(fdict[k])
    def setup(self):
        """
        Don't call this, let the process_ccd/process_all_ccds call it
        """

        self.logger.debug("In setup")

        # this stuff only needs to be loaded once.
        if not hasattr(self, 'tmv_dir'):

            stdout.write('running setup\n'); stdout.flush()

            self.get_band()
            self.get_proc_environ()

            # only loads if not serun is not None.  Will override the dataset
            if self.stat['serun'] is not None:
                self.load_serun()

            #self.set_outdir()
            self.set_config()

            self.load_wl()

            self.load_file_lists()

    def get_image_cat(self, ccd):
        # Here we rely on the fact that we picked out the unique, newest
        # version for each

        self.logger.debug("Entering get_image_cat")

        image=None
        expname=self.stat['expname']
        for ti in self.infolist:
            if ti['expname'] == expname and ti['ccd'] == ccd:
                image=ti['image_url']
                cat=ti['cat_url']
                break

        if image is None:
            bname = '%s-%s' % (expname,ccd)
            self.logger.debug("caught error loading image/cat")
            self.stat['error'] = ERROR_SE_MISC
            self.stat['error_string'] = \
                "Exposure ccd '%s' not found in '%s'" % \
                             (bname, self.stat['info_url'])
            raise ValueError(self.stat['error_string'])
        image = os.path.expandvars(image)
        cat = os.path.expandvars(cat)
        return image,cat

    def load_images(self):

        stdout.write("Loading images from '%s'\n" % self.stat['image'])
        stdout.flush()
        try:
            self.wl.load_images(str(self.stat['image']))
        except RuntimeError as e:
            # the internal routines currently throw const char* which
            # swig is converting to runtime
            self.logger.debug("caught error running load_images")
            self.stat['error'] = ERROR_SE_IO
            self.stat['error_string'] = unicode(str(e),errors='replace')
            stdout.write(self.stat['error_string']+'\n')
            raise e
        except TypeError as e:
            self.logger.debug("caught type error running load_images, probably string conversion")
            self.stat['error'] = ERROR_SE_IO
            self.stat['error_string'] = unicode(str(e),errors='replace')
            stdout.write(self.stat['error_string']+'\n')
            raise e

    def load_catalog(self):

        stdout.write("Loading catalog from '%s'\n" % self.stat['cat'])
        stdout.flush()
        try:
            self.wl.load_catalog(str(self.stat['cat']))
        except RuntimeError as e:
            # the internal routines currently throw const char* which
            # swig is converting to runtime
            self.logger.debug("caught error running load_catalog")
            self.stat['error'] = ERROR_SE_IO
            self.stat['error_string'] = unicode(str(e),errors='replace')
            stdout.write(self.stat['error_string']+'\n')
            raise e
        except TypeError as e:
            self.logger.debug("caught type error running load_catalog, probably string conversion")
            self.stat['error'] = ERROR_SE_IO
            self.stat['error_string'] = unicode(str(e),errors='replace')
            stdout.write(self.stat['error_string']+'\n')
            raise e

    def set_log(self):
        """
        log is actually the QA file.

        Interacts with C++, need some error processing because error strings
        sometimes get corrupted
        """
        self.logger.debug("In set_log")
        try:
            qafile=self.stat['output_files']['qa']
            stdout.write("    Setting qa file: '%s'\n" % qafile)
            stdout.flush()
            self.wl.set_log(str(qafile))
        except RuntimeError as e:
            self.logger.debug("caught internal RuntimeError running set_log")
            self.stat['error'] = ERROR_SE_SET_LOG
            self.stat['error_string'] = unicode(str(e),errors='replace')
        except TypeError as e:
            self.logger.debug("caught type error running set_log, probably string conversion")
            self.stat['error'] = ERROR_SE_SET_LOG
            self.stat['error_string'] = unicode(str(e),errors='replace')
            stdout.write(self.stat['error_string']+'\n')
            raise e

    def _process_ccd_types(self, types):

        self.stars_loaded=False
        self.psf_loaded=False
        self.shear_loaded=False

        if 'stars' in types:
            self.run_findstars()

        if 'psf' in types:
            self.run_measurepsf()

        if 'shear' in types:
            self.run_shear() 

        if 'split' in types:
            self.run_split()



    def set_config(self):
        # config file
        defconfig = path_join(self.stat['WL_DIR'], 'etc','wl.config')

        if self.stat['config'] is None:
            stdout.write("Determining config...\n")
            if self.runconfig is not None:
                if 'wl_config' in self.runconfig:
                    self.stat['config'] = self.runconfig['wl_config']
                else:
                    self.stat['config'] = defconfig
            else:
                self.stat['config'] = defconfig
            stdout.write("    using: '%s'\n" % self.stat['config'])
        else:
            stdout.write("Using input config: '%s'\n" % self.stat['config'])

    def load_serun(self):

        stdout.write("    Loading runconfig for serun: '%s'\n" % self.stat['serun'])
        stdout.flush()
        self.runconfig=deswl.files.Runconfig(self.stat['serun'])
            
        # make sure we have consistency in the software versions
        stdout.write('    Verifying runconfig: ...'); stdout.flush()
        self.runconfig.verify()

        stdout.write('OK\n'); stdout.flush()

        self.stat['dataset'] = self.runconfig['dataset']

    def set_outdir(self):
        """
        This method is unused
        """
        stdout.write("Setting outdir:\n")
        if self.stat['outdir'] is None:
            if self.stat['serun'] is not None:
                self.stat['outdir'] = \
                        deswl.files.wlse_dir(self.stat['serun'], 
                                             self.stat['expname'])
            else:
                self.stat['outdir']='.'

        self.stat['outdir'] = os.path.expandvars(self.stat['outdir'])
        if self.stat['outdir'] == '.':
            self.stat['outdir']=os.path.abspath('.')

        stdout.write("    outdir: '%s'\n" % self.stat['outdir'])


    def get_proc_environ(self):
        e=deswl.files.get_proc_environ()
        for k in e:
            self.stat[k] = e[k]

    def load_wl(self):
        # usually is saved as $DESFILES_DIR/..
        config_fname = os.path.expandvars(self.stat['config'])
        stdout.write("Loading config file '%s'\n" % config_fname)
        stdout.flush()

        if not os.path.exists(config_fname):
            self.stat['error'] = ERROR_SE_MISSING_FILE
            self.stat['error_string'] = \
                "Config File does not exist: '%s'" % config_fname
            raise IOError(self.stat['error_string'])
        
        try:
            self.wl = deswl.WL(str(config_fname))
            if self.stat['nodots']:
                self.wl.set_param("output_dots","false");
            if self.verbose > 0:
                self.wl.set_verbose(self.verbose)
            
            # this will go in the header
            if self.stat['serun']  is not None:
                self.wl.set_param("wlserun",self.stat['serun'])

        except RuntimeError as e:
            self.stat['error'] = ERROR_SE_LOAD_CONFIG
            self.stat['error_string'] = 'Error loading config: %s' % e
            stdout.write(self.stat['error_string']+'\n')
            raise e

    def set_verbose(self, verbose):
        self.verbose=int(verbose)

    def load_file_lists(self):
        if not hasattr(self,'infolist'):
            info, ifile=\
                    deswl.files.collated_redfiles_read(self.stat['dataset'], 
                                                       self.stat['band'], 
                                                       getpath=True)
            self.infolist = info
            self.stat['info_url'] = ifile

    def get_band(self):
        stdout.write("    Getting band for '%s'..." % self.stat['expname'])
        stdout.flush()
        self.stat['band']=\
            getband_from_expname(self.stat['expname'])
        stdout.write(" '%s'\n" % self.stat['band'])






    def write_status(self):
        statfile = self.stat['output_files']['stat']
        d=os.path.dirname(statfile)
        if not os.path.exists(d):
            os.makedirs(d)

        stdout.write("Writing status file: '%s'\n" % statfile)
        stdout.write("  staterr: %s\n" % self.stat['error'])
        stdout.write("  staterr string: '%s'\n" % self.stat['error_string'])
        json_util.write(self.stat, statfile)


    def run_findstars(self):
        starsfile = self.stat['output_files']['stars']
        try:
            stdout.write("\nFinding stars. Will write to \n    '%s'\n" \
                         % starsfile)
            stdout.flush()
            self.wl.find_stars(starsfile)
            stars_loaded=True
        except RuntimeError as e:
            self.stat['error'] = ERROR_SE_FINDSTARS
            self.stat['error_string'] = unicode(str(e),errors='replace')
            stdout.write(self.stat['error_string']+'\n')
            raise e

    def ensure_stars_loaded(self):
        if not self.stars_loaded:
            starsfile = self.stat['output_files']['stars']
            stdout.write("Loading star cat: '%s'\n" % starsfile)
            self.wl.load_starcat(starsfile)
            self.stars_loaded=True

    def run_measurepsf(self):
        try:
            self.ensure_stars_loaded() 

            files=self.stat['output_files']
            stdout.write("\nMeasuring psf; Will write to: \n"
                         "    '%s'\n"
                         "    '%s'\n" % (files['psf'],files['fitpsf']))
            stdout.flush()
            self.wl.measure_psf(files['psf'],files['fitpsf'])
            self.psf_loaded=True
        except RuntimeError as e:
            self.stat['error'] = ERROR_SE_MEASURE_PSF
            self.stat['error_string'] = unicode(str(e),errors='replace')
            stdout.write(self.stat['error_string']+'\n')
            raise e
    def ensure_psf_loaded(self):
        if not self.psf_loaded:
            files=self.stat['output_files']
            stdout.write("Loading psf cat: '%s'\n" % files['psf'])
            self.wl.load_psfcat(files['psf'])
            stdout.write("Loading fitpsf: '%s'\n" % files['fitpsf'])
            self.wl.load_fitpsf(files['fitpsf'])
            self.psf_loaded=True

    def run_shear(self):
        try:
            self.ensure_stars_loaded()
            self.ensure_psf_loaded()

            files=self.stat['output_files']
            stdout.write("\nMeasuring shear. Will write to \n    '%s'\n" \
                         % files['shear'])
            stdout.flush()
            self.wl.measure_shear(files['shear'])
            self.shear_loaded=True
        except RuntimeError as e:
            self.stat['error'] = ERROR_SE_MEASURE_SHEAR
            self.stat['error_string'] = unicode(str(e),errors='replace')
            stdout.write(self.stat['error_string']+'\n')
            raise e

    def run_split(self):
        try:
            files=self.stat['output_files']

            self.ensure_stars_loaded()
            
            stdout.write("\nSplitting catalog: \n"
                         "    '%s'\n"
                         "    '%s'\n" % (files['stars1'],files['stars2']))
            stdout.flush()
            self.wl.split_starcat(files["stars1"],files["stars2"])


            stdout.write("\nLoading stars1 \n'%s'\n" % files['stars1'])
            stdout.flush()
            self.wl.load_starcat(files['stars1'])

            stdout.write("\nMeasuring psf1; Will write to: \n"
                         "    '%s'\n"
                         "    '%s'\n" % (files['psf1'],files['fitpsf1']))
            stdout.flush()
            self.wl.measure_psf(files['psf1'],files['fitpsf1'])

            stdout.write("\nMeasuring shear1. Will write to \n'%s'\n" \
                         % files['shear1'])
            stdout.flush()
            self.wl.measure_shear(files['shear1'])

            stdout.write("\n\nLoading stars2 \n'%s'\n" % files['stars2']); stdout.flush()
            self.wl.load_starcat(files['stars2'])

            stdout.write("measuring psf2; Will write to: \n"
                         "    '%s'\n"
                         "    '%s'\n" % (files['psf2'],files['fitpsf2']))
            stdout.flush()
            self.wl.measure_psf(files['psf2'],files['fitpsf2'])

            stdout.write("Measuring shear2. Will write to \n'%s'\n" \
                         % files['shear2'])
            stdout.flush()
            self.wl.measure_shear(files['shear2'])
        except RuntimeError as e:
            self.stat['error'] = ERROR_SE_SPLIT_STARS
            self.stat['error_string'] = unicode(str(e),errors='replace')
            stdout.write(self.stat['error_string']+'\n')
            raise e





    def copy_root(self):
        """

        This method does *not* change the self.stat dictionary.  This is
        important because we don't want things like the 'outdir' to change.
        We *might* want to eventually write a new stat file though.

        """
        # this assumes the default root, to which we will copy
        fdict_def=\
            deswl.files.generate_se_filenames(self.stat['expname'], 
                                              self.stat['ccd'], 
                                              serun=self.stat['serun'])


        # see if the file is already in the destination directory.  
        if self.stat['output_files']['stat'] != fdict_def['stat']:

            dirname=os.path.dirname(fdict_def['stat'])
            old_dirname = os.path.dirname(self.stat['output_files']['stat'])

            if not os.path.exists(dirname):
                os.makedirs(dirname)
            # copy the files
            rc=deswl.files.Runconfig()
            for ftype in rc.se_filetypes:
                f=self.stat['output_files'][ftype]
                df=fdict_def[ftype]
                hostshort=self.stat['hostname'].split('.')[0]
                stdout.write(' * moving %s:%s to\n    %s\n' % (hostshort,f,df))
                if not os.path.exists(f):
                    stdout.write('    Source file does not exist\n')
                    stdout.flush()
                else:
                    shutil.copy2(f, df)
                    os.remove(f)




    def make_output_dir(self):
        outdir=self.stat['outdir']
        if not os.path.exists(outdir):
            stdout.write("Creating output dir: '%s'\n" % outdir)
            os.makedirs(outdir)
class ImageProcessorOld(deswl.WL):
    """
    Much simpler than ExposureProcessor for processing a single image/catalog pair.
    Good for interactive use.
    """
    def __init__(self, config, image, cat, **args):
        deswl.WL.__init__(self, config)

        self.config_url = config
        self.image_url = image
        self.cat_url = cat

        outdir = args.get('outdir',None)
        if outdir is None:
            outdir = os.path.dirname(self.cat_url)
        self.outdir = outdir
        if self.outdir == '':
            self.outdir = '.'

        self.set_output_names()

        self.imcat_loaded = False

    def set_output_names(self):
        out_base = os.path.basename(self.cat_url)
        out_base = out_base.replace('_cat.fits','')
        out_base = out_base.replace('.fits','')
        out_base = out_base.replace('.fit','')

        out_base = path_join(self.outdir, out_base)

        rc = deswl.files.Runconfig()
        self.names = {}
        for type in rc.se_filetypes:
            self.names[type] = out_base+'-'+type+'.fits'

    def load_data(self):
        stdout.write('Loading %s\n' % self.image_url)
        self.load_images(self.image_url)
        stdout.write('Loading %s\n' % self.cat_url)
        self.load_catalog(self.cat_url)
        self.imcat_loaded = True

    def process(self, types=['stars','psf','shear','split']):
        if not self.imcat_loaded:
            self.load_data()
        if 'stars' in types:
            self.find_stars()
        if 'psf' in types:
            self.measure_psf()
        if 'shear' in types:
            self.measure_shear()
        if 'split' in types:
            self.split_starcat()
            # note only 'psf' and 'shear' will actually get processed
            self.process_split(types=types)


    def process_full(self):
        if not self.imcat_loaded:
            self.load_data()
        self.find_stars()
        self.measure_psf()
        self.measure_shear()

    def find_stars(self):
        print("Finding stars.  Will write to: %s" % self.names['stars'])
        deswl.WL.find_stars(self, self.names['stars'])
    def measure_psf(self):
        print("Measuring PSF. Will write to: "
              "\n    %s\n    %s" % (self.names['psf'],self.names['fitpsf']))
        deswl.WL.measure_psf(self,self.names['psf'],self.names['fitpsf'])
    def measure_shear(self):
        print("Measuring shear.  Will write to: %s" % self.names['shear'])
        deswl.WL.measure_shear(self,self.names['shear'])

    def split_starcat(self):
        stdout.write("splitting catalog: \n"
                     "    '%s'\n"
                     "    '%s'\n" % (self.names['stars1'],self.names['stars2']))
        deswl.WL.split_starcat(self, self.names["stars1"],self.names["stars2"])

        """
        stdout.write('loading stars1\n')
        self.load_starcat(fdict['stars1'])
        stdout.write('measuring psf1\n')
        deswl.WL.measure_psf(self, self.names['psf1'], self.names['fitpsf1'])
        stdout.write('measuring shear1\n')
        deswl.WL.measure_shear(self,self.names['shear1'])

        stdout.write('loading stars2\n')
        self.load_starcat(fdict['stars2'])
        stdout.write('measuring psf2\n')
        deswl.WL.measure_psf(self, self.names['psf2'], self.names['fitpsf2'])
        stdout.write('measuring shear2\n')
        deswl.WL.measure_shear(self,self.names['shear2'])
        """

    def process_split(self, splitnum=[1,2], types=['psf','shear']):
        for num in splitnum:
            nstr = str(num)

            t = ImageProcessor(self.config_url, self.image_url, self.cat_url, 
                               outdir=self.outdir) 

            t.names['psf'] = t.names['psf'+nstr]
            t.names['fitpsf'] = t.names['fitpsf'+nstr]
            t.names['shear'] = t.names['shear'+nstr]

            t.load_starcat(t.names['stars'+nstr])

            # need to do this explicitly just in case 'stars' or 'split' 
            # were accidentally sent
            if 'psf' in types:
                t.process('psf')
            if 'shear' in types:
                t.process('shear')


class SECondorJobs(dict):
    def __init__(self, serun, band, 
                 type='fullpipe',
                 nodes=1, ppn=1, nthread=1):
        """
        Need to implement nodes, ppn
        """
        self['run'] = serun
        self['band'] = band
        self['type'] = type
        self['nodes'] = nodes
        self['ppn'] = ppn
        self['nthread'] = nthread

        self.rc = deswl.files.Runconfig(self['run'])
        self['dataset'] = self.rc['dataset']

        # get the file lists
        self.fileinfo = deswl.files.collated_redfiles_read(self['dataset'], band)


    def write(self, dryrun=False):
        # get unique exposure names
        edict={}
        for fi in self.fileinfo:
            edict[fi['expname']] = fi
        
        for expname in edict:
            sejob = SECondorJob(self['run'], expname, 
                                type=self['type'],
                                nodes=self['nodes'], 
                                ppn=self['ppn'], 
                                nthread=self['nthread'])
            sejob.write_submit(dryrun=dryrun)
            sejob.write_script(dryrun=dryrun)

    def write_byccd(self, dryrun=False):
        for fi in self.fileinfo:
            expname=fi['expname']
            ccd=fi['ccd']

            sejob = SECondorJob(self['run'], expname, ccd=ccd,
                                type=self['type'],
                                nodes=self['nodes'], 
                                ppn=self['ppn'], 
                                nthread=self['nthread'])
            sejob.write_submit(dryrun=dryrun)
            sejob.write_script(dryrun=dryrun)



 
class SECondorJob(dict):
    def __init__(self, serun, expname, 
                 ccd=None, type='fullpipe',
                 nodes=1, ppn=1, nthread=1):
        """
        Need to implement nodes, ppn
        """
        self['run'] = serun
        self['expname'] = expname
        self['ccd'] = ccd
        self['type'] = type
        self['nodes'] = nodes
        self['ppn'] = ppn
        self['nthread'] = nthread

    def write_submit(self, verbose=False, dryrun=False):
        self.make_condor_dir()
        f=deswl.files.wlse_condor_path(self['run'], 
                                       self['expname'], 
                                       typ=self['type'], 
                                       ccd=self['ccd'])

        print "writing to condor submit file:",f 
        text = self.submit_text()
        if verbose or dryrun:
            print text 
        if not dryrun:
            with open(f,'w') as fobj:
                fobj.write(text)
        else:
            print "this is just a dry run" 

    def write_script(self, verbose=False, dryrun=False):
        self.make_condor_dir()
        f=deswl.files.wlse_script_path(self['run'], 
                                       self['expname'], 
                                       typ=self['type'], 
                                       ccd=self['ccd'])

        print "writing to condor script file:",f 
        text = self.script_text()
        if verbose or dryrun:
            print text 
        if not dryrun:
            with open(f,'w') as fobj:
                fobj.write(text)

            print "changing mode of file to executable"
            os.popen('chmod 755 '+f)
        else:
            print "this is just a dry run" 


    def submit_text(self):
        condor_dir = self.condor_dir()
        script_base = deswl.files.wlse_script_base(self['expname'],
                                                   ccd=self['ccd'],
                                                   typ=self['type'])
        submit_text="""
Universe        = vanilla
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

        return submit_text

    def script_text(self):
        rc=deswl.files.Runconfig(self['run'])
        wl_load = _make_load_command('wl',rc['wlvers'])
        tmv_load = _make_load_command('tmv',rc['tmvvers'])
        esutil_load = _make_load_command('esutil', rc['esutilvers'])
     
        if self['ccd'] == None:
            ccd=''
        else:
            ccd=self['ccd']

        script_text="""#!/bin/bash
source ~astrodat/setup/setup.sh
source ~astrodat/setup/setup-modules.sh
{tmv_load}
{wl_load}
{esutil_load}
module load desfiles

export OMP_NUM_THREADS={nthread}
shear-run                     \\
     --serun={serun}          \\
     --nodots                 \\
     {expname} {ccd} 2>&1
""".format(wl_load=wl_load, 
           tmv_load=tmv_load, 
           esutil_load=esutil_load,
           nthread=self['nthread'],
           serun=self['run'],
           expname=self['expname'],
           ccd=ccd)

        return script_text

    def condor_dir(self):
        if self['ccd'] is None:
            return deswl.files.condor_dir(self['run'])
        else:
            return deswl.files.condor_dir(self['run'], subdir='byccd')


    def make_condor_dir(self):
        condor_dir = self.condor_dir()
        if not os.path.exists(condor_dir):
            print 'making output dir',condor_dir
            os.makedirs(condor_dir)




'''
