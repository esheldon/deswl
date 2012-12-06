"""
The process is similar to what happens for the SE and ME runs

In short
    - Generate a runconfig
    - /bin/generate-generic-wq

        - ~/des-wq/{run}/{expname}.yaml
            These are the actual process jobs; calls the code
            for each ccd using the config files under /byccd

        - for each ccd, there are config file
            ~/des-wq/{run}/byccd/{expname}-{ccd}-config.yaml

        - ~/des-wq/{run}/{expname}-check.yaml
            These are jobs to check the processing

        - ~/des-wq/{run}/check-reduce.py
            Run this to collate the results into the "goodlist"
            and "badlist"

    - submit wq jobs
        Follow the .wqlog files for very brief updates.
    - submit the check wq jobs
        Follow the .wqlog files for very brief updates.
    - run the reducer to generate the goodlist and badlist

The GenericProcessor will send stdout and stderr for process, and
some other diagnostics to the 'log' entry in config['output_files']['log']

    - .../DES/{fileclass}/{run}/{expname}/{run}-{expname}-{ccd}.log

Similarly, the GenericProcessor writes the stat file

    - .../DES/{fileclass}/{run}/{expname}/{run}-{expname}-{ccd}-stat.yaml

The stat file holds the exit_status and whatever was in the config file.

The collation step is not yet implemented; probably each owner of the code will
have to do this step.
"""
import os
from sys import stderr
import esutil as eu
import deswl

_url_pattern_byexp='%(run)s-%(expname)s-%(ftype)s.%(ext)s'
_url_pattern_byccd='%(run)s-%(expname)s-%(ccd)02d-%(ftype)s.%(ext)s'

def gendir(fileclass, run, expname, **keys):
    """
    Generate a directory url.

    The format is $DESDATA/{fileclass}/{run}/{expname}
    $DESDATA may be in hdfs

    parameters
    ----------
    fileclass: string
        The fileclass, e.g. 'am' etc.
    run: string
        The run identifier
    expname:
        The DES exposure name
    """

    rundir=deswl.files.run_dir(fileclass, run, **keys)
    dir=os.path.join(rundir, expname)
    return dir

def genurl(pattern, fileclass, run, expname, **keys):
    """
    Generate a url using the input pattern.

    parameters
    ----------
    pattern: string 
        A patterns to convert run, expname, etc. into a file url.  If ccd is
        sent in the keywords it will also be used.
    fileclass: string
        The fileclass, e.g. 'impyp' or 'am' etc.
    run: string
        The run identifier
    expname:
        The DES exposure name
    ccd: integer, optional
        The ccd number.
    """

    ccd=keys.get('ccd',None)
    if ccd is not None:
        basename=pattern % {'run':run,
                            'expname':expname,
                            'ccd':int(ccd)}
    else:
        basename=pattern % {'run':run,
                            'expname':expname,
                            'ccd':int(ccd)}

    dir = gendir(fileclass, run, expname, **keys)

    url = os.path.join(dir, basename)
    return url

def generate_filenames(patterns, fileclass, run, expname, **keys):
    """
    Generate all files for the input pattern dictionary.

    parameters
    ----------
    patterns: dict
        A dictionary with format patterns to convert run, expname, etc.  into a
        file url.  If ccd is sent in the keywords it will also be used.
    fileclass: string
        The fileclass, e.g. 'impyp' or 'am' etc.
    run: string
        The run identifier
    expname:
        The DES exposure name
    ccd: integer, optional
        The ccd number.
    """
    fdict={}
    for ftype,pattern in patterns.iteritems():
        fdict[ftype] = genurl(pattern,fileclass,run,expname,**keys)

    return fdict


class GenericConfig(dict):
    """
    to create and write the "config" files, which hold the command
    to run, input/output file lists, and other metadata.
    """
    def __init__(self,run):
        self['run'] = run
        # this has serun in it
        self.rc = deswl.files.Runconfig(self['run'])

        self.config_data=None

    def write_byccd(self):
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
        raise RuntimeError("you must over-ride get_config_data")

    def get_command(self, fdict):
        raise RuntimeError("you must over-ride get_command")


class GenericProcessor(dict):
    def __init__(self, config):
        """
        required keys in config
            - 'run' The run id, just for identification
            - 'input_files', which is itself a dictionary.  The files will be
            checked to see if any files are in hdfs and if so these will be staged
            out.

            - 'output_files', a dictionary.  HDFS files will be first written
            locally and then pushed in.
                - This dict must contain an entry called 'stat' which 
                should be a .yaml file or .json file. It will contain the
                exit_status for the process and other metadata.  This
                file is written by THIS PROGRAM and should not be written
                by the called external program.

                It must also contain an entry called 'log' where all
                stdout and stderr will be written.

            - 'command'.  This can refer to objects in the config file,
            including sub-objects of input_files and output_files, which will
            be pulled into the main name space.  Reference should be made using
            formats like %(name)s %(name)0.2f etc.

            It is important not to put hdfs file names into the command
            directly, as these should instead use the *local* versions
            of the file names.  The correct version will be used during
            interpolation of variables from the config into the command

        Some optional fields

            - 'timeout': a timeout for the code in seconds.  Default two hours,
            2*60*60

        """
        req_fields=['run','input_files','output_files','command']
        for k in req_fields:
            if k not in config:
                raise ValueError("required field missing: '%s'" % k)
        if 'stat' not in config['output_files']:
            raise ValueError("required field missing from output_files: 'stat'")
        if 'log' not in config['output_files']:
            raise ValueError("required field missing from output_files: 'log'")

        # add a log file
        stat=config['output_files']['stat']
        # this and exit status the only thing that goes to stderr or stdout
        print >>stderr,"log file:",config['output_files']['log']

        for k,v in config.iteritems():
            self[k]=v

        if 'timeout' not in self:
            self['timeout'] = 2*60*60 # two hours
        else:
            self['timeout'] = int(self['timeout'])

        self.setup_files()

        log_name = self.outf['log']['local_url']
        #eu.ostools.makedirs_fromfile(log_name)
        self._log = open(log_name,'w')

    def __enter__(self):
        return self
    def __exit__(self, exception_type, exception_value, traceback):
        # this shouldn't be necessary
        if hasattr(self,'_log'):
            if isinstance(self._log,file):
                self._log.close()

    def run(self):

        self['exit_status'] = -9999
        #self._dorun()
        try:
            self._dorun()
            print >>self._log,'Done'
        finally:
            self.write_status()
            self.cleanup()

    def _dorun(self):
        from subprocess import STDOUT
        print >>self._log,os.uname()[1]
        
        #self.make_output_dirs()
        self.stage()

        command=self.get_command()

        print >>self._log,"running command: \n\t",command
        self._log.flush()
        exit_status, oret, eret = \
            eu.ostools.exec_process(command,
                                    timeout=self['timeout'],
                                    stdout_file=self._log,
                                    stderr_file=STDOUT)

        self['exit_status'] = exit_status
        print >>self._log,'exit_status:',exit_status
        print >>stderr,'exit_status:',exit_status


    def get_command(self):
        """
        Interpolate keys into the input command
        """
        command = self['command'] % self.allkeys
        return command

    def make_output_dirs(self):
        """
        run through all the output files and make sure associated
        directories exist
        """
        for k,v in self.outf.iteritems():
            eu.ostools.makedirs_fromfile(v['local_url'])
    def stage(self):
        """
        Stage in hdfs files as necessary
        """
        for k,v in self.inf.iteritems():
            if v['in_hdfs']:
                print >>self._log,'staging:',\
                        v['hdfs_file'].hdfs_url,'->',v['hdfs_file'].localfile
                v['hdfs_file'].stage()
    def cleanup(self):
        """
        Remove local versions of staged hdfs files.
        For input files, put them in hdfs and clean up local storage
        """
        # clean up staged input files
        for k,v in self.inf.iteritems():
            if v['in_hdfs']:
                print >>self._log,'removing:',v['hdfs_file'].localfile
                v['hdfs_file'].cleanup()

        # push local versions of outputs into hdfs, then clean up
        do_log_hdfs=False
        for k,v in self.outf.iteritems():
            if v['in_hdfs']:
                if k == 'log':
                    # save till later so we see the log messages
                    do_log_hdfs=True
                    continue
                if os.path.exists(v['local_url']):
                    print >>self._log,'putting:',\
                        v['hdfs_file'].localfile,'->',v['hdfs_file'].hdfs_url
                    v['hdfs_file'].put(clobber=True)
                else:
                    print >>self._log,'local file not found:',v['local_url']
                v['hdfs_file'].cleanup()
        if do_log_hdfs:
            hf=self.outf['log']
            print >>self._log,'putting:',\
                hf['hdfs_file'].localfile,'->',hf['hdfs_file'].hdfs_url

            self._log.flush()
            hf['hdfs_file'].put(clobber=True)
            self._log.close()
            hf['hdfs_file'].cleanup()

    def setup_files(self):
        """
        Create self.inf and self.out, as well as the self.allkeys dict with all
        keys together from input config and output/input file dicts.

        HDFS files are checked for and marked appropriately.
        """
        inf={}
        outf={}

        self.inf = self._setup_files(self['input_files'])
        self.outf = self._setup_files(self['output_files'])

        # bring all keys into a single namespace for use
        # in the command string interpolation
        self.allkeys={}
        for k,v in self.iteritems():
            self.allkeys[k] = v
        for k,v in self.inf.iteritems():
            self.allkeys[k] = v['local_url']
        for k,v in self.outf.iteritems():
            self.allkeys[k] = v['local_url']

    def _make_log_name(self, example):
        idot=example.rfind('.')
        if idot == -1:
            log_name = example+'.log'
        else:
            log_name = example[0:idot]+'.log'
        return log_name

    def _setup_files(self, fdict_in):
        """
        Get info about the files.
        """
        fdict={}
        for f,v in fdict_in.iteritems():
            fdict[f] = {'in_hdfs':False, 'url':v,'local_url':v}
            if v[0:4] == 'hdfs':
                fdict[f]['in_hdfs'] = True
                fdict[f]['hdfs_file'] = \
                    eu.hdfs.HDFSFile(v)
                fdict[f]['local_url'] = fdict[f]['hdfs_file'].localfile
        return fdict

    def write_status(self):
        """
        Add a bunch of new things to self and write self out as the stat file
        """
        print >>self._log,'writing status file:',self.outf['stat']['local_url']
        outd={}
        for k,v in self.iteritems():
            outd[k] = v
        eu.io.write(self.outf['stat']['local_url'],outd)



class GenericSEWQJob(dict):
    """
    Generic WQ job to process all ccds in an exposure.

    You should also create the config files for each exposure/ccd using. config
    files go to deswl.files.se_config_path(run,expname,ccd=ccd)

    Send check=True to generate the check file instead of the processing file
    """
    def __init__(self, run, expname, **keys):
        self['run'] = run
        self['expname'] = expname
        self['groups'] = keys.get('groups',None)
        self['priority'] = keys.get('priority','low')

        self['job_file']= deswl.files.se_wq_path(self['run'], self['expname'])

    def write(self, check=False):

        expname=self['expname']
        groups = self['groups']

        if check:
            groups=None # can run anywhere for sure
            job_file=self['job_file'].replace('.yaml','-check.yaml')
            job_name=expname+'-chk'
        else:
            job_file=self['job_file']
            job_name=expname

        if groups is None:
            groups=''
        else:
            groups='group: ['+groups+']'

        rc=deswl.files.Runconfig(self['run'])
        wl_load = deswl.files._make_load_command('wl',rc['wlvers'])
        esutil_load = deswl.files._make_load_command('esutil', rc['esutilvers'])

        # naming schemem for this generic type figurd out from run
        config_file1=deswl.files.se_config_path(self['run'], 
                                                self['expname'], 
                                                ccd=1)
        config_file1=os.path.join('byccd',os.path.basename(config_file1))
        conf=config_file1.replace('01-config.yaml','$i-config.yaml')
        if check:
            chk=config_file1.replace('01-config.yaml','$i-check.json')
            err=config_file1.replace('01-config.yaml','$i-check.err')

            #cmd="wl-check-generic {conf} 1> {chk} 2> {err}"
            cmd="wl-check-generic {conf} 1> {chk}"
            cmd=cmd.format(conf=conf, chk=chk, err=err)
        else:
            # log is now automatically created by GenericProcessor
            # and written into hdfs
            cmd="wl-run-generic {conf}".format(conf=conf)

        text = """
command: |
    source /opt/astro/SL53/bin/setup.hadoop.sh
    source ~astrodat/setup/setup.sh
    source ~/.dotfiles/bash/astro.bnl.gov/modules.sh
    source ~esheldon/local/des-oracle/setup.sh

    %(esutil_load)s
    %(wl_load)s

    for i in `seq -w 1 62`; do
        echo "ccd: $i"
        %(cmd)s
    done

%(groups)s
priority: %(priority)s
job_name: %(job_name)s\n""" % {'esutil_load':esutil_load,
                               'wl_load':wl_load,
                               'cmd':cmd,
                               'groups':groups,
                               'priority':self['priority'],
                               'job_name':job_name}


        with open(job_file,'w') as fobj:
            fobj.write(text)

class GenericSEPBSJob(dict):
    """
    Generic job files to process all ccds in an exposure.

    You should also create the config files for each exposure/ccd using. config
    files go to deswl.files.se_config_path(run,expname,ccd=ccd)

    Send check=True to generate the check file instead of the processing file
    """
    def __init__(self, run, expname, **keys):
        self['run'] = run
        self['expname'] = expname
        self['queue'] = keys.get('queue','serial')
        self['job_file']= deswl.files.get_se_pbs_path(self['run'], self['expname'])

    def write(self, check=False):

        expname=self['expname']
        groups = self['groups']

        if check:
            groups=None # can run anywhere for sure
            job_file=self['job_file'].replace('.pbs','-check.pbs')
            job_name=expname+'-chk'
        else:
            job_file=self['job_file']
            job_name=expname

        job_file_base=os.path.basename(job_file).replace('.pbs','')

        if groups is None:
            groups=''
        else:
            groups='group: ['+groups+']'

        rc=deswl.files.Runconfig(self['run'])
        wl_load = deswl.files._make_load_command('wl',rc['wlvers'])
        esutil_load = deswl.files._make_load_command('esutil', rc['esutilvers'])

        # naming scheme for this generic type figured out from run
        config_file1=deswl.files.se_config_path(self['run'], 
                                                self['expname'], 
                                                ccd=1)
        config_file1=os.path.join('byccd',os.path.basename(config_file1))
        conf=config_file1.replace('01-config.yaml','$i-config.yaml')
        if check:
            chk=config_file1.replace('01-config.yaml','$i-check.json')
            err=config_file1.replace('01-config.yaml','$i-check.err')

            cmd="wl-check-generic {conf} 1> {chk}"
            cmd=cmd.format(conf=conf, chk=chk, err=err)
        else:
            # log is now automatically created by GenericProcessor
            # and written into hdfs
            cmd="wl-run-generic {conf}".format(conf=conf)

        text = """#!/bin/bash -l
#PBS -q %(queue)s
#PBS -l nodes=1:ppn=1
#PBS -l walltime=00:30:00
#PBS -N %(job_name)s
#PBS -e %(job_file_base)s.err
#PBS -o %(job_file_base)s.out
#PBS -V

cd $PBS_O_WORKDIR

%(esutil_load)s
%(wl_load)s

for i in `seq -w 1 62`; do
    echo "ccd: $i"
    %(cmd)s
done
        \n""" % {'esutil_load':esutil_load,
                 'wl_load':wl_load,
                 'cmd':cmd,
                 'groups':groups,
                 'queue':queue,
                 'job_name':job_name,'job_file_base':job_file_base}


        with open(job_file,'w') as fobj:
            fobj.write(text)


