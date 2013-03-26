"""
The process is similar to what happens for the SE and ME runs

In short
    - Generate a runconfig
    - /bin/deswl-gen-pbs

        - /{run}/{expname}.pbs
            These are the actual process jobs; calls the code
            for each ccd using the config files under /byccd

        - for each ccd, there are config file
            /{run}/byccd/{expname}-{ccd}-config.yaml

        - there can be a script, name of your choosing, e.g.
            /{run}/byccd/{expname}-{ccd}.sh

        - /{run}/{expname}-check.pbs
            These are jobs to check the processing

        - /{run}/check-reduce.py
            Run this to collate the results into the "goodlist"
            and "badlist"

    - submit pbs jobs
    - submit the check wq jobs
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

def get_run_command(config_file):
    return 'deswl-run %s' % config_file

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
                            'expname':expname}

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

    also write script files if needed
    """
    def __init__(self,run, **keys):
        self['run'] = run
        for k,v in keys.iteritems():
            self[k] = v

        # this has serun in it
        self.rc = deswl.files.Runconfig(self['run'])

        self.config_data=None
        self._make_module_loads()

    def _make_module_loads(self):
        for module in ['desdb','deswl','esutil']:
            load_key='%s_load' % module
            mup=p.upper()
            vers=self.rc['%s_VERS'] % mup

            lcmd='module unload {module} && module load {module}/{vers}'
            lcmd=lcmd.format(module=m,vers=vers)

            self[load_key] = lcmd


    def write_byccd(self):
        """
        Write all config files for expname/ccd
        """

        mpibatch_cmds=deswl.files.get_mpibatch_cmds_file(self['run'])
        print 'writing mpibatch commands to',mpibatch_cmds
        eu.ostools.makedirs_fromfile(mpibatch_cmds)
        with open(mpibatch_cmds,'w') as cmds:
            cmds.write('&listcmd\n')
            cmds.write('cmd=\n')

            all_fd = self.get_config_data()
            i=1
            ne=62*len(all_fd)
            for expname,fdlist in all_fd.iteritems():
                # commands for by-exposure processing
                cmdlist_file=\
                    deswl.files.get_exp_mpibatch_cmds_file(self['run'], expname)
                eu.ostools.makedirs_fromfile(cmdlist_file)
                with open(cmdlist_file,'w') as ecmds:
                    ecmds.write('&listcmd\n')
                    ecmds.write('cmd=\n')

                    # mpi pbs script to submit all those ecmds
                    self.write_exp_mpi(self['run'],expname)

                    # now by ccd
                    for iccd,fd in enumerate(fdlist):
                        config_file=deswl.files.get_se_config_path(fd['run'],
                                                                   fd['expname'],
                                                                   ccd=fd['ccd'])
                        log_file=deswl.files.get_se_log_path(fd['run'],
                                                             fd['expname'],
                                                             ccd=fd['ccd'])
                        fd['output_files']['log']=log_file

                        if i==1 or (i % 1000) == 0:
                            print >>stderr,"Writing config (%d/%d) %s" % (i,ne,config_file)
                        eu.ostools.makedirs_fromfile(config_file)
                        eu.io.write(config_file, fd)

                        mpiscript_file=deswl.files.get_se_mpiscript_path(fd['run'],
                                                                         fd['expname'],
                                                                         ccd=fd['ccd'])
                        cmds.write("'%s',\n" % mpiscript_file)
                        ecmds.write("'%s',\n" % mpiscript_file)

                        if 'script' in fd:
                            script_file=fd['script']
                            if i==1 or (i % 1000) == 0:
                                print >>stderr,"    %s" % script_file
                            with open(script_file,'w') as fobj:
                                script_data=self.get_script(fd)
                                fobj.write(script_data)

                        self.write_mpi_script(fd)

                        i += 1

                    ecmds.write('/\n')

            cmds.write('/\n')

    def write_exp_mpi(self, run, expname):
        job_file=deswl.files.get_exp_mpibatch_pbs_file(run, expname)
        cmdlist_file=deswl.files.get_exp_mpibatch_cmds_file(run, expname)

        self['job_file']= deswl.files.get_se_pbs_path(run, expname)
        job_name='%s-%s' % (run,expname.replace('decam-',''))

        nodes=2
        ppn=8
        np=nodes*ppn
        walltime="1:00:00"

        text="""#!/bin/bash -l
#PBS -N {job_name}
#PBS -j oe
#PBS -l nodes={nodes}:ppn={ppn},walltime={walltime}
#PBS -q regular
#PBS -o {job_file}.pbslog
#PBS -A des

if [[ "Y${{PBS_O_WORKDIR}}" != "Y" ]]; then
    cd $PBS_O_WORKDIR
fi

# mpibatch takes the cmdlist file name on stdin
cmdlist={cmdlist_file}
mpirun -np {np} minions < ${cmdlist}

        \n""".format(job_name=job_name,
                     nodes=nodes,
                     ppn=ppn,
                     walltime=walltime,
                     job_file=job_file,
                     cmdlist_file=cmdlist_file,
                     np=np)

        eu.ostools.makedirs_fromfile(job_file)
        with open(job_file,'w') as fobj:
            fobj.write(text)


    def write_mpi_script(self, fd):

        config_file=deswl.files.get_se_config_path(fd['run'],
                                                   fd['expname'],
                                                   ccd=fd['ccd'])
        cmd=get_run_command(config_file)
        text="""#!/bin/bash -l
{esutil_load}
{desdb_load}
{deswl_load}

{cmd}
        \n""".format(esutil_load=self['esutil_load'],
                     desdb_load=self['desdb_load'],
                     deswl_load=self['deswl_load'],
                     cmd=cmd)

        mpiscript_file=deswl.files.get_se_mpiscript_path(self['run'],
                                                         fd['expname'],
                                                         ccd=fd['ccd'])
        
        eu.ostools.makedirs_fromfile(mpiscript_file)
        with open(mpiscript_file,'w') as fobj:
            fobj.write(text)
        os.system('chmod u+x %s' % mpiscript_file)

    def write_mpibatch(self):
        """
        Batching individual jobs using mpi

        requires the program mpibatch installed
        """
        rc=self.rc
        job_name='%s-batch' % self['run']
        nodes=16
        ppn=8
        np=nodes*ppn
        # assuming 31,000 jobs (ccds), 7 minutes
        # per job and 16*8=128 cores, we need ~29
        # hours.  36 should be plenty
        # using 128 and less than 48 hours cores means means
        # we expect to end up in the reg_small exec queue
        walltime='36:00:00'
        queue=self.get('queue','regular')

        job_file=deswl.files.get_mpibatch_pbs_file(self['run'])
        cmdlist_file=deswl.files.get_mpibatch_cmds_file(self['run'])

        mpibatch_text="""#!/bin/bash -l
#PBS -N {job_name}
#PBS -j oe
#PBS -l nodes={nodes}:ppn={ppn},walltime={walltime}
#PBS -q {queue}
#PBS -o {job_file}.pbslog
#PBS -A des

if [[ "Y${{PBS_O_WORKDIR}}" != "Y" ]]; then
    cd $PBS_O_WORKDIR
fi

find ./byccd -name "*decam-*-mpi.sh" | mpirun -np {np} mpibatch

echo "done mpibatch"
        \n"""

        mpibatch_text=mpibatch_text.format(job_name=job_name,
                         nodes=nodes,
                         ppn=ppn,
                         np=np,
                         walltime=walltime,
                         queue=queue,
                         job_file=job_file,
                         cmdlist_file=cmdlist_file)

        print 'Writing mpibatch pbs file:',job_file
        eu.ostools.makedirs_fromfile(job_file)
        with open(job_file,'w') as fobj:
            fobj.write(mpibatch_text)
        
    def write_check_mpibatch(self):
        """
        Batching individual jobs using mpi

        requires the program mpibatch installed
        """
        rc=self.rc
        job_name='%s-check' % self['run']
        nodes=8
        ppn=8
        np=nodes*ppn
        # assuming 31,000 jobs (ccds), 7 minutes
        # per job and 16*8=128 cores, we need ~29
        # hours.  36 should be plenty
        # using 128 and less than 48 hours cores means means
        # we expect to end up in the reg_small exec queue
        walltime='00:30:00'
        queue=self.get('queue','regular')

        job_file=deswl.files.get_mpibatch_check_pbs_file(self['run'])
        cmdlist_file=deswl.files.get_mpibatch_cmds_file(self['run'])

        mpibatch_text="""#!/bin/bash -l
#PBS -N {job_name}
#PBS -j oe
#PBS -l nodes={nodes}:ppn={ppn},walltime={walltime}
#PBS -q {queue}
#PBS -o {job_file}.pbslog
#PBS -A des

if [[ "Y${{PBS_O_WORKDIR}}" != "Y" ]]; then
    cd $PBS_O_WORKDIR
fi

find ./byexp -name "*decam-*-check.pbs" | mpirun -np {np} mpibatch

echo "done mpibatch"
        \n"""

        mpibatch_text=mpibatch_text.format(job_name=job_name,
                         nodes=nodes,
                         ppn=ppn,
                         np=np,
                         walltime=walltime,
                         queue=queue,
                         job_file=job_file,
                         cmdlist_file=cmdlist_file)

        print 'Writing check mpibatch pbs file:',job_file
        eu.ostools.makedirs_fromfile(job_file)
        with open(job_file,'w') as fobj:
            fobj.write(mpibatch_text)
 
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

        self.make_output_dirs()

        log_name=self.outf['log']['local_url']
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
        try:
            for k,v in self.outf.iteritems():
                eu.ostools.makedirs_fromfile(v['local_url'])
        except:
            # probably a race condition
            pass
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
        config_file1=deswl.files.get_se_config_path(self['run'], 
                                                    self['expname'], 
                                                    ccd=1)
        config_file1=os.path.join('byccd',os.path.basename(config_file1))
        conf=config_file1.replace('01-config.yaml','$i-config.yaml')
        if check:
            chk=config_file1.replace('01-config.yaml','$i-check.json')
            err=config_file1.replace('01-config.yaml','$i-check.err')

            #cmd="wl-check-generic {conf} 1> {chk} 2> {err}"
            cmd="deswl-check {conf} 1> {chk}"
            cmd=cmd.format(conf=conf, chk=chk, err=err)
        else:
            # log is now automatically created by GenericProcessor
            # and written into hdfs
            cmd=get_run_command(conf)

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
        self['job_file']= \
            deswl.files.get_se_pbs_path(self['run'], self['expname'])

        self.rc=deswl.files.Runconfig(self['run'])

        ver=self.rc['DESDB_VERS']
        self['desdb_load'] = \
            'module unload desdb && module load desdb/%s' % ver

        ver=self.rc['DESWL_VERS']
        self['deswl_load'] = \
            'module unload deswl && module load deswl/%s' % ver

        ver=self.rc['ESUTIL_VERS']
        self['esutil_load'] = \
            'module unload esutil && module load esutil/%s' % ver

    def write(self, check=False):


        expname=self['expname']
        queue = self['queue']

        job_name='se-'+expname.replace('decam-','')
        if check:
            job_file=self['job_file'].replace('.pbs','-check.pbs')
            job_name += '-chk'
        else:
            job_file=self['job_file']

        job_file_base=os.path.basename(job_file).replace('.pbs','')


        rc=deswl.files.Runconfig(self['run'])

        # naming scheme for this generic type figured out from run
        config_file1=deswl.files.get_se_config_path(self['run'], 
                                                    self['expname'], 
                                                    ccd=1)
        config_dir=os.path.dirname(config_file1)
        config_base1=os.path.basename(config_file1)
        conf=config_base1.replace('01-config.yaml','${ccd}-config.yaml')
        if check:
            chk=conf.replace('${ccd}-config.yaml','${ccd}-check.json')
            err=conf.replace('${ccd}-config.yaml','${ccd}-check.err')

            cmd="""
    deswl-check \\
        $dir/{conf} \\
        1> $dir/{chk} \\
        2> $dir/{err}\n"""
            cmd=cmd.format(conf=conf, chk=chk, err=err)
        else:
            # log is now automatically created by GenericProcessor
            # and written into hdfs
            cmd=get_run_command(conf)

        # need -l for login shell because of all the crazy module stuff
        # we have to load
        text = """#!/bin/bash -l
#PBS -q %(queue)s
#PBS -l nodes=1:ppn=1
#PBS -l walltime=02:00:00
#PBS -N %(job_name)s
#PBS -j oe
#PBS -o %(job_file)s.pbslog
#PBS -V
#PBS -A des

if [[ "Y${PBS_O_WORKDIR}" != "Y" ]]; then
    cd $PBS_O_WORKDIR
fi

%(esutil_load)s
%(desdb_load)s
%(deswl_load)s

dir=%(config_dir)s
for ccd in `seq -w 1 62`; do
    echo "ccd: ${ccd}"
    %(cmd)s
done

        \n""" % {'config_dir':config_dir,
                 'esutil_load':self['esutil_load'],
                 'desdb_load':self['desdb_load'],
                 'deswl_load':self['deswl_load'],
                 'cmd':cmd,
                 'queue':queue,
                 'job_file':job_file,
                 'job_name':job_name,'job_file_base':job_file_base}


        eu.ostools.makedirs_fromfile(job_file)
        with open(job_file,'w') as fobj:
            fobj.write(text)

        os.system('chmod u+x %s' % job_file)

