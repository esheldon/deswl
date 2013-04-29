"""

In short
    - Create a module for the code and add loading of it in deswl-gen-pbs
    - Generate a runconfig
        - if you are splitting the processing of meds files, 
          send --nper  

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

    - $DESDATA/wlpipe//{run}/{expname}/{run}-{expname}-{ccd}.log

Similarly, the GenericProcessor writes the stat file

    - $DESDATA/wlpipe//{run}/{expname}/{run}-{expname}-{ccd}-stat.yaml

The stat file holds the exit_status and whatever was in the config file.

The collation step is not yet implemented; probably each owner of the code will
have to do this step.
"""
import os
from sys import stderr
import copy
import esutil as eu
import deswl
import desdb

class GenericScripts(dict):
    """
    to create and write the metatadata files and scripts

    The user must over-ride the methods
        get_flists
            just call your choise of _by_tile or _by_ccd
        get_job_name
            given a fdict
    And set the member variables
        seconds_per
            seconds per job
        timeout
            timeout for job
        filetypes
            output file types
        module_uses
            module use dirs to add
        modules
            a *list* of modules to load
        commands
            A single string with commands, can be
            multi-line, should do error checking etc.
            See the get_script function for details
    """
    def __init__(self, run, **keys):
        self['run'] = run
        for k,v in keys.iteritems():
            self[k] = v

        # this has serun in it
        self.rc = deswl.files.Runconfig(self['run'])

        self.flists =None
        self._make_module_loads()

        self._df=desdb.files.DESFiles()

        # time to check the outputs
        self.seconds_per_check=1.0

        # these need to be over-ridden
        self.seconds_per=None
        self.timeout=None
        self.filetypes=None
        self.modules=None
        self.module_uses=None
        self.commands=None

    def get_flists(self):
        """
        The over-ridden version will call get_flists_by_ccd
        or get_flists_by_tile or customize
        """
        raise RuntimeError("you must over-ride get_flists")

    def get_job_name(self, fd):
        raise RuntimeError("you must over-ride get_job_name")

    def get_script(self, fdict):
        rc=self.rc

        # note we interpolate commands first
        text = """#!/bin/bash
#PBS -q serial
#PBS -l nodes=1:ppn=1
#PBS -l walltime=3:00:00
#PBS -N %(job_name)s
#PBS -j oe
#PBS -o %(pbslog)s
#PBS -V
#PBS -A des

# this script is auto-generated

function wlpipe_module_use {
    for mod_dir; do
        module use $mod_dir 2>&1 >> $log_file
    done
}
# workaround because the module command does
# not indicate an error status
function wlpipe_load_modules() {
    for mod; do
        module load $mod 2>&1 >> $log_file

        res=$(module show $mod 2>&1 | grep -i error)
        if [[ $res != "" ]]; then
            return 1
        fi
    done
    return 0
}

# rules for commmands
# - before entry these are set $timeout, $log_file
# - commands is a single string
# - test for errors and use return to indicate a status
# - append stdout/stderr to the $log_file, e.g. 
#        use 2>&1 >> $log_file
# - commands should be run with 
#        timeout $timeout command...

function wlpipe_run_code() {
%(commands)s
}

#
# main
#

log_file=%(log)s
status_file=%(status)s
timeout=%(timeout)d

echo "host: $(hostname)" > $log_file

%(module_uses)s
%(module_loads)s
exit_status=$?

if [[ $exit_status == "0" ]]; then
    wlpipe_run_code
    exit_status=$?
fi

mess="writing status $exit_status to:
    $status_file"
echo $mess >> $log_file
echo "time-seconds: $SECONDS" >> $log_file

echo "$exit_status" > "$status_file"
exit $exit_status
        \n"""


        # now interpolate the rest
        allkeys={}
        if self.module_uses is not None:
            module_uses=' '.join(self.module_uses)
            module_uses='wlpipe_module_use "%s"' % module_uses
        else:
            module_uses=''

        if self.modules is not None:
            modules=' '.join(self.modules)
            module_loads='wlpipe_load_modules %s'
        else:
            module_loads=''
        allkeys['module_uses']=module_uses
        allkeys['module_loads'] = module_loads



        for k,v in fdict.iteritems():
            if k not in ['input_files','output_files']:
                allkeys[k] = v
        for k,v in fdict['input_files'].iteritems():
            allkeys[k] = v
        for k,v in fdict['output_files'].iteritems():
            allkeys[k] = v

        allkeys['pbslog']=fdict['script']+'.pbslog'

        job_name=self.get_job_name(fdict)
        allkeys['job_name']=job_name

        commands=self.commands % allkeys
        allkeys['commands']=commands

        text = text % allkeys
        return text


    def write_by_tile(self):
        """
        Write all scripts by tilename/band
        """

        all_fd = self.get_flists()
        if all_fd[0]['start'] is not None:
            dosplit=True
            basename='wlpipe_me_split_'
        else:
            dosplit=False
            basename='wlpipe_me_'

        i=1
        ne=len(all_fd)
        modnum=ne/10
        for i,fd in enumerate(all_fd):

            # start/end are for 
            script,status,meta,log=self._extract_tile_files(fd)

            fd['script'] = script
            fd['output_files']['log']=log
            fd['output_files']['meta']=meta
            fd['output_files']['status']=status

            ii=i+1
            if ii==1 or (ii % modnum) == 0:
                print >>stderr,"%d/%d" % (ii,ne)
                print >>stderr,"    %s" % meta
                print >>stderr,"    %s" % fd['script']

            self._write_meta_and_script_single(fd)

    def _extract_tile_files(self, fd):
        run=fd['run']
        tilename=fd['tilename']
        band=fd['band']
        if 'start' in fd:
            extra='_split'
            start,end=_extract_start_end(**fd)
        else:
            extra=''
            start=None
            end=None

        df=self._df
        script=df.url('wlpipe_me_script'+extra,
                      run=run,
                      tilename=tilename,
                      band=band,
                      start=start,
                      end=end)

        status=df.url('wlpipe_me_status'+extra,
                      run=run,
                      tilename=tilename,
                      band=band,
                      start=start,
                      end=end)

        meta=df.url('wlpipe_me_meta'+extra,
                    run=run,
                    tilename=tilename,
                    band=band,
                    start=start,
                    end=end)
        log=df.url('wlpipe_me_log'+extra,
                   run=run,
                   tilename=tilename,
                   band=band,
                   start=start,
                   end=end)
        return script, status, meta, log

    def get_flists_by_tile(self):
        """
        For each tile and band, get the input and outputs
        files and some other data.  Return as a list of dicts

        If nper is set, split into chunks

        The sub-modules will create a get_flists() function
        that calls this
        """
        if self.flists is not None:
            return self.flists

        df=desdb.files.DESFiles()

        print 'getting coadd info by release'
        flists0 = desdb.files.get_coadd_info_by_release(self.rc['dataset'],
                                                        self.rc['band'])

        medsconf=self.rc['medsconf']
        nper=self.rc.get('nper',None)

        flists=[]
        for fd0 in flists0:

            tilename=fd0['tilename']
            band=fd0['band']

            fd0['run'] = self['run']
            fd0['medsconf']=medsconf
            fd0['timeout'] = self.timeout

            meds_file=df.url('meds',
                             coadd_run=fd0['coadd_run'],
                             medsconf=fd0['medsconf'],
                             tilename=tilename,
                             band=band)

            fd0['input_files'] = {'meds':meds_file}

            if nper:
                fd0['nper']=nper
                fd_bychunk=self._set_me_outputs_by_chunk(fd0,
                                                         self.filetypes)
                flists += fd_bychunk
            else:
                self._set_me_outputs(fd0, self.filetypes)
                fd0['start']=None
                fd0['end']=None
                fd0['nobj']=None
                flists.append(fd0)

        self.flists = flists
        return flists

    def _set_me_outputs(self, fd, filetypes, start=None, end=None):
        tilename=fd['tilename']
        band=fd['band']

        fd['output_files']=self.get_me_outputs(filetypes,
                                               tilename=tilename,
                                               band=band,
                                               start=start,
                                               end=end)


    def _set_me_outputs_by_chunk(self, fd0, filetypes):
        nper=fd0['nper']
        nrows=self._get_nrows(fd0['cat_url'])
        startlist,endlist=self._get_chunks(nrows, nper)

        flists=[]
        for start,end in zip(startlist,endlist):
            fd=copy.deepcopy(fd0)

            self._set_me_outputs(fd, filetypes, start=start, end=end)

            fd['start'] = start
            fd['end'] = end
            fd['nobj'] = end-start+1

            flists.append( fd )

        return flists


    def get_me_outputs(self, filetypes, **keys):
        """
        Generate output file names for me processing.

        parameters
        ----------
        filetypes: dict of dicts
            Keyed by file type. The sub-dicts should have the 'ext' key

        tilename: string, keyword
            The tilename as a string, e.g. 'DES0652-5622'
        band: string, keyword
            the band as a string, e.g. 'i'

        start: int, optional
            A start index for processing the MEDS file.  You must
            send both start and end
        end: int, optional
            An end index for processing the MEDS file.  You must
            send both start and end
        """

        tilename=keys['tilename']
        band=keys['band']
        start,end=_extract_start_end(**keys)

        if start is not None:
            type='wlpipe_me_split'
        else:
            type='wlpipe_me_generic'

        fdict={}
        for ftype in filetypes:
            ext=filetypes[ftype]['ext']
            fdict[ftype] = self._df.url(type=type,
                                        run=self['run'],
                                        tilename=tilename,
                                        band=band,
                                        filetype=ftype,
                                        ext=ext,
                                        start=start,
                                        end=end)
        return fdict

    def write_by_ccd(self):
        """
        Write all scripts by expname/ccd
        """

        df=self._df

        all_fd = self.get_flists()
        i=1
        ne=len(all_fd)
        modnum=ne/10
        for i,fd in enumerate(all_fd):
            run=fd['run']
            expname=fd['expname']
            ccd=fd['ccd']

            script_file=df.url('wlpipe_se_script',
                               run=run,
                               expname=expname,
                               ccd=ccd)

            status_file=df.url('wlpipe_se_status',
                               run=run,
                               expname=expname,
                               ccd=ccd)

            meta_file=df.url('wlpipe_se_meta',
                             run=run,
                             expname=expname,
                             ccd=ccd)
            log_file=df.url('wlpipe_se_log',
                            run=run,
                            expname=expname,
                            ccd=ccd)

            fd['script'] = script_file
            fd['output_files']['log']=log_file
            fd['output_files']['meta']=meta_file
            fd['output_files']['status']=status_file

            ii=i+1
            if ii==1 or (ii % modnum) == 0:
                print >>stderr,"%d/%d" % (ii,ne)
                print >>stderr,"    %s" % meta_file
                print >>stderr,"    %s" % fd['script']

            self._write_meta_and_script_single(fd)

    def get_flists_by_ccd(self):
        """
        For each expname and ccd, get the input and outputs
        files and some other data.  Return as a list of dicts

        The sub-modules will create a get_flists() function
        that calls this
        """

        if self.flists is not None:
            return self.flists

        flists = self.cache_flists_by_ccd()
        for fd in flists:
            expname=fd['expname']
            ccd=fd['ccd']

            fd['run'] = self['run']
            fd['bkg_url']=fd['image_url'].replace('.fits.fz','_bkg.fits.fz')
            fd['cat_url']=fd['image_url'].replace('.fits.fz','_cat.fits')
            fd['input_files'] = {'image':fd['image_url'],
                                 'bkg':fd['bkg_url'],
                                 'cat':fd['cat_url']}
            fd['output_files']=self.get_se_outputs(self.filetypes,
                                                   expname=expname,
                                                   ccd=ccd)

            fd['timeout'] = self.timeout


        self.flists = flists
        return flists

    def cache_flists_by_ccd(self):
        """
        Get raw red info
        """
        fname=self._df.url(type='wlpipe_flist_red', run=self['run'])
        if not os.path.exists(fname):
            print 'cache not found, generating raw red info list'
            eu.ostools.makedirs_fromfile(fname)
            flists = desdb.files.get_red_info_by_release(self.rc['dataset'],
                                                         self.rc['band'])
            print 'writing cache:',fname
            eu.io.write(fname, flists)
        else:
            print 'reading cache:',fname
            flists = eu.io.read(fname)

        return flists


    def get_se_outputs(self, filetypes, **keys):
        """
        Generate output file names for me processing.

        parameters
        ----------
        filetypes: dict of dicts
            Keyed by file type. The sub-dicts should have the 'ext' key

        expname: string, keyword
            The exposurename as a string,, e.g. 'DECam_00154939'
        ccd: int, keyword
            The ccd as an integer keyword keyword
        """

        expname=keys['expname']
        ccd=keys['ccd']
        fdict={}
        for ftype in filetypes:
            tinfo=filetypes[ftype]
            ext=tinfo['ext']
            if 'typename' in tinfo:
                tname=tinfo['typename']
            else:
                tname=ftype
            fdict[ftype] = self._df.url(type='wlpipe_se_generic',
                                        run=self['run'],
                                        expname=expname,
                                        ccd=ccd,
                                        filetype=tname,
                                        ext=ext)
        return fdict


    def _write_meta_and_script_single(self, fd):

        # this is in the output directory, so we are good from
        # here on!
        meta_file=fd['output_files']['meta']
        script_file=fd['script']

        eu.ostools.makedirs_fromfile(meta_file)
        eu.ostools.makedirs_fromfile(script_file)

        eu.io.write(meta_file, fd)

        with open(script_file,'w') as fobj:
            script_data=self.get_script(fd)
            fobj.write(script_data)
        os.system('chmod u+x %s' % script_file)




    def write_mpi_script(self, fd):
        """
        not used
        """
        config_file=deswl.files.get_se_config_path(fd['run'],
                                                   fd['expname'],
                                                   fd['ccd'])

        minion_file=deswl.files.get_se_minion_path(self['run'],
                                                   fd['expname'],
                                                   fd['ccd'])
 
        job_name='%s_%02d' % (fd['expname'],fd['ccd'])
        job_name=job_name.replace('decam-','')
        job_name=job_name.replace('DECam_','')

        cmd=get_run_command(config_file)
        text="""#!/bin/bash -l
#PBS -q serial
#PBS -l nodes=1:ppn=1
#PBS -l walltime=30:00
#PBS -N {job_name}
#PBS -j oe
#PBS -o {job_file}.pbslog
#PBS -V
#PBS -A des

# this is usually called as a minion, but with the header
# above can be submitted directly to pbs for checks

{esutil_load}
{desdb_load}
{deswl_load}

{cmd}
        \n""".format(esutil_load=self['esutil_load'],
                     desdb_load=self['desdb_load'],
                     deswl_load=self['deswl_load'],
                     job_file=minion_file,
                     job_name=job_name,
                     cmd=cmd)

       
        eu.ostools.makedirs_fromfile(minion_file)
        with open(minion_file,'w') as fobj:
            fobj.write(text)
        os.system('chmod u+x %s' % minion_file)

    def calc_walltime(self, ncpu, check=False):
        """
        If check=True, use the time expected
        to for each check
        """
        from math import ceil

        flists=self.get_flists()

        # get the total cpu time
        njobs = len(flists)

        if check:
            seconds_per=self.seconds_per_check
        else:
            seconds_per=self.seconds_per

        total_time=seconds_per*njobs

        # the walltime in seconds given our
        # ncpu
        walltime_seconds = total_time/ncpu

        walltime_hours=walltime_seconds/3600.
        walltime_hours=int(ceil(walltime_hours))

        walltime='%d:00:00' % walltime_hours
        print '  ',walltime
        return walltime

    def write_minions(self):
        """
        Batching individual jobs using mpi

        requires the program minions installed
        """
        rc=self.rc
        job_name='%s-minions' % self['run']
        nodes=rc['nodes']
        ppn=rc['ppn']
        ncpu=nodes*ppn

        print 'calculating wall time'
        walltime=self.calc_walltime(ncpu)

        queue=self.get('queue','regular')

        job_file=self._df.url(type='wlpipe_minions',
                              run=self['run'])

        minions_text="""#!/bin/bash -l
#PBS -N {job_name}
#PBS -j oe
#PBS -l nodes={nodes}:ppn={ppn},walltime={walltime}
#PBS -q {queue}
#PBS -o {job_file}.pbslog
#PBS -A des

if [[ "Y${{PBS_O_WORKDIR}}" != "Y" ]]; then
    cd $PBS_O_WORKDIR
fi

module load openmpi-gnu
find . -name "*script.pbs" | mpirun -np {ncpu} minions

echo "done minions"
        \n"""

        minions_text=minions_text.format(job_name=job_name,
                         nodes=nodes,
                         ppn=ppn,
                         ncpu=ncpu,
                         walltime=walltime,
                         queue=queue,
                         job_file=job_file)

        print 'Writing minions pbs file:',job_file
        eu.ostools.makedirs_fromfile(job_file)
        with open(job_file,'w') as fobj:
            fobj.write(minions_text)
        
    def write_check_minions(self):
        """
        Batching individual jobs using mpi

        requires the program minions installed
        """
        rc=self.rc
        job_name='%s-check' % self['run']
        nodes=8
        ppn=8
        ncpu=nodes*ppn
        walltime=self.calc_walltime(ncpu,check=True)

        print 'calculating check walltime'
        queue=self.get('queue','regular')

        job_file=self._df.url(type='wlpipe_minions_check',
                              run=self['run'])

        minions_text="""#!/bin/bash -l
#PBS -N {job_name}
#PBS -j oe
#PBS -l nodes={nodes}:ppn={ppn},walltime={walltime}
#PBS -q {queue}
#PBS -o {job_file}.pbslog
#PBS -A des

if [[ "Y${{PBS_O_WORKDIR}}" != "Y" ]]; then
    cd $PBS_O_WORKDIR
fi

module load openmpi-gnu
find . -name "*check.pbs" | mpirun -np {ncpu} minions

echo "done minions"
        \n"""

        minions_text=minions_text.format(job_name=job_name,
                         nodes=nodes,
                         ppn=ppn,
                         ncpu=ncpu,
                         walltime=walltime,
                         queue=queue,
                         job_file=job_file)

        print 'Writing check minions pbs file:',job_file
        eu.ostools.makedirs_fromfile(job_file)
        with open(job_file,'w') as fobj:
            fobj.write(minions_text)
 


    def _get_chunks(self, nrow, nper):
        """
        These are not slices!
        """
        startlist=[]
        endlist=[]

        nchunk, nleft = divmod(nrow, nper)
        if nleft != 0:
            nchunk += 1

        for i in xrange(nchunk):
            start=i*nper
            end=(i+1)*nper-1
            if end > (nrow-1):
                end=nrow-1

            startlist.append(start)
            endlist.append(end)
        return startlist, endlist

    def _get_nrows(self, cat_file):
        import fitsio
        with fitsio.FITS(cat_file) as fobj:
            nrows=fobj[1].get_nrows()
        return nrows

    def _make_module_loads(self):
        """
        Not using these right now
        """
        for module in ['desdb','deswl','esutil']:
            load_key='%s_load' % module
            mup=module.upper()
            vers=self.rc['%s_VERS'% mup]

            lcmd='module unload {module} && module load {module}/{vers}'
            lcmd=lcmd.format(module=module,vers=vers)

            self[load_key] = lcmd


class GenericSEPBSJob(dict):
    """
    only using check scripts at nersc currently

    You should also create the config files for each exposure/ccd using. config
    files go to deswl.files.se_config_path(run,expname,ccd=ccd)

    Send check=True to generate the check file instead of the processing file
    """
    def __init__(self, run, expname, ccd, **keys):

        self['run'] = run
        self['expname'] = expname
        self['ccd'] = ccd
        self['queue'] = keys.get('queue','serial')

        df=desdb.files.DESFiles()
        self._df=df

        self['job_file']= df.url(type='wlpipe_se_script',
                                 run=run,
                                 expname=expname,
                                 ccd=ccd)
        self['check_file']= df.url(type='wlpipe_se_check',
                                   run=run,
                                   expname=expname,
                                   ccd=ccd)

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

        run=self['run']
        expname=self['expname']
        ccd=self['ccd']

        queue = self['queue']

        job_name=expname.replace('decam-','')
        job_name=expname.replace('DECam-','')
        job_name='se-'+job_name

        if check:
            job_file=self['check_file']
            job_name += '-chk'
        else:
            job_file=self['job_file']

        rc=deswl.files.Runconfig(self['run'])

        # naming scheme for this generic type figured out from run
        df=self._df
        meta=df.url(type='wlpipe_se_meta',
                    run=run,
                    expname=expname,
                    ccd=ccd)
        if check:
            chk=job_file[0:job_file.rfind('.')]+'.json'
            err=job_file[0:job_file.rfind('.')]+'.err'

            cmd="""
meta="{meta}"
chk="{chk}"
err="{err}"
deswl-check "$meta" 1> "$chk" 2> "$err"
"""
            cmd=cmd.format(meta=meta, chk=chk, err=err)
        else:
            # log is now automatically created by GenericProcessor
            # and written into hdfs
            cmd=get_run_command(meta)

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

nsetup_ess
%(esutil_load)s
%(desdb_load)s
%(deswl_load)s

%(cmd)s
        \n""" % {'esutil_load':self['esutil_load'],
                 'desdb_load':self['desdb_load'],
                 'deswl_load':self['deswl_load'],
                 'cmd':cmd,
                 'queue':queue,
                 'job_file':job_file,
                 'job_name':job_name}


        eu.ostools.makedirs_fromfile(job_file)
        with open(job_file,'w') as fobj:
            fobj.write(text)

        os.system('chmod u+x %s' % job_file)



class GenericMEChecker(dict):
    """
    Write out scripts that are also PBS scripts for checking outputs.

    I've been calling these as minions at nersc
    """
    def __init__(self, run, tilename, band, start=None, end=None,
                 **keys):

        self['run'] = run
        self['tilename'] = tilename
        self['band'] = band
        self['queue'] = keys.get('queue','serial')

        start,end=_extract_start_end(start=start,end=end)
        self['start']=start
        self['end']=end
        if self['start'] is None:
            self._extra=''
        else:
            self._extra='_split'

        df=desdb.files.DESFiles()
        self._df=df

        extra=self._extra
        self['job_file']= df.url(type='wlpipe_me_script'+extra,
                                 run=run,
                                 tilename=tilename,
                                 band=band,
                                 start=self['start'],
                                 end=self['end'])
        self['check_file']= df.url(type='wlpipe_me_check'+extra,
                                   run=run,
                                   tilename=tilename,
                                   band=band,
                                   start=self['start'],
                                   end=self['end'])

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

    def write(self):

        run=self['run']
        tilename=self['tilename']
        band=self['band']

        queue = self['queue']

        if self['start'] is not None:
            job_name='%s-%s' % (self['start'],self['end'])
        else:
            job_name='%s-%s' % (tilename,band)

        job_file=self['check_file']

        rc=deswl.files.Runconfig(self['run'])

        # naming scheme for this generic type figured out from run
        df=self._df
        extra=self._extra
        meta=df.url(type='wlpipe_me_meta'+extra,
                    run=run,
                    tilename=tilename,
                    band=band,
                    start=self['start'],
                    end=self['end'])
        chk=job_file[0:job_file.rfind('.')]+'.json'
        err=job_file[0:job_file.rfind('.')]+'.err'

        cmd="""
meta="{meta}"
chk="{chk}"
err="{err}"
deswl-check "$meta" 1> "$chk" 2> "$err"
"""
        cmd=cmd.format(meta=meta, chk=chk, err=err)

        # need -l for login shell because of all the crazy module stuff
        # we have to load
        text = """#!/bin/bash -l
#PBS -q %(queue)s
#PBS -l nodes=1:ppn=1
#PBS -l walltime=00:10:00
#PBS -N %(job_name)s
#PBS -j oe
#PBS -o %(job_file)s.pbslog
#PBS -V
#PBS -A des

if [[ "Y${PBS_O_WORKDIR}" != "Y" ]]; then
    cd $PBS_O_WORKDIR
fi

nsetup_ess
%(esutil_load)s
%(desdb_load)s
%(deswl_load)s

%(cmd)s
        \n""" % {'esutil_load':self['esutil_load'],
                 'desdb_load':self['desdb_load'],
                 'deswl_load':self['deswl_load'],
                 'cmd':cmd,
                 'queue':queue,
                 'job_file':job_file,
                 'job_name':job_name}


        eu.ostools.makedirs_fromfile(job_file)
        with open(job_file,'w') as fobj:
            fobj.write(text)

        os.system('chmod u+x %s' % job_file)



def get_run_command(config_file):
    return 'deswl-run %s' % config_file

def _extract_start_end(**keys):
    """
    get start and end as strings
    """
    start=keys.get('start',None)
    end=keys.get('end',None)
    if ((start is not None and end is None)
            or 
            (start is None and end is not None)):
        raise ValueError("send both start= and end= or neither")

    if start is not None:
        start='%06d' % start
        end='%06d' % end
    return start, end



# not using these right now
class GenericProcessor(dict):
    def __init__(self, config):
        """
        not using this at nersc

        required keys in config
            - 'run' The run id, just for identification
            - 'input_files', which is itself a dictionary.  The files will be
            checked to see if any files are in hdfs and if so these will be 
            staged out.

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
        raise ValueError("fix paths")
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


