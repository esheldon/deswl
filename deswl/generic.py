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
from math import ceil
import copy
import esutil as eu
import deswl
import desdb

MODFAC=1000

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

        self._df=desdb.files.DESFiles()

        # time to check the outputs
        self.seconds_per_check=1.0

        # these need to be over-ridden
        self.seconds_per=None
        self.timeout=None
        self.filetypes=None
        self.commands=None

    def get_flists(self, **keys):
        """
        The over-ridden version will call get_flists_by_ccd
        or get_flists_by_tile or customize
        """
        raise RuntimeError("you must over-ride get_flists")

    def get_job_name(self, fd):
        raise RuntimeError("you must over-ride get_job_name")

    def get_detrun(self):
        return self.rc.get('detrun',None)

    def _get_allkeys(self, fdict):
        rc=self.rc

        # now interpolate the rest
        allkeys={}
        allkeys.update(rc)

        for k,v in fdict.iteritems():
            if k not in ['input_files','output_files']:
                allkeys[k] = v

        allkeys.update(fdict['input_files'])
        allkeys.update(fdict['output_files'])

        return allkeys

    def get_master_script(self, fdict):
        """
        The user defines the script template and it gets filled here

        We take an fdict that may have some details for a particular
        split, but the meds file is fixed as is config, version, etc.
        """

        template=self._get_master_script_template()
        allkeys=self._get_allkeys(fdict)
        text = template % allkeys

        return text

    def get_master_command(self, fdict, have_detrun=False):
        """
        The user defines the script template and it gets filled here

        We take an fdict that may have some details for a particular
        split, but the meds file is fixed as is config, version, etc.
        """

        allkeys=self._get_allkeys(fdict)
        template=self._get_command_template(have_detrun=have_detrun)
        text=template % allkeys
        return text


    def get_script(self, fdict):
        rc=self.rc

        # note we interpolate commands first
        text = """#!/bin/bash
#PBS -q serial
#PBS -l nodes=1:ppn=1
#PBS -l walltime=%(walltime_hours)d:00:00
#PBS -N %(job_name)s
#PBS -j oe
#PBS -o %(pbslog)s
#PBS -V
#PBS -A des

# this script is auto-generated

# rules for commmands
# - commands is a single string
# - test for errors and use return to indicate a status

function wlpipe_run_code() {
%(commands)s
}

#
# main
#

if [[ "Y${PBS_O_WORKDIR}" != "Y" ]]; then
    cd $PBS_O_WORKDIR
fi

# preamble
%(head)s

log_file=%(log)s
status_file=%(status)s
timeout=%(timeout)d

echo "host: $(hostname)" > $log_file

exit_status=$?

if [[ $exit_status == "0" ]]; then
    wlpipe_run_code >> $log_file 2>&1 
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
        allkeys.update(rc)

        for k,v in fdict.iteritems():
            if k not in ['input_files','output_files']:
                allkeys[k] = v
        allkeys.update(fdict['input_files'])
        allkeys.update(fdict['output_files'])

        allkeys['pbslog']=fdict['script']+'.pbslog'

        job_name=self.get_job_name(fdict)
        allkeys['job_name']=job_name

        commands=self.commands % allkeys
        allkeys['commands']=commands
        allkeys['walltime_hours']=self.calc_walltime_job()

        text = text % allkeys
        return text


    def _extract_tilename(self,fdlist,tilename):
        for fd in fdlist:
            if fd['tilename'] == tilename:
                return fd
        raise ValueError("tilename not found: %d" % tilename)

    def _write_master_script(self, fdict):
        path=self._df.url(type='wlpipe_master_script',run=self['run'])

        print >>stderr,path
        eu.ostools.makedirs_fromfile(path)

        with open(path,'w') as fobj:
            text=self.get_master_script(fdict)
            fobj.write(text)

        cmd='chmod a+x %s' % path
        print >>stderr,cmd
        os.system(cmd)

    def _get_condor_template(self, master_script, overall_name):
        template="""Universe        = vanilla

Notification    = Never 

# Run this exe with these args
Executable      = {master_script}

Image_Size      = 1000000

GetEnv = True

kill_sig        = SIGINT

requirements = (cpu_experiment == "star") || (cpu_experiment == "phenix")
#requirements = (cpu_experiment == "star")

+Experiment     = "astro"
        \n\n"""

        return template.format(overall_name=overall_name,
                               master_script=master_script)

    def _write_condor_me(self, all_fd, missing=False):
        """
        Write the file to do all at once as well as one per tile
        """
        # a dictionary keyed by tilename with all the entries for
        # that tile
        fdd = eu.misc.collect_keyby(all_fd, 'tilename')

        master_script=self._df.url(type='wlpipe_master_script',run=self['run'])

        if missing:
            all_condor_type='wlpipe_me_condor_missing'
            condor_type='wlpipe_me_tile_condor_missing'
        else:
            all_condor_type='wlpipe_me_condor'
            condor_type='wlpipe_me_tile_condor'

        condor_path_all=self._df.url(type=all_condor_type,run=self['run'])
        checker_all=self._df.url(type='wlpipe_me_checker',run=self['run'])

        print condor_path_all,'\n', checker_all
        with open(condor_path_all,'w') as fobjall, open(checker_all,'w') as chall:

            overall_name = self['run']
            text=self._get_condor_template(master_script,overall_name)
            fobjall.write(text)

            num=len(fdd)
            i=0
            for tilename,fdlist in fdd.iteritems():


                condor_path=self._df.url(type=condor_type,
                                         run=self['run'],
                                         tilename=tilename)
                checker_path=self._df.url(type='wlpipe_me_tile_checker',
                                          run=self['run'],
                                          tilename=tilename)
                tdir=self._df.dir(type='wlpipe_tile',
                                  run=self['run'],
                                  tilename=tilename)
                if not os.path.exists(tdir):
                    print >>stderr,'making dir:',tdir
                    os.makedirs(tdir)

                eu.ostools.makedirs_fromfile(condor_path)
                eu.ostools.makedirs_fromfile(checker_path)
                print >>stderr,condor_path

                nwrite=0
                with open(condor_path,'w') as fobj,open(checker_path,'w') as ch:

                    overall_name= fdlist[0]['tilename']
                    text=self._get_condor_template(master_script,overall_name)
                    fobj.write(text)

                    for ifd,fd in enumerate(fdlist):

                        fd['run'] = self['run']

                        # start/end are for 
                        script,status,meta,log=self._extract_tile_files(fd)

                        fd['script'] = script
                        fd['output_files']['log']=log
                        fd['output_files']['meta']=meta
                        fd['output_files']['status']=status

                        if ifd==0:
                            eu.ostools.makedirs_fromfile(log)

                        if missing:
                            ftypes=self.filetypes
                            ok=True
                            for ft in ftypes:
                                path=fd['output_files'][ft]
                                if not os.path.exists(path):
                                    ok=False

                            if ok:
                                continue

                        text=self.get_master_command(fd)

                        job_name="%s-%06d-%06d" % (overall_name,fd['start'],fd['end'])
                        text="""
+job_name = "%s"
Arguments = %s
Queue
                        \n""" % (job_name, text)
                        fobj.write(text)
                        fobjall.write(text)
                        #fobj.write('+job_name = "%s"\n' % job_name)
                        #fobj.write("Arguments = %s\n" % text)
                        #fobj.write("Queue\n\n")

                        #fobjall.write('+job_name = "%s"\n' % job_name)
                        #fobjall.write("Arguments = %s\n" % text)
                        #fobjall.write("Queue\n\n")

                        nwrite+=1

                        for key,o in fd['output_files'].iteritems():
                            chall.write('f="%s"\n' % o)
                            chall.write('if [ ! -e "$f" ]; then echo "missing: $f"; fi\n')

                            ch.write('f="%s"\n' % o)
                            ch.write('if [ ! -e "$f" ]; then echo "missing: $f"; fi\n')
                print >>stderr,nwrite,'were written'
                if nwrite==0:
                    os.remove(condor_path)

                if i==0 or ((i+1) % 10) == 0:
                    print >>stderr,"%d/%d" % (i+1,num)
                i+=1



    def _write_me_command_list_by_tile(self, all_fd):

        # a dictionary keyed by tilename with all the entries for
        # that tile
        fdd = eu.misc.collect_keyby(all_fd, 'tilename')

        script_path=self._df.url(type='wlpipe_master_script',run=self['run'])

        detrun=self.get_detrun()
        detrun_fd = None
        have_detrun=False
        if detrun is not None:
            if self.rc['detband'] != self.rc['band']:
                # note these are the collated files
                detrun_fd = self.get_flists(run=detrun, nper=None)
                have_detrun=True

        if all_fd[0]['start'] is not None:
            dosplit=True
            basename='wlpipe_me_split_'
        else:
            dosplit=False
            basename='wlpipe_me_'


        num=len(fdd)
        i=0
        for tilename,fdlist in fdd.iteritems():


            commands_path=self._df.url(type='wlpipe_me_tile_commands',
                                       run=self['run'],
                                       tilename=tilename)
            minions_path=self._df.url(type='wlpipe_me_tile_minions',
                                      run=self['run'],
                                      tilename=tilename)
            eu.ostools.makedirs_fromfile(commands_path)
            print >>stderr,commands_path
            with open(commands_path,'w') as fobj:

                for ifd,fd in enumerate(fdlist):

                    fd['script_path']=script_path
                    fd['run'] = self['run']

                    # start/end are for 
                    script,status,meta,log=self._extract_tile_files(fd)

                    fd['script'] = script
                    fd['output_files']['log']=log
                    fd['output_files']['meta']=meta
                    fd['output_files']['status']=status

                    if detrun_fd is not None:
                        # copy in collated files
                        dfd = self._extract_tilename(detrun_fd,fd['tilename'])
                        for key,val in dfd['output_files'].iteritems():
                            new_key = '%s_detband' % (key,)
                            fd['input_files'][new_key] = val

                    if ifd==0:
                        eu.ostools.makedirs_fromfile(log)

                    text=self.get_master_command(fd, have_detrun=have_detrun)
                    fobj.write(text)
                    fobj.write('\n')
                
            print >>stderr,minions_path
            njobs=len(fdlist)
            self.write_sub_minions(minions_path,commands_path,njobs)

            if i==0 or ((i+1) % 10) == 0:
                print >>stderr,"%d/%d" % (i+1,num)
            i+=1


    def write_by_tile_master(self, tilename=None, missing=False):
        """
        Instead of writing scripts for each job, write out
        a list of commands.

        These commands call a master script with minimal arguments
        """

        all_fd = self.get_flists(tilename=tilename)
        self._write_master_script(all_fd[0])

        ppn=self.rc.get('ppn',None)
        if ppn is None:
            self._write_condor_me(all_fd, missing=missing)
        else:
            self._write_me_command_list_by_tile(all_fd, missing=missing)


    def write_by_tile(self, tilename=None):
        """
        Write all scripts by tilename/band

        Send tilename to only write that tile
        """

        all_fd = self.get_flists(tilename=tilename)

        detrun=self.get_detrun()
        detrun_fd = None
        if detrun is not None:
            detband=self.rc.get('detband',None)
            if self.rc['detband'] != self.rc['band']:
                # note these are the collated files
                detrun_fd = self.get_flists(run=detrun, nper=None, tilename=tilename)

        if all_fd[0]['start'] is not None:
            dosplit=True
            basename='wlpipe_me_split_'
        else:
            dosplit=False
            basename='wlpipe_me_'

        #if tilename is not None:
        #    tfd=all_fd
        #    all_fd = [fd for fd in tfd if fd['tilename']==tilename]

        ne=len(all_fd)
        modnum=ne/MODFAC
        if modnum <= 0:
            modnum=1
        for i,fd in enumerate(all_fd):

            fd['run'] = self['run']

            # start/end are for 
            script,status,meta,log=self._extract_tile_files(fd)

            fd['script'] = script
            fd['output_files']['log']=log
            fd['output_files']['meta']=meta
            fd['output_files']['status']=status

            if detrun_fd is not None:
                # copy in collated files
                dfd = self._extract_tilename(detrun_fd,fd['tilename'])
                for key,val in dfd['output_files'].iteritems():
                    new_key = '%s_detband' % (key,)
                    fd['input_files'][new_key] = val

            if i==0 or (i % modnum) == 0:
                print >>stderr,"%d/%d" % (i+1,ne)
                print >>stderr,"    %s" % meta
                print >>stderr,"    %s" % fd['script']

            self._write_meta_and_script_single(fd)

    def _extract_tile_files(self, fd):
        #import pprint
        run=self['run']
        tilename=fd['tilename']
        if 'start' in fd:
            extra='_split'
            start,end=extract_start_end(**fd)
        else:
            extra=''
            start=None
            end=None

        band=fd['band']
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

    def get_flists_by_tile(self, run=None, nper=None, tilename=None):
        """
        For each tile and band, get the input and outputs
        files and some other data.  Return as a list of dicts

        If run is not sent, then the rc of the current run
        and nper are used.  If run is sent, that runconfig
        will be loaded and nper will be taken from the keyword.

        The sub-modules will create a get_flists() function
        that calls this
        """
        
        if run is None:
            # using current run, we always honor the requested nper
            # if set
            run=self['run']
            rc = self.rc
            nper=rc.get('nper',None)
        else:
            rc = deswl.files.Runconfig(run)

        df=desdb.files.DESFiles()

        band=rc['band']
        withbands=rc.get('withbands', band)

        release=rc['dataset']
        print 'getting coadd info by release'
        print 'releases:',release
        if isinstance(band,list):
            band_is_list=True
            useband='i'
        else:
            band_is_list=False
            useband=band


        d=self._df.dir(type='wlpipe_flists', run=self['run'])
        if isinstance(release,list):
            rstr='-'.join(release)
        else:
            rstr=release
        fname='%s-coadd-info-%s-%s' % (self['run'],rstr, useband)
        bstr='-'.join(band)
        fname='%s-%s' % (fname, bstr)

        fname=fname+'.json'
        path=os.path.join(d, fname)

        #if not os.path.exists(path):
        if True:
            #print 'cache not found, generating coadd info list'

            flists0 = desdb.files.get_coadd_info_by_release(release,
                                                            useband,
                                                            withbands=withbands)


            if tilename is not None:
                tfd=flists0
                flists0 = [fd for fd in tfd if fd['tilename']==tilename]

            medsconf=rc['medsconf']

            flists=[]
            for fd0 in flists0:

                tilename=fd0['tilename']

                fd0['run'] = run
                fd0['medsconf']=medsconf
                fd0['timeout'] = self.timeout

                if band_is_list:
                    meds_files=[]

                    input_files={}
                    for b in band:
                        n='meds_'+b
                        meds_file=df.url('meds',
                                         coadd_run=fd0['coadd_run'],
                                         medsconf=fd0['medsconf'],
                                         tilename=tilename,
                                         band=b)

                        input_files[n] = meds_file
                        meds_files.append(meds_file)
                    fd0['meds'] = ','.join(meds_files)
     
                else:
                    meds_file=df.url('meds',
                                     coadd_run=fd0['coadd_run'],
                                     medsconf=fd0['medsconf'],
                                     tilename=tilename,
                                     band=band)
                    input_files={'meds':meds_file}
                fd0['input_files'] = input_files

                if nper:
                    fd0['nper']=nper
                    fd_bychunk=self._set_me_outputs_by_chunk(run, fd0, self.filetypes)
                    flists += fd_bychunk
                else:
                    # copies into fd0
                    self._set_me_outputs(run, fd0, self.filetypes)
                    fd0['start']=None
                    fd0['end']=None
                    fd0['nobj']=None
                    flists.append(fd0)

            #print 'writing cache:',path
            #eu.io.write(path, flists)
        else:
            print 'reading cache:',path
            flists=eu.io.read(path)

        self.flists = flists
        return flists

    def _set_me_outputs(self, run, fd, filetypes, start=None, end=None):
        tilename=fd['tilename']
        band=fd['band']

        fd['output_files']=self.get_me_outputs(filetypes,
                                               run=run,
                                               tilename=tilename,
                                               band=band,
                                               start=start,
                                               end=end)


    def _set_me_outputs_by_chunk(self, run, fd0, filetypes):
        nper=fd0['nper']
        nrows=self._get_nrows(fd0['cat_url'])
        startlist,endlist=get_chunks(nrows, nper)

        flists=[]
        for start,end in zip(startlist,endlist):
            fd=copy.deepcopy(fd0)

            self._set_me_outputs(run, fd, filetypes, start=start, end=end)

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

        run: 
            processing run
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

        run=keys['run']
        tilename=keys['tilename']
        band=keys['band']
        start,end=extract_start_end(**keys)

        if start is not None:
            type='wlpipe_me_split'
        else:
            type='wlpipe_me_generic'

        fdict={}
        for ftype in filetypes:
            ext=filetypes[ftype]['ext']
            fdict[ftype] = self._df.url(type=type,
                                        run=run,
                                        tilename=tilename,
                                        band=band,
                                        filetype=ftype,
                                        ext=ext,
                                        start=start,
                                        end=end)
        return fdict

    def write_by_ccd_master(self):
        """
        Instead of writing scripts for each job, write out
        a list of commands.

        These commands call a master script with minimal arguments
        """

        all_fd = self.get_flists()
        num=len(all_fd)
        df=self._df

        self._write_master_script(all_fd[0])

        # also makes directory
        minions_path=df.url(type='wlpipe_minions', run=self['run'])
        commands_path=df.url(type='wlpipe_commands', run=self['run'])
        self.write_sub_minions(minions_path,commands_path,num)

        self._write_se_command_list_by_ccd(all_fd)


    def _write_se_command_list_by_ccd(self, all_fd):
        num=len(all_fd)
        df=self._df

        commands_path=df.url(type='wlpipe_commands', run=self['run'])
        with open(commands_path,'w') as fobj:
            for i,fd in enumerate(all_fd):
                
                run=self['run']
                expname=fd['expname']
                ccd=fd['ccd']

                # not all of these will be used by every pipeline
                log_file=df.url('wlpipe_se_log',
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
                eu.ostools.makedirs_fromfile(log_file)

                fd['output_files']['log']=log_file

                # need to make these optional
                fd['output_files']['meta']=meta_file
                fd['output_files']['status']=status_file

                text=self.get_master_command(fd)
                fobj.write(text)
                fobj.write('\n')

                if i==0 or (i % 10000) == 0:
                    print >>stderr,"%d/%d" % (i,num)


    def write_by_ccd(self):
        """
        Write all scripts by expname/ccd
        """

        df=self._df

        all_fd = self.get_flists()
        i=1
        ne=len(all_fd)
        modnum=ne/MODFAC
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

            if i==0 or (i % modnum) == 0:
                print >>stderr,"%d/%d" % (i,ne)
                print >>stderr,"    %s" % meta_file
                print >>stderr,"    %s" % fd['script']

            self._write_meta_and_script_single(fd)

    def get_flists_by_ccd(self, **keys):
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
            if 'red_bkg' in fd:
                # in this case we already have what we need
                fd['input_files'] = {'image': fd['red_image'],
                                     'bkg':   fd['red_bkg'],
                                     'cat':   fd['red_cat']}

            else:
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

            release = self.rc['dataset']
            bands = self.rc['band']
            if 'coadd' in release[0]:
                print 'getting runs/expnames associated with coadd'
                flists = desdb.files.get_coadd_srclist_by_release(release, bands)
            else:
                flists = desdb.files.get_red_info_by_release(release, bands=bands)
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

    def calc_walltime_job(self):
        if hasattr(self, 'walltime_job_hours'):
            return self.walltime_job_hours
        else:
            seconds_per_job=self.seconds_per
            walltime_hours=seconds_per_job/3600.
            walltime_hours=int(ceil(walltime_hours))
            return walltime_hours



    def calc_minions_walltime(self, ncpu, njobs=None, check=False):
        """
        If njobs is not sent, then the length entire list is used

        If check=True, use the time expected
        to for each check
        """

        if njobs is None:
            flists=self.get_flists()
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
        print '  njobs:',njobs
        print '  ncpu:',ncpu
        print '  walltime:',walltime
        return walltime

    def write_sub_minions(self, job_file, commands_file, njobs):
        ppn=self.rc.get('ppn',None)
        if ppn is not None:
            self.write_sub_minions_pbs(job_file, commands_file, njobs)
        else:
            self.write_sub_minions_wq(job_file, commands_file, njobs)

    def write_sub_minions_wq(self, job_file, commands_file, njobs):
        print 'no wq minions submit available yet'
        pass

    def write_sub_minions_pbs(self, job_file, commands_file, njobs):
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
        walltime=self.calc_minions_walltime(ncpu, njobs=njobs)

        queue=self.get('queue','regular')

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
mpirun -np {ncpu} minions < {commands_file}

echo "done minions"
        \n"""

        cbase=os.path.basename(commands_file)
        minions_text=minions_text.format(job_name=job_name,
                                         nodes=nodes,
                                         ppn=ppn,
                                         ncpu=ncpu,
                                         walltime=walltime,
                                         queue=queue,
                                         commands_file=cbase,
                                         job_file=job_file)

        print 'Writing minions pbs file:',job_file
        eu.ostools.makedirs_fromfile(job_file)
        with open(job_file,'w') as fobj:
            fobj.write(minions_text)
 
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
        walltime=self.calc_minions_walltime(ncpu)

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
mpirun -np {ncpu} minions < commands.txt

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
        walltime=self.calc_minions_walltime(ncpu,check=True)

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
mpirun -np {ncpu} minions < check-commands.txt

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
 



    def _get_nrows(self, cat_file):
        import fitsio
        with fitsio.FITS(cat_file) as fobj:
            nrows=fobj[1].get_nrows()
        return nrows

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

        start,end=extract_start_end(start=start,end=end)
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
            job_name='%06d-%06d' % (self['start'],self['end'])
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

def extract_start_end(**keys):
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

def get_chunks(nrow, nper):
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


