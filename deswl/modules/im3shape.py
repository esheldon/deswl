from deswl import generic
import desdb
import copy

# don't put status, meta, or log here, they will get
# over-written
ME_FILETYPES={'raw':{'ext':'txt'},
              'clean':{'ext':'txt'}}

# I'm taking 3 seconds per object and 120 
# objects per chunk, and 10 images per object.
# that would give 1 hour, so 3 is plenty
ME_TIMEOUT=3*60*60 # seconds


class I3MEScripts(generic.GenericScripts):
    def __init__(self, run, **keys):
        super(I3MEScripts,self).__init__(run)

    def get_flists(self):
        """
        im3shape is slow so we use the chunking functionality
        """
        if self.flists is not None:
            return self.flists

        df=desdb.files.DESFiles()

        print 'getting coadd info by release'
        flists0 = desdb.files.get_coadd_info_by_release(self.rc['dataset'],
                                                       self.rc['band'])

        medsconf=self.rc['medsconf']
        nper=self.rc['nper']

        flists=[]
        for fd0 in flists0:

            tilename=fd0['tilename']
            band=fd0['band']

            fd0['run'] = self['run']
            fd0['medsconf']=medsconf
            fd0['nper']=nper
            fd0['timeout'] = ME_TIMEOUT

            meds_file=df.url('meds',
                             coadd_run=fd0['coadd_run'],
                             medsconf=fd0['medsconf'],
                             tilename=tilename,
                             band=band)

            fd0['input_files'] = {'meds':meds_file}

            nrows=self._get_nrows(fd0['cat_url'])
            startlist,endlist=self._get_chunks(nrows, nper)

            for start,end in zip(startlist,endlist):
                fd=copy.deepcopy(fd0)

                fd['output_files']=self.get_me_outputs(ME_FILETYPES,
                                                       tilename=tilename,
                                                       band=band,
                                                       start=start,
                                                       end=end)
                fd['start'] = start
                fd['end'] = end

                flists.append( fd )


        self.flists = flists
        return flists

    def get_script(self, fdict):
        rc=self.rc
        load='module load im3shape/%s'
        load = load % rc['IM3SHAPE_VERS']


        # note only the {key} are set at this time
        text = """#!/bin/bash
#PBS -q serial
#PBS -l nodes=1:ppn=1
#PBS -l walltime=3:00:00
#PBS -N %(job_name)s
#PBS -j oe
#PBS -o %(pbslog)s
#PBS -V
#PBS -A des

# stdout goes to the log file
%(load)s

meds_file=%(meds)s
raw_file=%(raw)s
clean_file=%(clean)s

timeout=%(timeout)d

status_file=%(status)s
log_file=%(log)s

export OMP_NUM_THREADS=1

echo "host: $(hostname)" > $log_file

command="
    %(script_path)s >> $log_file
"

echo "time-seconds: $SECONDS" >> $log_file

mess="writing status $exit_status to:
    $status_file"
echo $mess >> $log_file

echo "$exit_status" > "$status_file"
exit $exit_status
        \n"""

        # now interpolate the rest
        allkeys={}
        allkeys['load'] = load
        for k,v in fdict.iteritems():
            if k not in ['input_files','output_files']:
                allkeys[k] = v
        for k,v in fdict['input_files'].iteritems():
            allkeys[k] = v
        for k,v in fdict['output_files'].iteritems():
            allkeys[k] = v

        allkeys['pbslog']=fdict['script']+'.pbslog'

        job_name='%s-%s' % (fdict['tilename'],fdict['band'])
        allkeys['job_name']=job_name

        text = text % allkeys
        return text

