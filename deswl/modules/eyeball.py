import os
from deswl import generic, files
import desdb

def make_sqlite_database(run):
    sm=SqliteMaker(run)
    sm.go()

class EyeballScripts(generic.GenericScripts):
    def __init__(self, run, **keys):
        super(EyeballScripts,self).__init__(run)

        # 5 minutes is plenty
        self.timeout=5*60*60

        # typical is 30 seconds
        self.seconds_per = 40

        # don't put status, meta, or log here, they will get
        # over-written
        # using optional typename to allow different filetypes to have
        # same type name but different extensions
        #self.filetypes={'mosaic_jpg':  {'typename':'mosaic', 'ext':'jpg'},
        #                'mosaic_fits': {'typename':'mosaic', 'ext':'fits.fz'},
        #                'field_jpg2':  {'typename':'field2', 'ext':'jpg'},
        #                'field_jpg4':  {'typename':'field4', 'ext':'jpg'}}
        self.filetypes={'field_fits':  {'typename':'field', 'ext':'fits.fz'}}

        # don't need this, will just load my normal modules
        self.module_uses=None
        self.modules=None
        self.commands=self._get_commands()

    def get_flists(self):
        return self.get_flists_by_ccd()

    def get_job_name(self, fd):
        job_name='%s-%02d' % (fd['expname'],fd['ccd'])
        job_name=job_name.replace('DECam_','')
        return job_name


    def _get_commands(self):
        """
        timeout and log_file are defined on entry
        """
        
        # indent because this is written within the function
        commands="""
    #nsetup_ess
    source ~/.bashrc

    image=%(image)s
    bkg=%(bkg)s
    field_fits=%(field_fits)s

    vers="%(version)s"
    module unload eyeballer && module load eyeballer/${vers}

    python $EYEBALLER_DIR/bin/make-se-eyeball.py \\
            ${image} ${bkg} ${field_fits} 2>&1 >> $log_file

    exit_status=$?
    
    return $exit_status
        """

        return commands

    def _get_command_template(self, **keys):
        """
        no newlines allowed!
        """
        t="./master.sh %(image)s %(bkg)s %(field_fits)s &> %(log)s"
        return t


    def _get_master_script_template(self):
        commands="""#!/bin/bash

#nsetup_ess
source ~/.bashrc

if [ $# -lt 3 ]; then
    echo "error: master.sh image bkg field_fits"
    exit 1
fi

# this can be a list
image="$1"
bkg="$2"
field_fits="$3"

vers="%(version)s"
module unload eyeballer && module load eyeballer/${vers}

python $EYEBALLER_DIR/bin/make-se-eyeball.py ${image} ${bkg} ${field_fits}

exit_status=$?
exit $exit_status\n"""

        return commands

def get_sqlite_dir(run):
    df = desdb.files.DESFiles()
    dir = df.dir(type='wlpipe_run', run=run)
    dir = os.path.join(dir, 'db')
    return dir

def get_sqlite_url(run):
    dir=get_sqlite_dir(run)
    name='%s.db' % run
    url=os.path.join(dir, name)
    return url

def _make_dir(dir):
    try:
        os.makedirs(dir)
    except:
        pass

class SqliteMaker(object):
    def __init__(self, run):
        self.run=run

        self.files_table='files'
        self.files_index_fields=['ccdname','expname','ccd','band']
        self.qa_table='qa'
        self.qa_index_fields=['userid','fileid','score','comments']

        self.rc = files.Runconfig(self.run)
        self._open_connection()
        os.chdir(self.dir)

    def go(self):
        #self.make_qa_table()
        self.make_files_table()
        self.populate_files_table()

        self.add_indices(self.files_table, self.files_index_fields)
        #self.add_indices(self.qa_table, self.qa_index_fields)
    
    def add_indices(self, tablename, index_fields):
        curs=self.conn.cursor()


        for field in index_fields:
            idname='%s_%s_idx' % (tablename, field)
            query="CREATE INDEX {idname} ON {tablename} ({field})"
            query=query.format(idname=idname,
                               tablename=tablename,
                               field=field)
            print query
            curs.execute(query)

        curs.close()
        self.conn.commit()

    def populate_files_table(self):
        import glob
        print 'populating files table'

        fzlist = glob.glob('../*/*field.fits.fz')
        nf=len(fzlist)
        band=self.rc['band']

        insert_query="""
        INSERT INTO {tablename} VALUES (?, ?, ?, ?, ?)
        """.format(tablename=self.files_table)

        curs=self.conn.cursor()
        for i,fzfile in enumerate(fzlist):
            if (i % 1000) == 0:
                print '    %d/%d' % (i+1,nf)

            bname=os.path.basename(fzfile)
            sp=bname.split('_')
            expname='%s_%s' % (sp[2], sp[3])
            ccdname='%s_%s_%s' % (sp[2], sp[3], sp[4])
            ccd = int(sp[4])

            data=(ccdname, expname, ccd, band, fzfile)

            curs.execute(insert_query, data)
         
        curs.close()
        self.conn.commit()

    def make_files_table(self):
        curs=self.conn.cursor()

        q="""
create table {tablename} (
    ccdname text,
    expname text,
    ccd integer,
    band text,
    field text
)
        """.format(tablename=self.files_table)

        print q
        curs.execute(q)
        curs.close()
        self.conn.commit()

    def make_qa_table(self):
        curs=self.conn.cursor()

        q="""
create table {tablename} (
    userid int,
    fileid int,
    score int,
    comments text
)
        """.format(tablename=self.qa_table)

        print q
        curs.execute(q)

        curs.close()
        self.conn.commit()

    def _open_connection(self):
        import sqlite3 as sqlite
        self.url=get_sqlite_url(self.run)
        self.dir=get_sqlite_dir(self.run)

        _make_dir(self.dir)

        if os.path.exists(self.url):
            print 'removing existing:',self.url
            os.remove(self.url)
        
        print 'opening database:',self.url
        self.conn=sqlite.Connection(self.url)


