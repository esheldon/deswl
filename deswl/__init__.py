"""

MEDS creation
-------------

First get the list of coadd runs.  There is no systematic way
currently.

Then get the runs associated for the input SE images using this
script from the desdb product.
    get-coadd-srcruns-by-run 

Do download use the script
    des-sync-red run
which can be used with gnu parallel.

If you want more parallelism you can send --byexp to get run exposurename
columns. Then you need to tell parallel you have columns

    cat run-explist.txt | parallel -u -C -j 3 "des-sync-red --expname {2} {1}"

Once you have the data you need to generate the source list and scripts using
programs from the deswl package.  You need to specify an owner if you don't
want it to be wlpipe

medconf=001
whose=ess
band=i
for run in $(cat runlist.txt); do
    echo "--------------------"
    deswl-gen-meds-srclist $medconf $run $band
    deswl-gen-meds-script --whose $whose $medconf $run $band
    deswl-gen-meds-pbs $medconf $run $band
done

Then just submit the pbs scripts, which run make-meds-input and make-cutouts.
Note make-cutouts lives in the shapelets code base; make-meds-input is from
deswl.

The exit status is put in the meds_status file, a yaml file.  I need to write
the code for checking exit status.

"""
import sys
from sys import stdout, stderr


def get_external_version(version_command):
    """
    Run a command to get a version string and return it through the
    standard output
    """
    import subprocess
    pobj=subprocess.Popen(version_command, shell=True, 
                          stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    # this will wait until process terminates
    out=pobj.communicate()
    sout=out[0]
    serr=out[1]
    estatus=pobj.poll()
    if sout == '' or estatus != 0:
        raise RuntimeError("Could not get version from "
                           "command: %s: %s" % (version_command,serr))
    return sout.strip()

def get_tmv_version():
    return get_external_version('tmv-version')

def get_python_version(numerical=False):
    if numerical:
        v=sys.version_info[0:3]
        pyvers=v[0] + 0.1*v[1] + 0.01*v[2]
    else:
        pyvers='v%s.%s.%s' % sys.version_info[0:3]
    return pyvers

from . import files
from . import generic
from . import modules

