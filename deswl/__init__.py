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


try:
    import cwl
except:
    #stderr.write('Could not import cwl\n')
    pass

from . import wlpipe
from . import files
from . import generic

from . import modules

from . import meds
