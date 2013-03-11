"""
Build the libary and optionally the test programs
"""
from fabricate import *
import sys, os
import optparse

import glob

parser = optparse.OptionParser()
# make an options list, also send to fabricate
optlist=[optparse.Option('--prefix',default=sys.exec_prefix,help="prefix for library install"),
         optparse.Option('--test',action='store_true',help="build the test code")]
         
parser.add_options(optlist)

options,args = parser.parse_args()
prefix=os.path.expanduser( options.prefix )

CC='gcc'

LINKFLAGS=['-lcfitsio','-lm']

CFLAGS=['-std=gnu99','-Wall','-Werror','-O2']

sources=['meds','test']
test_programs=[{'name':'test','sources':sources}]
#install_targets = [(prog['name'],'bin') for prog in programs]

def build():
    compile()
    link()

def compile():
    for prog in test_programs:
        for source in prog['sources']:
            run(CC, '-c', '-o',source+'.o', CFLAGS, source+'.c')

def link():
    for prog in test_programs:
        objects = [s+'.o' for s in prog['sources']]
        run(CC,'-o',prog['name'],objects,LINKFLAGS)

def install():
    import shutil

    # make sure everything is built first
    build()

    for target in install_targets:
        (name,subdir) = target
        subdir = os.path.join(prefix, subdir)
        if not os.path.exists(subdir):
            os.makedirs(subdir)

        dest=os.path.join(subdir, os.path.basename(name))
        sys.stdout.write("install: %s\n" % dest)
        shutil.copy(name, dest)


def clean():
    autoclean()

# send options so it won't crash on us
main(extra_options=optlist)

