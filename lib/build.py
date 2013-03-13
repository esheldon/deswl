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
AR='ar'

LINKFLAGS=['-L.','-lcfitsio','-lm','-lmeds']

CFLAGS=['-std=gnu99','-Wall','-Werror','-O2']

libname='meds'
aname='lib%s.a' % libname

lib_sources=['meds']
test_sources=['test']
test_speed_sources=['test-speed']
all_sources=lib_sources + test_sources + test_speed_sources
test_programs=[{'name':'test','sources':lib_sources+test_sources},
               {'name':'test-speed','sources':lib_sources+test_speed_sources}]

libraries=[aname]
install_targets = [(lib,'lib') for lib in libraries]

def build():
    compile()
    link_library()
    link_programs()

def compile():
    for source in all_sources:
        run(CC, '-c', '-o',source+'.o', CFLAGS, source+'.c')

def link_library():
    objects = [s+'.o' for s in lib_sources]
    run(AR,'rcs',aname,objects)
    

def link_programs():
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

