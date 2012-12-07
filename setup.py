import os
import glob
from distutils.core import setup

scripts= ['deswl-gen-runconfig',
          'deswl-gen-pbs',
          'deswl-run',
          'deswl-check']

scripts=[os.path.join('bin',s) for s in scripts]

runconfig_files=glob.glob('runconfig/*.json')
config_files=glob.glob('config/*/*')
all_files=runconfig_files+config_files

data_files=[]
for f in all_files:
    d=os.path.dirname(f)
    n=os.path.basename(f)
    d=os.path.join('share',d)

    data_files.append( (d,[f]) )

setup(name="deswl", 
      version="0.1.0",
      description="DES weak lensing framework",
      license = "GPL",
      author="Erin Scott Sheldon",
      author_email="erin.sheldon@gmail.com",
      packages=['deswl'], 
      data_files=data_files,
      scripts=scripts)
