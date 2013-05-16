import os
import glob
from distutils.core import setup

scripts= ['deswl-gen-runconfig',
          'deswl-gen-pbs',
          'deswl-gen-meds-script',
          'deswl-gen-meds-pbs',
          'deswl-gen-meds-srclist',
          'deswl-gen-meds-all',
          'deswl-check-meds',
          'deswl-run',
          'deswl-check']

scripts=[os.path.join('bin',s) for s in scripts]

runconfig_files=glob.glob('runconfig/*.json')
config_files=glob.glob('config/*/*.config')
config_files += glob.glob('config/*/*.yaml')
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
      packages=['deswl','deswl/modules'], 
      data_files=data_files,
      scripts=scripts)

