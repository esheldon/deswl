import os
import glob
from distutils.core import setup

scripts= ['deswl-gen-runconfig',
          'deswl-gen-pbs',
          'deswl-gen-meds-script',
          'deswl-gen-meds-idfile',
          'deswl-gen-meds-pbs',
          'deswl-gen-meds-wq',
          'deswl-gen-meds-srclist',
          'deswl-gen-meds-all',
          'deswl-gen-meds-all-release',
          'deswl-check-meds',
          'deswl-run',
          'deswl-check']

scripts=[os.path.join('bin',s) for s in scripts]

runconfig_files_json=glob.glob('runconfig/*.json')
runconfig_files_yaml=glob.glob('runconfig/*.yaml')

runconfig_files = runconfig_files_json + runconfig_files_yaml

config_files=glob.glob('config/*/*.config')
config_files += glob.glob('config/*/*.yaml')
other_files = glob.glob('data/*.txt')

all_files=runconfig_files+config_files+other_files

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
      packages=['deswl','deswl/modules','deswl/desmeds'], 
      data_files=data_files,
      scripts=scripts)

