deswl
=====

Generic framework for DES weak lensing processing

In the process of moving codes from the old svn package.  Also moving to new
more generic framework

python modules and sub-packages
-------------------------------

    generic
        Generic pluggable code runner
    modules
        More specific pipeline runners
    wlpipe
        The old framework
    files
        work with inputs and outputs to wl codes
    meds
        Pure python library to work with a Multi Epoch Data Structure
        https://cdcvs.fnal.gov/redmine/projects/deswlwg/wiki/Multi_Epoch_Data_Structure

    To install
        python setup.py install --prefix=/some/path

MEDS C library
--------------

This is a pure C library for working with MEDS

To install

    cd lib
    python build.py --prefix=/some/path install
    
Make sure that path is in your LD_LIBRARY_PATH or
it is in the "usual" place.

You can also run a test from the /lib directory against your
favorite cutouts file.
    ./test cutouts_file.fits


To link your code against the library
    CC  ... -lmeds
