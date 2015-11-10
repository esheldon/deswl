deswl
=====

This code is deprecated

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
        Code docs here:
        https://github.com/esheldon/deswl/blob/master/deswl/meds.py

        Description of the file format here
        https://cdcvs.fnal.gov/redmine/projects/deswlwg/wiki/Multi_Epoch_Data_Structure

    To install
        # in the usual place
        python setup.py install

        # at a different prefix
        python setup.py install --prefix=/some/path
        
        # make sure it is on your PYTHONPATH


MEDS C library
--------------

This is a pure C library for working with MEDS.  Docs here
    https://github.com/esheldon/deswl/blob/master/src/meds.h

To install

    cd src
    # to install in the "usual" place.
    make install


    # to install to a different prefix
    make install prefix=/some/path install
    
    # Make sure that path is in your LIBRARY_PATH

You can also run a test from the /src directory against your
favorite cutouts file.

    ./test cutouts_file.fits


To link your code against the library, make sure to get the order correct

    CC  ... -lmeds -lcfitsio -lm ...

Making MEDS input catalogs
--------------------------

Followign the instructions above also results in a program
called make-meds-input being installed.  This is a function
to convert a fits file to an input file for the make-cutouts
program

See the docs:
    https://github.com/esheldon/deswl/blob/master/src/make-meds-input.c
