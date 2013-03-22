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
    python build.py install

    # to install to a different prefix
    python build.py --prefix=/some/path install
    
    # Make sure that path is in your LD_LIBRARY_PATH

You can also run a test from the /src directory against your
favorite cutouts file.

    ./test cutouts_file.fits


To link your code against the library

    CC  ... -lcfitsio -lmeds ...

Making MEDS input catalogs
--------------------------

Running build.py as described above also creates a simple
code to make meds input catalogs from a fits. file.

    cd src
    python build install
    # or under a prefix
    python build.py --prefix=/some/path install

This installs the executable
    make-meds-input
under prefix/bin

See the docs:
    https://github.com/esheldon/deswl/blob/master/src/make-meds-input.c
