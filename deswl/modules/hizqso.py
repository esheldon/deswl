import os
import numpy
import fitsio
import desdb
import deswl

class HiZQSO(object):
    def __init__(self, hizqso_fname, medsconf, size_band='i'):
        """
        medsconf determines where the scripts and fake catalogs and outputs go

        You need to generate the medsconf as usual, with a release etc.

        todo
            make deswl-gen-meds-script take the coadd catalog on the command
            line
        """
        self.hizqso_fname=hizqso_fname
        self.medsconf=medsconf
        self.size_band=size_band

        self.conf=deswl.files.read_meds_config(medsconf)

        self.data = fitsio.read(hizqso_fname, lower=True)

        self.df = desdb.files.DESFiles()

        self.set_dir()
        self.set_runs()

    def go(self):
        """
        make fake catalog files for make-meds-input, and call
        the code to make the meds scripts etc.

        """

        tiles=numpy.unique( self.data['tilename'] )
        tiles.sort()

        ntile=len(tiles)
        for i,tile in enumerate(tiles):
            print '-'*70
            print '%s/%s' % (i+1, ntile)
            run = self.get_run(tile)
            out=self.make_fake_tile_cat(tile)

            # currently the same for all bands
            for band in ['g','r','i','z','Y']:
                fname=self.get_fake_tile_fname(tile, band)
                print 
                print fname
                fitsio.write(fname, out, clobber=True)

                self.make_scripts(run, band, fname)

    def make_scripts(self, run, band, fake_cat):
        cmds=['deswl-gen-meds-srclist %(medsconf)s %(run)s %(band)s',
              'deswl-gen-meds-script --catalog %(cat)s %(medsconf)s %(run)s %(band)s',
              'deswl-gen-meds-wq %(medsconf)s %(run)s %(band)s']
        
        fdict={'run':run,
               'band':band,
               'medsconf':self.medsconf,
               'cat':fake_cat}
        cmds = [cmd % fdict for cmd in cmds]

        for cmd in cmds:
            ret=os.system(cmd)
            if ret != 0:
                err="""
                error running command:
                    %s
                exit status
                    %s
                """ % (cmd, ret)
                raise RuntimeError(err)

    def make_fake_tile_cat(self, tile):
        sb = self.size_band

        w,=numpy.where(self.data['tilename'] == tile)

        out = self.get_struct(w.size)

        out['ALPHAMODEL_J2000'] = self.data['ra'][w]
        out['DELTAMODEL_J2000'] = self.data['dec'][w]

        out['X_IMAGE'] = self.data['xwin_image_%s' % sb][w]
        out['Y_IMAGE'] = self.data['ywin_image_%s' % sb][w]

        # making these boxes small. Won't matter since we
        # will set min_boxsize when creating the MEDS files
        # use the size from the flux radius
        out['XMIN_IMAGE'] = 1.0
        out['XMAX_IMAGE'] = 2.0
        out['YMIN_IMAGE'] = 1.0
        out['YMAX_IMAGE'] = 2.0

        mrad = self.data['flux_radius_%s' % sb][w].max()
        frad = self.data['flux_radius_%s' % sb][w].clip(2.0, mrad)

        out['FLUX_RADIUS'] = frad

        # round
        out['A_WORLD'] = frad
        out['B_WORLD'] = frad

        return out


    def get_cat_dtype(self):
        return [('ALPHAMODEL_J2000','f8'),
                ('DELTAMODEL_J2000','f8'),
                ('X_IMAGE','f8'),
                ('Y_IMAGE','f8'),
                ('XMIN_IMAGE','f8'),
                ('XMAX_IMAGE','f8'),
                ('YMIN_IMAGE','f8'),
                ('YMAX_IMAGE','f8'),
                ('FLUX_RADIUS','f8'),
                ('A_WORLD','f8'),
                ('B_WORLD','f8')]
    def get_struct(self, n):
        dt=self.get_cat_dtype()
        return numpy.zeros(n, dtype=dt)

    def set_dir(self):
        d=self.df.dir('meds_run',medsconf=self.medsconf)

        d=os.path.join(d, 'fake-catalogs')

        if not os.path.exists(d):
            print 'making dir:',d
            os.makedirs(d)

        self.dir = d

    def get_fake_tile_fname(self, tile, band):
        fname='%s-%s-%s.fits' % (self.medsconf,tile, band)
        path=os.path.join(self.dir, fname)
        return path

    def get_run(self, tilename):
        for run in self.allruns:
            if run.find(tilename) != -1:
                return run

        raise ValueError("filename not found: '%s', should not happen!" % tilename)

    def set_runs(self):
        print 'setting runs'
        self.allruns = desdb.files.get_release_runs(self.conf['release'])

