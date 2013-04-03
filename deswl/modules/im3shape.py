from deswl import generic
import desdb

# don't put status, meta, or log here, they will get
# over-written
ME_FILETYPES={'raw':{'ext':'txt'},
              'clean':{'ext':'txt'}}

# need to measure this
ME_TIMEOUT=60*60 # minutes


class I3MEScripts(generic.GenericScripts):
    """
    to create and write the "config" files, which hold the command
    to run, input/output file lists, and other metadata.
    """
    def __init__(self, run, **keys):
        super(ShapeletsSEScripts,self).__init__(run)

    def write(self):
        """
        Write all config files for expname/ccd
        """
        super(ShapeletsSEScripts,self).write_by_tile()

    def get_output_filenames(self, **keys):
        tilename=keys['tilename']
        band=keys['band']
        start=keys.get('start',None)
        if start is not None:
            end=keys.get('end',None)
            if end is None:
                raise ValueError("send both start= and end=")
            type='wlpipe_me_split'
        else:
            type='wlpipe_me_generic'

        fdict={}
        for ftype in ME_FILETYPES:
            ext=ME_FILETYPES[ftype]['ext']
            fdict[ftype] = self._df.url(type=type,
                                        run=self['run'],
                                        tilename=tilename,
                                        band=band,
                                        filetype=ftype,
                                        ext=ext,
                                        start=start,
                                        end=end)
        return fdict


