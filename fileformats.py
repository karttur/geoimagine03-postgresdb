'''
Created on 23 feb. 2018

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.postgresdb import PGsession


class SelectFileFormats(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self):
        """ The constructor connects to the database"""
        
        HOST = 'karttur'
        
        query = self._GetCredentials( HOST )

        #Connect to the Postgres Server
        PGsession.__init__(self,query,'SelectFileFormats')


    def _SelectCellTypes(self):
        '''
        '''
        
        self.cursor.execute("SELECT gdal, arr, np, usgs FROM process.celltypes")
        
        records = self.cursor.fetchall()
        
        return records

    def _SelectGDALof(self):
        '''
        '''
        
        self.cursor.execute("SELECT hdr, dat, gdaldriver FROM process.gdalformat")
        
        records = self.cursor.fetchall()
        
        return records
