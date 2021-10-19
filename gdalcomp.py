'''
Created on 10 Mar 2021

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.postgresdb import PGsession

from geoimagine.postgresdb.compositions import InsertLayer, DeleteLayer, SelectComp, DeleteComposition

class ManageGDALcomps(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self, db):
        """ The constructor connects to the database"""
        
        # Initiate the Postgres session
        self.session = PGsession.__init__(self,'ManageGDALcomps')
                
        # Set the HOST name for this process
        HOST = 'karttur'
        
        # Get the credentioals for the HOST
        query = self._GetCredentials( HOST )
        
        query['db'] = db

        #Connect to the Postgres Server
        self._Connect(query)
        
    def _SelectSingleTileCoords(self,queryD, paramL, schema):
        '''
        '''

        return self._SingleSearch(queryD, paramL, schema,'tilecoords')