'''
Created on 10 Oct 2018

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.postgresdb import PGsession

from geoimagine.postgresdb.compositions import InsertCompDef, InsertCompProd, InsertLayer, SelectComp

from .hdfstuff import ManageHdf

class ManageSMAP(PGsession, ManageHdf):
    '''
    DB support for setting up processes
    '''

    def __init__(self, db):
        """ The constructor connects to the database"""
        
        # Initiate the ManageHdfcommon functions
        ManageHdf.__init__(self)
        
        # Initiate the Postgres session
        self.session = PGsession.__init__(self,'ManageMODIS')
                
        # Set the HOST name for this process
        HOST = 'karttur'
        
        # Get the credentioals for the HOST
        query = self._GetCredentials( HOST )
        
        query['db'] = db

        #Connect to the Postgres Server
        self._Connect(query)

    def _InsertDaacData(self,queryD):
        ''' Insert SMAP online hdf file
        '''
        self._CheckInsertSingleRecord(queryD,'smap', 'daacdata', [('filename',)])

    def _UpdateDaacStatus(self, queryD):
        '''
        '''
        
        queryD['schema'] = 'smap'
        
        self._UpdateHdfStatus(queryD)

    def _SelectHdfTemplate(self,query,paramL):
        '''
        '''
        
        return self._SelectTemplate(query,paramL,'smap')
      
    def _SelectSingleSMAPDaacTile(self, queryD, paramL):
        return self._SingleSearch(queryD, paramL, 'smap','daacdata')

    def _SelectTemplateLayersOnGrid(self,query,paramL):
        return self._SingleSearch(query, paramL, 'smap', 'template')

    def _SelectSMAPTemplate(self,queryD,paramL):
        return self._MultiSearch(queryD,paramL,'smap','template')

    def _InsertLayer(self,layer,overwrite,delete):
        InsertLayer(self,layer,overwrite,delete)

    def _SelectComp(self, compQ):
        return SelectComp(self, compQ)
