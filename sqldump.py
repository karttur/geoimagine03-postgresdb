'''
Created on 1 apr. 2019

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.postgresdb import PGsession

class ManageSqlDumps(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self):
        """ The constructor connects to the database"""
        
        HOST = 'karttur'
        
        query = self._GetCredentials( HOST )

        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'ManageSMAP')

    def _SelectAllTableRecs(self,query):
        '''
        '''
        #print ("SELECT %(items)s FROM %(schematab)s;" %query)
        #recs = self.session._SelectAllTableRecs(query)
        self.cursor.execute("SELECT %(items)s FROM %(schematab)s;" %query)
        return self.cursor.fetchall()
