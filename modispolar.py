'''
Created on 10 Oct 2018

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.postgresdb import PGsession

from geoimagine.postgresdb.compositions import InsertCompDef, InsertCompProd, InsertLayer, SelectComp

from .hdfstuff import ManageHdf

class ManageModisPolar(PGsession, ManageHdf):
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

    def _InsertUrlFileOld(self,queryD):
        '''
        '''
        #self._CheckInsertSingleRecord(queryD,'modispolar', 'nsidcdata', [('daacid',),('version',),('filename',)])
        self._CheckInsertSingleRecord(queryD,'modispolar', 'nsidcdata', [('filename',)])

    def _InsertDaacData(self,queryD):
        '''
        '''
        #self._CheckInsertSingleRecord(queryD,'modispolar', 'daacdata', [('daacid',),('version',),('filename',)])
        self._CheckInsertSingleRecord(queryD,'modispolar', 'daacdata', [('filename',)])

    
    def _SelectDaacDataOLD(self, period, params, statusD, schema, table):
        
        queryD = {}

        queryD['daacid'] = {'val':params.product, 'op':'=' }
        
        queryD['version'] = {'val':params.version, 'op':'=' }

        for status in statusD:
            
            queryD[status] = {'val':statusD[status], 'op':'=' }
            
        queryD['acqdate'] = {'val':period.startdate, 'op':'>=' }
        
        queryD['#acqdate'] = {'val':period.enddate, 'op':'<=' }
        
        if period.enddoy > 0 and period.enddoy > period.startdoy:
            
            queryD['doy'] = {'val':period.startdoy, 'op':'>=' }
            
            queryD['#doy'] = {'val':period.enddoy, 'op':'<=' }

        wherestr = self._DictToSelect(queryD)
        
        queryD = {'where':wherestr, 'schema':schema, 'table':table}

        sql = "SELECT filename, daacid, version, acqdate FROM %(schema)s.%(table)s \
                %(where)s;" %queryD

        if self.verbose > 1:
            
            print (sql)
            
        self.cursor.execute(sql)
        
        return self.cursor.fetchall()

    def _SelectModisEase2n(self,period,params,statusD):
        '''
        '''
        queryD = {}

        queryD['product'] = {'val':params.product, 'op':'=' }
        
        queryD['version'] = {'val':params.version, 'op':'=' }

        for status in statusD:
            
            queryD[status] = {'val':statusD[status], 'op':'=' }
            
        queryD['acqdate'] = {'val':period.startdate, 'op':'>=' }
        
        queryD['#acqdate'] = {'val':period.enddate, 'op':'<=' }
        
        if period.enddoy > 0 and period.enddoy > period.startdoy:
            
            queryD['doy'] = {'val':period.startdoy, 'op':'>=' }
            
            queryD['#doy'] = {'val':period.enddoy, 'op':'<=' }

        wherestr = self._DictToSelect(queryD)

        query = "SELECT datafilename, dataid, source, product, version, content, acqdate FROM ease2n.nsidcdata \
                %s;" %(wherestr)
        
        if self.verbose > 1:
            
            print (query)
            
        self.cursor.execute(query)
        
        return self.cursor.fetchall()

    def _UpdateDaacStatus(self, queryD):
        query = "UPDATE modispolar.daacdata SET %(column)s = '%(status)s' WHERE filename = '%(filename)s'" %queryD
        self.cursor.execute(query)
        self.conn.commit()
        
    def _SelectHdfTemplate(self,query,paramL):
        '''
        '''
        
        return self._SelectTemplate(self,query,paramL,'modispolar')

    def _SelectTemplateLayersOnProdVerOLD(self,query,paramL):
        '''
        '''
        recs = self._MultiSearch(query, paramL, 'modispolar', 'template')
        
        recsDL = []
        
        for rec in recs:
        
            recsDL. append( dict( zip( paramL, rec ) ) )
            
        return recsDL
    
        #return dict( zip( self._MultiSearch(query, paramL, 'ease2n', 'template') ) )

    def _SelectSingleSModisLayer(self, queryD, paramL):
        return self._SingleSearch(queryD, paramL, 'ease2n','daacdata')

    def _SelectTemplateLayersOnGrid(self,query,paramL):
        return self._SingleSearch(query, paramL, 'ease2n', 'template')

    def _SelectSMAPTemplate(self,queryD,paramL):
        return self._MultiSearch(queryD,paramL,'ease2n','template')

    def _InsertLayer(self,layer,overwrite,delete):
        InsertLayer(self,layer,overwrite,delete)

    def _SelectComp(self, compQ):
        return SelectComp(self, compQ)
