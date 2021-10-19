'''
Created on 24 Feb 2021

@author: thomasgumbricht

Support functions for hdf related db searches
'''

class ManageHdf:
    '''
    '''

    def __init__(self):
        """ Access the defined functions
        """
        
        pass

    def _SelectDaacData(self,period,params,statusD,schema):
        '''
        '''
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
        
        queryD = {'schema':schema, 'where':wherestr }

        sql = "SELECT filename, daacid, version, acqdate FROM %(schema)s.daacdata %(where)s;" %queryD
        
        if self.verbose > 1:
            
            print ('            ',sql)
            
        self.cursor.execute(sql)
        
        return self.cursor.fetchall()
    

    def _UpdateHdfStatus(self, queryD, ):
        '''
        '''
        query = "UPDATE %(schema)s.daacdata SET %(column)s = '%(status)s' WHERE filename = '%(filename)s'" %queryD
        
        self.cursor.execute(query)
        
        self.conn.commit()

    def _SelectTemplate(self, query, paramL, schema):
        '''
        '''
        
        recs = self._MultiSearch(query, paramL, schema, 'template')
        
        recsDL = []
        
        for rec in recs:
        
            recsDL.append( dict( zip( paramL, rec ) ) )
            
        return recsDL
        