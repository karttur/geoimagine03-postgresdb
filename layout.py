'''
Created on 22 feb. 2018

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.postgresdb import PGsession

class ManageLayout(PGsession):
    '''
    DB connections for layout
    '''

    def __init__(self, db):
        """ The constructor connects to the database"""
        
        # Initiate the Postgres session
        self.session = PGsession.__init__(self,'ManageLayout')
                
        # Set the HOST name for this process
        HOST = 'karttur'
        
        # Get the credentioals for the HOST
        query = self._GetCredentials( HOST )
        
        query['db'] = db

        #Connect to the Postgres Server
        self._Connect(query)

    def _ManageRasterPalette(self, queryD, colorD, overwrite, delete):
        '''
        '''
        

        self._CheckInsertSingleRecord(queryD,'layout','rasterpalette', [['palette']])
        
        #If overwrite or delete all colors all deleted
        if overwrite or delete:
            
            sql = "DELETE FROM layout.rasterpalcolors WHERE palette = '%(palette)s';" %queryD
            
            self.cursor.execute(sql)
            
            self.conn.commit()
            
        else:
            
            sql = "SELECT * FROM layout.rasterpalcolors WHERE palette = '%(palette)s';" %queryD
            
            self.cursor.execute(sql)
        
            recs = self.cursor.fetchall()
        
            if len(recs) > 0:
                
                return

        if not delete:
            
            for col in colorD:
                
                query = colorD[col]
                
                query['palette'] = queryD['palette']
                
                query['value'] = int(col)
                                
                self._CheckInsertSingleRecord(query,'layout','rasterpalcolors', [['palette'],['value']])

    def _ManageRasterLegend(self,queryD, comp, overwrite, delete):
        '''
        '''
        
        compid = '%(f)s_%(b)s' %{'f':comp['content'].lower(),'b':comp['layerid'].lower()}
        
        queryD['compid'] = compid
        
        queryD['source'] = comp['source']
        
        queryD['product'] = comp['product']
        
        queryD['suffix'] = comp['suffix']

        booleans = ['label','compresslabels','matrix','two51','two52','two53','two54','two55']
        
        for b in booleans:
        
            if queryD[b]:
            
                queryD[b] = 'Y'
            
            else:
            
                queryD[b] = 'N'

        if overwrite or delete:
            
            sql = "DELETE FROM layout.legend WHERE \
                compid = '%(compid)s' AND source = '%(source)s' AND product = '%(product)s' AND suffix = '%(suffix)s';" %queryD
            
            self.cursor.execute(sql)
            
            self.conn.commit()
        
        if not delete:
        
            self._CheckInsertSingleRecord(queryD,'layout','legend', [['compid'],['source'],['product'],['suffix']])

    def _ManageRasterScaling(self,paramsD,comp,overwrite,delete):
        '''
        '''
        
        compid = '%(f)s_%(b)s' %{'f':comp['content'].lower(),'b':comp['layerid'].lower()}
        
        paramsD['compid'] = compid
        
        paramsD['source'] = comp['source']
        
        paramsD['product'] = comp['product']
        
        paramsD['suffix'] = comp['suffix']

        #If overwrite or delete all colors all deleted
        if overwrite or delete:
            
            self.cursor.execute("DELETE FROM layout.scaling WHERE \
            compid = '%(compid)s' AND source = '%(source)s' AND product = '%(product)s' AND suffix = '%(suffix)s';" %paramsD)
            
            self.conn.commit()
        
        if not delete:
            
            self._CheckInsertSingleRecord(paramsD,'layout','scaling', [['compid'],['source'],['product'],['suffix']])

    def _GetCompFormat(self, query):
        
        exitstr = 'postgresdb.layout.ManageLayout._GetCompFormat not yet implemented'
        
        exit (exitstr)


    def _SelectPaletteColors(self,query,paramL):
        '''
        '''
        
        query['cols'] = ",".join(paramL)
        
        sql = "SELECT  %(cols)s FROM layout.rasterpalcolors \
            WHERE palette = '%(palette)s';" %query
        
        self.cursor.execute(sql)
        
        recs = self.cursor.fetchall()
        
        if len(recs) == 0:
                    
            print (sql)
                
        return (recs)

    def _SelectCompDefaultPalette(self,query):
        '''
        '''
        
        self.cursor.execute("SELECT palette FROM layout.defaultpalette \
            WHERE compid = '%(compid)s';" %query)
        
        recs = self.cursor.fetchone()
        
        return (recs)

    def _ManageMovieClock(self,queryD,overwrite,delete):
        '''
        '''
        
        if overwrite or delete:
        
            self.cursor.execute("DELETE FROM layout.movieclock WHERE \
                name = '%(name)s';" %queryD)
            
            self.conn.commit()
        
        if not delete:
        
            self._CheckInsertSingleRecord(queryD,'layout','movieclock', [['name']])
