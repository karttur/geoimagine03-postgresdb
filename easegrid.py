'''
Created on 1 Mar 2021

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.postgresdb import PGsession

from geoimagine.postgresdb.compositions import InsertLayer, DeleteLayer, SelectComp, DeleteComposition

from geoimagine.postgresdb.region import CommonRegions

class ManageEASEgrid(PGsession, CommonRegions):
    '''
    DB support for setting up processes
    '''
    
    def __init__(self, db):
        """ The constructor connects to the database"""
        
        # Initiate the Postgres session
        self.session = PGsession.__init__(self,'ManageMODIS')
        
        # Initate CommonRegions
        
        CommonRegions.__init__(self)
                
        # Set the HOST name for this process
        HOST = 'karttur'
        
        # Get the credentioals for the HOST
        query = self._GetCredentials( HOST )
        
        query['db'] = db

        #Connect to the Postgres Server
        self._Connect(query)

    def _InsertTileCoord(self,system,xytile,xtile,ytile,ulxease,ulyease,lrxease,lryease,west,south,east,north,ullat,ullon,lrlat,lrlon,urlat,urlon,lllat,lllon):
        '''
        '''
        
        query = {'system':system,'xytile':xytile}
        
        sql = "SELECT * FROM %(system)s.tilecoords WHERE xytile = '%(xytile)s';" %query       
        
        self.cursor.execute( sql )
        
        record = self.cursor.fetchone()
        
        if record == None:
            
            sql = "INSERT INTO %s.tilecoords (xytile,xtile,ytile,minxease,maxyease,maxxease,minyease,west,south,east,north,ullat,ullon,lrlat,lrlon,urlat,urlon,lllat,lllon) \
            VALUES ('%s',%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s);" %(system,
                xytile,xtile,ytile,ulxease,ulyease,lrxease,lryease,west,south,east,north,ullat,ullon,lrlat,lrlon,urlat,urlon,lllat,lllon)
            
            print (sql)
            
            self.cursor.execute( sql )
            
            self.conn.commit()

    def _SelectTileCoords(self):
        '''
        '''
        #construct where statement - LATER
        query = {}
        
        self.cursor.execute("SELECT xytile,xtile,ytile,minxease,maxyease,maxxease,minyease,ullat,ullon,lrlat,lrlon,urlat,urlon,lllat,lllon FROM easegrid.tilecoords;" %query)
        
        records = self.cursor.fetchall()
        
        return records

    def _SelectSingleTileCoords(self, queryD, paramL):
        '''
        '''
        NOTCORRECT
        return self._SingleSearch(queryD, paramL, 'easegrid','tilecoords')

    def _InsertRegionTileMovedToRegion(self, query):
        '''
        '''
        
        if query['table'] == 'regions':
            
            sql = "SELECT * FROM %(system)s.%(table)s WHERE regionid = '%(regionid)s';"  %query
            
            self.cursor.execute( sql )
            
        elif query['table'] == 'tracts':
            
            sql = "SELECT * FROM %(system)s.%(table)s WHERE tractid = '%(regionid)s';"  %query
            
            self.cursor.execute( sql)

        record = self.cursor.fetchone()
        
        if record == None:
            
            print ( sql )
            
            warnstr = 'WARNING can not add tile to region %(easegrid)s, no such region in system  %(system)s and tabel %(table)s' %query
            
            print (warnstr)
            
            BALLE
            
            return
        
        sql = "SELECT * FROM %(easegrid)s.regions WHERE xtile = %(x)s AND ytile = %(y)s AND regiontype = '%(regiontype)s' AND regionid = '%(regionid)s';" %query
        
        self.cursor.execute( sql )
        
        record = self.cursor.fetchone()

        if record == None and not query['delete']:
            
            sql = "INSERT INTO %(easegrid)s.regions (regionid, regiontype, xtile, ytile)\
            VALUES ('%(regionid)s', '%(regiontype)s', %(x)d, %(y)d)" %query 
            
            self.cursor.execute( sql )       
                    
            #(query['easegrid'],query['regionid'], query['regiontype'],query['x'], query['y']))
            
            self.conn.commit()
        
        elif record and query['delete']:
        
            self.cursor.execute("DELETE FROM easegrid.regions WHERE easegrid = %(easegrid)s AND xtile = '%(x)s' AND ytile = '%(y)s' AND regiontype = '%(regiontype)s'AND regionid = '%(regionid)s';" %query)
            
            self.conn.commit()
