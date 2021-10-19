'''
Created on 8 mars 2018

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.postgresdb import PGsession

from geoimagine.postgresdb.compositions import InsertCompDef, InsertCompProd, InsertLayer, SelectComp, SelectCompleteComp
 
class ManageAncillary(PGsession):
    '''
    DB support for managing regions
    '''
    
    def __init__(self, db):
        """ The constructor connects to the database"""
        
        # Initiate the Postgres session
        self.session = PGsession.__init__(self,'ManageAncillary')
                
        # Set the HOST name for this process
        HOST = 'karttur'
        
        # Get the credentioals for the HOST
        query = self._GetCredentials( HOST )
        
        query['db'] = db

        #Connect to the Postgres Server
        self._Connect(query)
                
    def _AncillaryMultiSearch(self, queryD, paramL, schema, table):
        '''
        '''
        return self._MultiSearch(queryD, paramL, schema, table)

    def _SelectDefaultRegion(self,defregid):
        '''
        '''
        query = {'regionid':defregid}
        self.cursor.execute("SELECT regioncat FROM system.defregions WHERE regionid = '%(regionid)s';" %query)
        rec = self.cursor.fetchone()
        return rec

    def _ManageAncilDS(self, queryD, overwrite, delete):
        '''
        '''        

        sql = "SELECT * FROM system.regions WHERE regionid = '%(regionid)s';" %queryD
        
        self.cursor.execute(sql)
        
        rec = self.cursor.fetchone()
        
        if rec == None:
            
            print (sql)
            
            exit()
                 
        sql = "SELECT * FROM %(system)s.datasets WHERE dsid = '%(dsid)s'" %queryD
        
        self.cursor.execute(sql)
        
        rec = self.cursor.fetchone()
        
        if rec == None and not delete:

            sql = "INSERT INTO %(system)s.datasets (orgid, dsname, regionid, title, label, dataurl, metaurl, dsid, accessdate, dsversion, copyright) VALUES \
                ('%(orgid)s', '%(dsname)s', '%(regionid)s', '%(title)s', '%(label)s', '%(dataurl)s', '%(metaurl)s','%(dsid)s', '%(accessdate)s', '%(dsversion)s', '%(copyright)s')" %queryD
  
            self.cursor.execute( sql )
            
            self.conn.commit()
        
        elif rec != None and overwrite:
            
            sql = "DELETE FROM %(system)s.datasets  WHERE dsid = '%(dsid)s';" %queryD
            
            self.cursor.execute( sql )
            
            self.conn.commit()
            
            sql = "INSERT INTO %(system)s.datasets (orgid, dsname, regionid, title, label, dataurl, metaurl, dsid, accessdate, dsversion, copyright) VALUES \
                ('%(orgid)s', '%(dsname)s', '%(regionid)s', '%(title)s', '%(label)s', '%(dataurl)s', '%(metaurl)s','%(dsid)s', '%(accessdate)s', '%(dsversion)s', '%(copyright)s')" %queryD
            
            self.cursor.execute( sql )
            
            self.conn.commit()
        
        elif delete:
            
            sql = "DELETE FROM %(system)s.datasets  WHERE dsid = '%(dsid)s';" %queryD
            
            self.cursor.execute( sql )
            
            self.conn.commit()

    def _InsertCompDef(self, comp, title, label):
        ''' Link to the generic function InsertCompDef for all systems
        '''
        
        InsertCompDef(self, comp, title, label)

    def _InsertCompProd(self, comp):
        ''' Link to the generic function InsertCompProd for all systems
        '''
        
        InsertCompProd(self, comp)

    def _InsertLayer(self,layer,overwrite,delete):
        ''' Link to the generic function InsertLayer for all systems
        '''
        
        InsertLayer(self,layer,overwrite,delete)
        
        
    def _RetrieveLayerComp(self, queryD, searchItemL):
        '''
        '''
        
        SelectCompleteComp(self, 'ancillary', queryD, searchItemL)
        

    def _LinkDsCompid(self,dsid,compid,overwrite,delete):
        '''
        '''

        query ={'dsid':dsid,'compid':compid}
        
        sql = "SELECT * FROM ancillary.dscompid WHERE dsid = '%(dsid)s' AND compid = '%(compid)s';" %query
        
        self.cursor.execute( sql )
        
        recs = self.cursor.fetchone()
        
        if recs == None and not delete:
            
            sql = "INSERT INTO ancillary.dscompid (dsid, compid) VALUES ('%(dsid)s', '%(compid)s' )" %query
            
            self.cursor.execute( sql )
            
            self.conn.commit()

    def _SelectComp(self, compQ):
        '''
        '''
        return SelectComp(self, compQ)

    def _InsertClimateIndex(self,queryL):
        ''' Insert climate index time series
        '''
        
        self.cursor.execute("DELETE FROM climateindex.climindex WHERE index = '%(index)s';" %queryL[0])
        
        for query in queryL:
            
            self.cursor.execute("INSERT INTO climateindex.climindex (index, acqdate, acqdatestr, value) VALUES ('%(index)s', '%(acqdate)s', '%(acqdatestr)s', %(value)s);" %query)
            
            self.conn.commit()

    def _SelectRegionExtent(self, queryD, paramL):
        '''
        '''
        
        return self._SingleSearch(queryD, paramL, 'system', 'regions')

    def _SelectClimateIndex(self,period,index):
        '''
        '''
        
        query = {'index':index, 'sdate': period.startdate, 'edate':period.enddate}
        
        self.cursor.execute("SELECT acqdate, value FROM climateindex.climindex WHERE \
            index = '%(index)s' AND acqdate >= '%(sdate)s' AND acqdate <= '%(edate)s' ORDER BY acqdate" %query )
        
        recs = self.cursor.fetchall()
        
        if len (recs) == 0:
            print ("SELECT acqdate, value FROM climateindex.climindex WHERE \
            index = '%(index)s' AND acqdate >= '%(sdate)s' AND acqdate <= '%(edate)s' ORDER BY acqdate" %query )
        
        return recs

    def _DeleteSRTMBulkTilesOld(self,params):
        '''
        '''
        
        query = {'prod':params.product, 'v':params.version}
        
        self.cursor.execute("DELETE FROM ancillary.srtmdptiles WHERE product= '%(prod)s' AND version = '%(v)s';" %query)
        
        self.conn.commit()

    def _LoadSRTMBulkTilesOld(self,params,tmpFPN,headL):
        self._DeleteSRTMBulkTiles(params)
        #query = {'tmpFPN':tmpFPN, 'items': ",".join(headL)}
        #print ("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #self.cursor.execute("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #print ("%(tmpFPN)s, 'modis.datapooltiles', columns= (%(items)s), sep=','; "%query)
        print ('Copying from',tmpFPN)
        with open(tmpFPN, 'r') as f:
            next(f)  # Skip the header row.
            self.cursor.copy_from(f, 'ancillary.srtmdptiles', sep=',')
            self.conn.commit()
            #cur.copy_from(f, 'test', columns=('col1', 'col2'), sep=",")

    def _SelectSRTMdatapooltilesOntileOld(self,queryD,paramL):
        '''
        '''
        
        rec = self._SingleSearch(queryD, paramL, 'ancillary', 'srtmdptiles', True)
        return rec

    def _SelectDefRegionExtentOld(self,queryD, paramL):
        '''
        '''
        
        rec = self._SingleSearch(queryD, paramL, 'system', 'regions', True)
        return rec

    def _Select1degSquareTilesOld(self, queryD):
        """
        """

        self.cursor.execute ("SELECT L.lltile FROM system.regions as R \
        JOIN system.defregions as D USING (regionid)\
        JOIN ancillary.srtmdptiles as L ON (R.regionid = L.lltile) \
        WHERE title = '1degsquare' AND lrlon > %(ullon)s AND ullon < %(lrlon)s AND ullat > %(lrlat)s AND lrlat < %(ullat)s;" %queryD)

        recs = self.cursor.fetchall()
        
        return recs
