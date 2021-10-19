'''
Created on 23 feb. 2018

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.postgresdb import PGsession

from geoimagine.postgresdb.compositions import InsertLayer, DeleteLayer, SelectComp, DeleteComposition

from .region import CommonRegions

from .hdfstuff import ManageHdf

class ManageMODIS(PGsession, ManageHdf, CommonRegions):
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

    def _InsertModisTileCoord(self,hvtile,h,v,ulxsin,ulysin,lrxsin,lrysin,west,south,east,north,ullat,ullon,lrlon,lrlat,urlon,urlat,lllon,lllat):
        '''
        '''
        
        query = {'hvtile':hvtile}
        
        sql = "SELECT * FROM modis.tilecoords WHERE hvtile = '%(hvtile)s';" %query       
        
        self.cursor.execute( sql )
        
        record = self.cursor.fetchone()
        if record == None:
            self.cursor.execute("INSERT INTO modis.tilecoords (hvtile,htile,vtile,minxsin,maxysin,maxxsin,minysin,west,south,east,north,ullat,ullon,lrlon,lrlat,urlon,urlat,lllon,lllat) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) ;",
                                (hvtile,h,v,ulxsin,ulysin,lrxsin,lrysin,west,south,east,north,ullat,ullon,lrlon,lrlat,urlon,urlat,lllon,lllat))
            self.conn.commit()

    
    def _SelectSingleTileCoords(self, queryD, paramL):
        '''
        '''
        return self._SingleSearch(queryD, paramL, 'modis','tilecoords')

    def _InsertModisRegionTile(self, query):
        '''
        '''
        
        if query['table'] == 'regions':
            
            self.cursor.execute("SELECT * FROM %(system)s.%(table)s WHERE regionid = '%(regionid)s';"  %query)
            
        elif query['table'] == 'tracts':
            
            self.cursor.execute("SELECT * FROM %(system)s.%(table)s WHERE tractid = '%(regionid)s';"  %query)

        record = self.cursor.fetchone()
        
        if record == None:
            
            print ("SELECT * FROM %(system)s.regions WHERE regionid = '%(regionid)s';"  %query)
            
            warnstr = 'WARNING can not add tile to region %(regionid)s, no such region at category %(category)s and type %(type)s' %query
            
            print (warnstr)
            
            return
        
        self.cursor.execute("SELECT * FROM modis.regions WHERE htile = %(h)s AND vtile = %(v)s AND regiontype = '%(regiontype)s' AND regionid = '%(regionid)s';" %query)
        
        record = self.cursor.fetchone()

        if record == None and not query['delete']:
            
            self.cursor.execute("INSERT INTO modis.regions (regionid, regiontype, htile, vtile) VALUES (%s, %s, %s, %s)",
            
                    (query['regionid'], query['regiontype'],query['h'], query['v']))
            
            self.conn.commit()
        
        elif record and query['delete']:
        
            self.cursor.execute("DELETE FROM modis.regions WHERE htile = '%(h)s' AND vtile = '%(v)s' AND regiontype = '%(regiontype)s'AND regionid = '%(regionid)s';" %query)
            
            self.conn.commit()

    def _InsertDaacData(self,queryD):
        ''' Insert MODIS online hdf file
        '''
        self._CheckInsertSingleRecord(queryD,'modis', 'daacdata', [('filename',)])

    def _UpdateDaacStatus(self, queryD):
        '''
        '''
        
        queryD['schema'] = 'modis'
        
        self._UpdateHdfStatus(queryD)
        
    def _SelectMODISdatapooltilesOntileOLD(self, params, period, statusD, paramL):
        '''
        '''
        queryD = {}
        queryD['D.product'] = {'val':params.product, 'op':'=' }
        queryD['D.version'] = {'val':params.version, 'op':'=' }
        queryD['D.h'] = {'val':params.htile, 'op':'=' }
        queryD['D.v'] = {'val':params.vtile, 'op':'=' }
        queryD['D.acqdate'] = {'val':period.startdate, 'op':'>=' }
        queryD['#D.acqdate'] = {'val':period.enddate, 'op':'<=' }
        if period.enddoy > 0 and period.enddoy > period.startdoy:
            queryD['D.doy'] = {'val':period.startdoy, 'op':'>=' }
            queryD['#D.doy'] = {'val':period.enddoy, 'op':'<=' }

        if len(paramL) == 1:
            cols = paramL[0]
        else:
            cols =  ','.join(paramL)
        if not statusD['downloaded']:
            #If only finding tiles not previosly downloaded
            queryD['T.tileid'] = {'val':'NULL', 'op':'IS' }
            wherestr = self._DictToSelect(queryD)
            wherestr = wherestr.replace("'NULL'", "NULL")
            qD = {'cols':cols,'where':wherestr}
            query = "SELECT %(cols)s FROM modis.datapooltiles D LEFT JOIN modis.tiles T USING (tileid)\
                %(where)s;" %qD
            self.cursor.execute(query)
            recs = self.cursor.fetchall()
            queryD.pop('T.tileid')
            queryD['T.downloaded'] = {'val':'N', 'op':'=' }
            wherestr = self._DictToSelect(queryD)
            qD = {'cols':cols,'where':wherestr}
            query = "SELECT %(cols)s FROM modis.datapooltiles D LEFT JOIN modis.tiles T USING (tileid)\
                %(where)s;" %qD
            #wherestr = wherestr.replace("'NULL'", "NULL")
            #query = "SELECT * FROM modis.datapooltiles D LEFT JOIN modis.tiles T USING (tileid)\
            #    %s;" %(wherestr)
            self.cursor.execute(query)
            recs2 = self.cursor.fetchall()
            recs.extend(recs2)
        else:
            #If only finding tiles not previosly downloaded
            queryD['T.downloaded'] = {'val':'Y', 'op':'=' }
            wherestr = self._DictToSelect(queryD)

            qD = {'cols':cols,'where':wherestr}
            query = "SELECT %(cols)s FROM modis.datapooltiles D LEFT JOIN modis.tiles T USING (tileid)\
                %(where)s;" %qD
            #query = "SELECT * FROM modis.datapooltiles D LEFT JOIN modis.tiles T USING (tileid)\
            #    %s;" %(wherestr)
            self.cursor.execute(query)
            recs = self.cursor.fetchall()
        return recs

    def _SelectSingleMODISdatapoolTileOLD(self, queryD, paramL):
        '''
        '''
        return self._SingleSearch(queryD, paramL, 'modis','datapooltiles')

    def _SelectTileIdOnhvdOLD(self,queryD,paramL):
        '''
        '''
        return self._SingleSearch(queryD, paramL, 'modis','datapooltiles')

    def _SelectMODISTemplateOLD(self,queryD,paramL):
        '''
        '''
        return self._MultiSearch(queryD,paramL,'modis','template')

    def _DeleteBulkTilesOLD(self,params,acqdate):
        '''
        '''
        query = {'prod':params.product, 'v':params.version, 'acqdate':acqdate}
        self.cursor.execute("DELETE FROM modis.datapooltiles WHERE product= '%(prod)s' AND version = '%(v)s' AND acqdate = '%(acqdate)s';" %query)
        self.conn.commit()

    def _LoadBulkTilesOLD(self,params,acqdate,tmpFPN,headL):
        self._DeleteBulkTiles(params,acqdate)
        #query = {'tmpFPN':tmpFPN, 'items': ",".join(headL)}
        #print ("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #self.cursor.execute("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #print ("%(tmpFPN)s, 'modis.datapooltiles', columns= (%(items)s), sep=','; "%query)
        print ('Copying from',tmpFPN)
        with open(tmpFPN, 'r') as f:
            next(f)  # Skip the header row.
            self.cursor.copy_from(f, 'modis.datapooltiles', sep=',')
            self.conn.commit()
            #cur.copy_from(f, 'test', columns=('col1', 'col2'), sep=",")

    def _SelectCompOLD(self,comp):
        return SelectComp(self, comp)

    def _SelectLayerOLD(self,system,queryD,paramL):
        return self._SingleSearch(queryD,paramL,system,'layer')

    def _SelectLayersOLD(self,system,queryD,paramL):
        return self._MultiSearch(queryD,paramL,system,'layer')

    

    def _InsertMODIStileOLD(self,query):
        self.cursor.execute("SELECT * FROM modis.tiles WHERE tileid = '%(tileid)s';"  %query)
        record = self.cursor.fetchone()
        if record == None:
            cols = query.keys()
            values = query.values()
            values =["'{}'".format(str(x)) for x in values]
            query = {'cols':",".join(cols), 'values':",".join(values)}
            self.cursor.execute ("INSERT INTO modis.tiles (%(cols)s) VALUES (%(values)s);" %query)
            self.conn.commit()

    def _UpdateModisTileStatusOLD(self, queryD):
        query = "UPDATE modis.tiles SET %(column)s = '%(status)s' WHERE tileid = '%(tileid)s'" %queryD
        self.cursor.execute(query)
        self.conn.commit()

    def _InsertLayerOLD(self,layer,overwrite,delete):
        InsertLayer(self,layer,overwrite,delete)

    def _DeleteLayerOLD(self,layer,overwrite,delete):
        DeleteLayer(self,layer,overwrite,delete)

    def _DeleteCompositionOLD(self,comp):
        compD = {'compid':comp.compid,'source':comp.source, 'product':comp.product, 'suffix':comp.suffix}
        DeleteComposition(self,'modis',compD)

    def _SelectDefRegionExtentOLD(self, queryD, paramL):
        return self._SingleSearch(queryD, paramL, 'system', 'regions')

    def _SelectParentRegionOLD(self,queryD,paramL):
        return self._SingleSearch(queryD, paramL, 'system', 'regions')

    def _SelectMODIStilesOLD(self, queryD,paramL=False):
        if not paramL:
            paramL = ['tileid']
        return self._MultiSearch(queryD, paramL,'modis','tiles', True)


    def _GetDefRegionOLD(self,query):
        #Test if the query contains a default region
        self.cursor.execute("SELECT regionid FROM system.regions WHERE regionid = '%(regionid)s';" %query)
        rec = self.cursor.fetchone()
        if rec == None:
            self.cursor.execute("SELECT parentid FROM regions.tracts WHERE tractid = '%(regionid)s';" %query)
            record = self.cursor.fetchone()
            if record == None:
                return record

            query['regionid'] = record[0]

            self.cursor.execute("SELECT regionid FROM system.regions WHERE regionid = '%(regionid)s';" %query)
            rec = self.cursor.fetchone()

        return rec

    def _SelectRegionLonLatExtentOLD(self, regionid, regiontype):

        query = {'id':regionid}
        if regiontype == 'T':
            self.cursor.execute("SELECT ullon, lllon, ullat, urlat, urlon, lrlon, lllat, lrlat FROM regions.regions WHERE regionid = '%(id)s';" %query)
        elif regiontype == 'D':
            self.cursor.execute("SELECT ullon, lllon, ullat, urlat, urlon, lrlon, lllat, lrlat FROM system.regions WHERE regionid = '%(id)s';" %query)
        rec = self.cursor.fetchone()
        if rec == None:
            print ("SELECT ullon, lllon, ullat, urlat, urlon, lrlon, lllat, lrlat FROM regions.regions WHERE regionid = '%(id)s';" %query)
        return rec

    def _SelectClimateIndexOLD(self,period,index):
        query = {'index':index, 'sdate': period.startdate, 'edate':period.enddate}
        self.cursor.execute("SELECT acqdate, value FROM climateindex.climindex WHERE \
            index = '%(index)s' AND acqdate >= '%(sdate)s' AND acqdate <= '%(edate)s' ORDER BY acqdate" %query )
        recs = self.cursor.fetchall()
        if len (recs) == 0:
            print ("SELECT acqdate, value FROM climateindex.climindex WHERE \
            index = '%(index)s' AND acqdate >= '%(sdate)s' AND acqdate <= '%(edate)s' ORDER BY acqdate" %query )
        return recs
