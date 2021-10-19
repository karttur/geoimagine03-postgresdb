'''
Created on 23 feb. 2018

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.postgresdb import PGsession

from geoimagine.postgresdb.compositions import InsertCompDef, InsertCompProd, InsertLayer, SelectComp

class CommonRegions:
    '''
    '''
    
    def _SelectRegionTiles(self, regtype, schema, mask, paramL):
        '''
        ''' 
        
        if regtype == 'siteid':
        
            queryD = {'regionid':mask, 'regiontype':'site'}

            tiles = self._MultiSearch(queryD, paramL, schema, 'regions')
            
            if len(tiles) > 0:
            
                return tiles
        
        elif regtype == 'tractid':
        
            queryD = {'regionid':mask, 'regiontype':'tract'}

            tiles = self._MultiSearch(queryD, paramL, schema, 'regions')
            
            if len(tiles) > 0:
            
                return tiles
        
        elif regtype == 'regionid':
            
            queryD = {'regionid':mask, 'regiontype':'default'}
            
            tiles = self._MultiSearch(queryD, paramL, schema, 'regions')
            
            if len(tiles) > 0:
            
                return tiles
            
        else:
            
            print (regtype)
            exit('Unrecognized regtype in postgresdb.regions._SeletRegionTiles')


    def _InsertRegionTileMovedToRegionCOPIEDHERETOCOMAPRE(self, query):
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


    def _InsertRegionTile(self, query):
        '''
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
            
            warnstr = 'WARNING can not add tile to system %(system)s, no such region in system  %(system)s and table %(table)s' %query
            
            exit (warnstr)
        '''
                            
        if query['system'] == 'modis':
            
            sql = "SELECT * FROM modis.regions WHERE htile = %(htile)s AND vtile = %(vtile)s AND regiontype = '%(regiontype)s' AND regionid = '%(regionid)s';" %query
        
        elif query['system'][0:4] == 'ease':
            
            sql = "SELECT * FROM %(system)s.regions WHERE xtile = %(xtile)s AND ytile = %(ytile)s AND regiontype = '%(regiontype)s' AND regionid = '%(regionid)s';" %query
 
        else:
            
            print (query)

            exitstr = 'add system %(system)s in postgresdb.region._InsertRegionTile' %query
  
            exit(exitstr) 
            
        self.cursor.execute( sql )
        
        record = self.cursor.fetchone()

        if record == None and not query['delete']:
            
            if query['system'] == 'modis':
                
                sql = "INSERT INTO %(system)s.regions (regionid, regiontype, htile, vtile)\
                        VALUES ('%(regionid)s', '%(regiontype)s', %(htile)d, %(vtile)d)" %query 
        
            elif query['system'][0:4] == 'ease':
                
                sql = "INSERT INTO %(system)s.regions (regionid, regiontype, xtile, ytile)\
                        VALUES ('%(regionid)s', '%(regiontype)s', %(xtile)d, %(ytile)d)" %query 
                                 
            self.cursor.execute( sql )       
                                
            self.conn.commit()
    
        elif record and query['delete']:
            
            if query['system'] == 'modis':
                
                sql = "DELETE FROM %(system)s.regions WHERE  htile = '%(htile)s' AND vtile = '%(vtile)s' AND regiontype = '%(regiontype)s'AND regionid = '%(regionid)s';" %query
    
            elif query['system'][0:4] == 'ease':
                
                sql = "DELETE FROM %(system)s.regions WHERE  xtile = '%(xtile)s' AND ytile = '%(ytile)s' AND regiontype = '%(regiontype)s'AND regionid = '%(regionid)s';" %query
   
            self.cursor.execute( sql )
            
            self.conn.commit()
        
class ManageRegion(PGsession, CommonRegions):
    '''
    DB support for setting up processes
    '''

    def __init__(self, db, verbose = 1):
        """ The constructor connects to the database"""
        
        
        PGsession.__init__(self,'ManageMODIS')
                
        # Initiate the region common processes
        CommonRegions.__init__(self)
                
        # Set the HOST name for this process
        HOST = 'karttur'
        
        # Get the credentioals for the HOST
        query = self._GetCredentials( HOST )
        
        query['db'] = db

        #Connect to the Postgres Server
        self._Connect(query)
        
        # Set verbosiry
        
        self.verbose = verbose
        
    def _MultiSearchRecs(self, queryD, paramL, table, schema='regions', asdict=False):
        ''' Select multiple records
        '''
        return self._MultiSearch( queryD, paramL, schema, table)            
         
    def _InsertRegionCat(self,process):
        ''' Insert region categories
        '''
        
        if self._CheckWhitespace(process.parameters.regioncat):
            
            exitstr = 'EXITING - the regioncat "%s" contains whithespace' %(process.parameters.regioncat)
            
            exit(exitstr)
            
        if not process.parameters.regioncat == process.parameters.regioncat.lower():
            
            exitstr = 'EXITING - the regioncat "%s" contains upper case' %(process.parameters.regioncat)
            
            exit(exitstr)
        
        queryD = {'parentcat':process.parameters.parentcat, 'regioncat':process.parameters.regioncat}
        #Get the parentid from all cats except tracts and global
        
        if process.parameters.stratum == 0:
            
            exit ('EXITING - _InsertRegionCat failed because Stratum 0 can not be altered')

        if process.parameters.parentcat == '*' and process.parameters.stratum > 11:
            
            catrec = True
            
        else:
            
            sql = "SELECT regioncat FROM system.regioncats WHERE regioncat = '%(parentcat)s';" %queryD

            self.cursor.execute(sql)
            
            catrec = self.cursor.fetchone()
            
        if catrec != None:
            
            #check for the regioncat itself
            #query = {'cat': region.regioncat}
            
            sql = "SELECT * FROM system.regioncats WHERE regioncat = '%(regioncat)s';" %queryD
            
            self.cursor.execute(sql)
            
            record = self.cursor.fetchone()
            
            if record != None: 
                
                if process.overwrite or process.delete:
                    
                    sql = "DELETE FROM system.regioncats WHERE regioncat = '%(regioncat)s';" %queryD
                
                    self.cursor.execute(sql)
                    
                    self.conn.commit()
                    
                    if process.delete:
                        
                        return
                    
                else:
                    
                    return
                
            # INSERT region category     
            sql = "INSERT INTO system.regioncats (regioncat, parentcat, stratum, title, label) VALUES ('%s', '%s', %s, '%s', '%s')"\
                    %(process.parameters.regioncat, process.parameters.parentcat, process.parameters.stratum,process.parameters.title, process.parameters.label)
            
            self.cursor.execute(sql)
            
            self.conn.commit()
                
        else:
            
            exitstr = 'The parentcat region %s for region %s does not exists, it must be added proir to the region' %(process.parameters.prantid, process.parameters.regioncat)
            
            exit(exitstr)

    def _Insert1DegDefRegion(self, query):
        '''
        '''
        
        if self._CheckWhitespace(query['regionid']):
            
            exitstr = 'EXITING - the regioncat "%s" contains whithespace' %(query['regionid'])
            
            exit(exitstr)
            
        if not query['regionid'] == query['regionid'].lower():
            
            exitstr = 'EXITING - the regioncat "%s" contains upper case' %(query['regionid'])
            
            exit(exitstr)
            
        self._CheckInsertSingleRecord(query,'system','defregions')

    def _InsertDefRegion(self, layer, query, bounds, llD, overwrite, delete):
        '''
        '''
        
        if self._CheckWhitespace(query['regionid']):
            
            exitstr = 'EXITING - the regioncat "%s" contains whithespace' %(query['regionid'])
            
            exit(exitstr)
            
        if not query['regionid'] == query['regionid'].lower():
            
            exitstr = 'EXITING - the regioncat "%s" contains upper case' %(query['regionid'])
            
            exit(exitstr)
            
        if overwrite or delete:
            
            self.cursor.execute("DELETE FROM system.defregions WHERE regionid = '%(regionid)s' AND regioncat ='%(parentcat)s' AND parentid ='%(parentid)s'  ;" %query)
            
            self.conn.commit()
            
            if delete:
            
                self._InsertRegion(query, bounds, llD, overwrite, delete)
                
                return
            
        #Check that the regioncat is correctly set
        self.cursor.execute("SELECT * FROM system.regioncats WHERE regioncat = '%(regioncat)s';" %query)
        
        record = self.cursor.fetchone()
        
        if record == None:
        
            exitstr = 'the regioncat %(regioncat)s does not exist in the regioncats table' %query
            
            exit(exitstr)

        #Check that the parent regions is set
        self.cursor.execute("SELECT * FROM system.defregions WHERE regionid = '%(parentid)s' AND regioncat ='%(parentcat)s' ;" %query)
        
        record = self.cursor.fetchone()
        
        if record == None:
            
            if query['parentid'] in ['south-america','antarctica'] and query['parentcat'] == 'subcontinent':
                
                xquery = {'parentid':query['parentid'], 'parentcat':'continent'}
                
                self.cursor.execute("SELECT * FROM system.defregions WHERE regionid = '%(parentid)s' AND regioncat ='%(parentcat)s' ;" %xquery)
                
                record = self.cursor.fetchone()
                
                if record == None:
                    
                    exitstr = 'the parentid region "%s" of regioncat "%s" does not exist in the defregions table' %(query['parentid'], query['parentcat'])
                    
                    exit(exitstr)
            else:

                exitstr = 'the parentid region "%s" of regioncat "%s" does not exist in the defregions table' %(query['parentid'], query['parentcat'])
               
                print ("SELECT * FROM system.defregions WHERE regionid = '%(parentid)s' AND regioncat ='%(parentcat)s' ;" %query)

                exit(exitstr)

        #Check if the region itself already exists

        self.cursor.execute("SELECT regioncat FROM system.defregions WHERE regionid = '%(regionid)s';" %query)
        
        record = self.cursor.fetchone()
        
        if record == None:
            
            self.cursor.execute('INSERT INTO system.defregions (regioncat, regionid, regionname, parentid, title, label) VALUES (%s, %s, %s, %s, %s, %s)',
                                (query['regioncat'], query['regionid'], query['regionname'], query['parentid'], query['title'], query['label']))
            
            self.conn.commit()

        else:
            
            if query['regioncat'] != record[0]:
                
                pass
            
                '''
                if layer.locus.locus in ['antarctica','south-america']:
                
                    query2 = {'id': layer.locus.locus,'cat':query['regioncat']}
                    
                    self.cursor.execute("SELECT regioncat FROM system.defregions WHERE regionid = '%(id)s' and regioncat = '%(cat)s';" %query2)
                    
                    record = self.cursor.fetchone()
                    
                    if record == None:
                    
                        self.cursor.execute('INSERT INTO system.defregions (regioncat, regionid, regionname, parentid, title, label) VALUES (%s, %s, %s, %s, %s, %s)',
                                            (query['regioncat'], query['regionid'], query['regionname'], query['parentid'], query['title'], query['label']))
                        
                        self.conn.commit()
                else:
                    
                    pass
                '''

        query['system'] = 'system'
        
        query['regiontype'] = 'D'
        
        self._InsertRegion(query, bounds, llD, overwrite, delete)

        InsertCompDef(self,layer.comp)
        
        InsertCompProd(self,layer.comp)
                
        InsertLayer(self, layer, overwrite, delete)

    def _InsertRegion(self, query, bounds, llD, overwrite, delete):
        '''
        '''
        
        if overwrite or delete:
            self.cursor.execute("DELETE FROM %(system)s.regions WHERE regionid = '%(regionid)s';" %query)
            self.conn.commit()
            if delete:
                return

        self.cursor.execute("SELECT * FROM %(system)s.regions WHERE regionid = '%(regionid)s';" %query)
        record = self.cursor.fetchone()
        if record == None:
            
            sql = "INSERT INTO %(system)s.regions (regionid, regioncat, regiontype) VALUES \
                    ('%(regionid)s', '%(regioncat)s', '%(regiontype)s');" %query
            
            self.cursor.execute( sql )

            self.conn.commit()
            query['epsg'] = query['epsg']
            query['minx'] = bounds[0]
            query['miny'] = bounds[1]
            query['maxx'] = bounds[2]
            query['maxy'] = bounds[3]
            
            sql = "UPDATE %(system)s.regions SET (epsg, minx, miny, maxx, maxy) = \
                    (%(epsg)s, %(minx)s, %(miny)s, %(maxx)s, %(maxy)s) WHERE regionid = '%(regionid)s';" %query
                    
            self.cursor.execute( sql )

            self.conn.commit()
            
            for key in llD:
                
                query[key] = llD[key]


            sql = "UPDATE %(system)s.regions SET (ullon,ullat,urlon,urlat,lrlon,lrlat,lllon,lllat) = \
                    (%(ullon)s,%(ullat)s,%(urlon)s,%(urlat)s,%(lrlon)s,%(lrlat)s,%(lllon)s,%(lllat)s) WHERE regionid = '%(regionid)s';" %query
                    
            self.cursor.execute( sql )
            
            self.conn.commit()
            
        elif record[0] != query['regioncat']:
            
            if query['regionid'] in ['antarctica','south-america']:
                
                pass
            
            else:
                                
                exitstr = 'Duplicate categories (%s %s) for regionid %s' %(record[0],query['regioncat'], query['regionid'])
                
                exit(exitstr)
        #TGTODO duplicate name for tract but different user???, delete and overwrite

    def _SelectComp(self,comp):
        #comp['system'] = system
        return SelectComp(self, comp)

    def _LoadBulkDefregions(self,tmpFPN):
        #self._DeleteSRTMBulkTiles(params)
        #query = {'tmpFPN':tmpFPN, 'items': ",".join(headL)}
        #print ("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #self.cursor.execute("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #print ("%(tmpFPN)s, 'modis.datapooltiles', columns= (%(items)s), sep=','; "%query)
        print ('Copying from',tmpFPN)
        with open(tmpFPN, 'r') as f:
            next(f)  # Skip the header row.
            self.cursor.copy_from(f, 'system.defregions', sep=',')
            self.conn.commit()
            #cur.copy_from(f, 'test', columns=('col1', 'col2'), sep=",")

    def _LoadBulkRegions(self,tmpFPN, skip1st=True):

        #query = {'tmpFPN':tmpFPN, 'items': ",".join(headL)}
        #print ("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #self.cursor.execute("COPY modis.datapooltiles (%(items)s) FROM '%(tmpFPN)s' DELIMITER ',' CSV HEADER;" %query)
        #print ("%(tmpFPN)s, 'modis.datapooltiles', columns= (%(items)s), sep=','; "%query)
        print ('Copying from',tmpFPN)
        with open(tmpFPN, 'r') as f:
            if skip1st:
                next(f)  # Skip the header row.
            self.cursor.copy_from(f, 'system.regions', sep=',')
            self.conn.commit()
          
    def _SelectTileCoords(self, system, paramL):
        '''
        '''
        cols = ",".join(paramL)
        
        queryD = {'cols':cols,'schema':system}
        
        sql = "SELECT %(cols)s FROM %(schema)s.tilecoords;" %queryD
        
        if self.verbose > 2:
            
            print (sql)
        
        self.cursor.execute( sql )
        
        records = self.cursor.fetchall()
        
        return records

    def _SelectDefaultRegionLayers(self, system, wherestatement = '' ):
        '''
        '''
        
        query = {'schema': system, 'table':'regions', 'where':wherestatement}
        
        if wherestatement == '':
            
            sql = "SELECT regioncat, regionid FROM system.defregions;"
            
            sql = "SELECT R.regioncat, R.regionid, L.compid, L.source, L.product,\
                D.content, D.layerid, D.prefix, L.suffix, L.acqdatestr FROM system.defregions\
                R LEFT JOIN system.layer L ON (R.regionid = L.regionid)\
                LEFT JOIN system.compdef D ON (L.compid = D.compid);"
            
        else:
            
            sql = "SELECT R.regioncat, R.regionid, L.compid, L.source, L.product,\
                D.content, D.layerid, D.prefix, L.suffix, L.acqdatestr FROM system.defregions R\
                LEFT JOIN system.regions S ON (R.regionid = S.regionid)\
                LEFT JOIN system.layer L ON (R.regionid = L.regionid)\
                LEFT JOIN system.compdef D ON (L.compid = D.compid)\
                LEFT JOIN %(schema)s.%(table)s M ON (R.regionid = M.regionid)\
                WHERE %(where)s;" %query
                
                
            #sql = "SELECT DISTINCT R.regioncat, R.regionid FROM system.defregions R LEFT JOIN %(schema)s.%(table)s M ON (R.regionid = M.regionid) WHERE %(where)s;" %query
         
        if self.verbose > 1:
            
            print (sql)
            
            self.cursor.execute( sql )
                
            return self.cursor.fetchall()
                
    def _SelectTilesWithinWSEN(self, system, paramL, west, south, east, north):
        '''
        '''
        
        cols = ",".join(paramL)
        
        query = {'schema':system,'cols':cols, 'west':west, 'south':south,'east':east,'north':north}
        
        sql = "SELECT %(cols)s FROM %(schema)s.tilecoords WHERE east > %(west)s AND west < %(east)s AND north > %(south)s AND south < %(north)s;" %query

        self.cursor.execute( sql )

        records = self.cursor.fetchall()
        
        return records
    
    