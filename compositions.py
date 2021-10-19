'''
Created on 8 mars 2018
Updated 2 januari 2021

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.support import Today
        
def InsertCompDef(session, comp, title=False, label=False):
    ''' Insert composition definitions
    '''

    def CheckCompDef():
        '''
        '''
        
        query = {'id':comp.compid,'system':comp.system}
        
        sql = "SELECT content, layerid, scalefac, offsetadd, dataunit, measure FROM %(system)s.compdef WHERE compid = '%(id)s';" %query
        
        session.cursor.execute(sql)
        
        record = session.cursor.fetchone()
        
        if record == None:
        
            return True
        
        if record[5] == 'N': #Nominal data, no check
        
            return False
        
        inT = (comp.content, comp.layerid, comp.scalefac, comp.offsetadd, comp.dataunit, comp.measure)
        
        diff = [x for x,y in zip(inT,record) if x != y]
        
        if len(diff) != 0:
            
            
            
            itemL = ['content','layerid','scalefac','offsetadd','dataunit','measure']
            
            diffix = 0
            
            for n in range(len(inT)):
            
                if inT[n] != record[n]:
                    
                    if itemL[n] in ['scalefac', 'offsetadd']:
                        
                        if float( inT[n] ) == float( record[n] ):
                          
                            diffix +=1
                            
                        else:
                            
                            print ('    Error at',itemL[n], inT[n], record[n])
                    
            if len(diff) - diffix == 0:
                
                return False
            
            exitstr = 'EXITING: duplicate compid %s, you must change the layerid name %s %s' %(comp.compid, inT,record)
            
            exit(exitstr)
        
        else:
        
            return False
        
        #end CheckCompDef 
         
    addrec = CheckCompDef()

    if addrec:

        tableschema = '%(schema)s.compdef' %{'schema':comp.system}
        
        query ={'table':tableschema, 'compid':comp.compid, 'content':comp.content, 'layerid':comp.layerid, 'prefix':comp.prefix,
                'measure':comp.measure, 'dataunit':comp.dataunit, 'scalefac':comp.scalefac, 'offsetadd':comp.offsetadd}
        
        sql = "INSERT INTO %(table)s (compid, content, layerid, prefix, measure, dataunit, scalefac, offsetadd)\
                VALUES ('%(compid)s','%(content)s','%(layerid)s','%(prefix)s','%(measure)s','%(dataunit)s',%(scalefac)s, %(offsetadd)s)" %query
        
        session.cursor.execute(sql)
        
        session.conn.commit()
        
        if title:
        
            query['title'] = title
            
            sql = "UPDATE %(table)s SET title = '%(title)s' WHERE compid = '%(compid)s';" %query

            session.cursor.execute( sql )
            
            session.conn.commit()
        
        if label:
        
            query['label'] = label
            
            sql = "UPDATE %(table)s SET label = '%(label)s' WHERE compid = '%(compid)s';" %query
        
            session.cursor.execute( sql )
        
            session.conn.commit()
                 
def InsertCompProd(session, comp):
    ''' Insert composition prodction
    '''
    
    def CheckCompProd():
        '''
        '''
        
        sql = "SELECT cellnull, celltype FROM %(system)s.compprod WHERE compid = '%(compid)s' AND source = '%(source)s' AND product = '%(product)s' AND suffix = '%(suffix)s';" %query
        
        session.cursor.execute( sql )
        
        record = session.cursor.fetchone()
        
        if record != None:
        
            record = (int(record[0]), record[1].lower())
            
            inT = (int(comp.cellnull), comp.celltype.lower())
            
            diff = [x for x,y in zip(inT,record) if x != y]
            
            if len(diff) != 0:
            
                exitstr = 'EXITING: duplicate compprod %s, you must change the layerid name %s %s' %(comp.compid, inT,record)

                exit(exitstr)
                
                itemL = ['cellnull','celltype']
                
                for n in range(len(inT)):
                
                    if inT[n] != record[n]:
                    
                        print ('    Error at',itemL[n], inT[n], record[n])
                
                exit(exitstr)
            
            else:
                
                return False
        
        elif record == None:
        
            return True
        
        #end CheckCompProd
        
    '''    
    if comp.cellnull < -32768:
        
        comp.cellnull = -32768 
        
    elif comp.cellnull > 32767:
        
        comp.cellnull = 32768
    '''
        
    #cast the comp class to a dict
    
    query = comp.__dict__

    addrec = CheckCompProd()
    
    if addrec:
        
        session.cursor.execute("INSERT INTO %(system)s.compprod (compid, system, source, product, suffix, cellnull, celltype) VALUES ('%(compid)s','%(system)s','%(source)s','%(product)s','%(suffix)s',%(cellnull)s,'%(celltype)s');" %query)
        
        session.conn.commit()
            
def InsertLayer(session,layer,overwrite,delete): 
    '''
    '''
    
    def InsertRegionLayer():
        '''
        '''
        
        query = {'system':layer.comp.system, 'compid': layer.comp.compid, 'source':layer.comp.source ,'product':layer.comp.product, 'suffix':layer.comp.suffix, 'acqdatestr':layer.datum.acqdatestr, 'regionid':layer.locus.locus, 'today':Today()}
        
        session.cursor.execute("SELECT * FROM %(system)s.layer WHERE compid = '%(compid)s' AND product = '%(product)s' AND suffix = '%(suffix)s' AND regionid = '%(regionid)s' AND acqdatestr = '%(acqdatestr)s';" %query)

        record = session.cursor.fetchone()
        
        if record != None and (delete or overwrite):
        
            session.cursor.execute("DELETE FROM %(system)s.layer WHERE compid = '%(compid)s' AND product = '%(product)s' AND suffix = '%(suffix)s' AND regionid = '%(regionid)s' AND acqdatestr = '%(acqdatestr)s';" %query)
            
            if delete:
            
                return
        
        if record == None or overwrite:
            
            sql = "INSERT INTO %(system)s.layer (compid, source, product, suffix, regionid, acqdatestr, createdate) VALUES ('%(compid)s', '%(source)s', '%(product)s', '%(suffix)s', '%(regionid)s', '%(acqdatestr)s', '%(today)s')" %query
        
            session.cursor.execute( sql )

            session.conn.commit()
            
            if layer.datum.acqdate:
            
                query['acqdate'] = layer.datum.acqdate
                
                query['doy'] = layer.datum.doy
                
                session.cursor.execute("UPDATE %(system)s.layer SET (acqdate, doy) = ('%(acqdate)s', %(doy)d) WHERE compid = '%(compid)s' AND product = '%(product)s' AND suffix = '%(suffix)s' AND regionid = '%(regionid)s' AND acqdatestr = '%(acqdatestr)s';" %query)
                
                session.conn.commit()
        
        #end InsertRegionLayer
        
    def InsertSentinelLayer():
        '''
        '''
                
        queryD = {'compid': layer.comp.compid, 'source':layer.comp.source ,'product':layer.comp.product, 'suffix':layer.comp.suffix, 
                  'acqdatestr':layer.datum.acqdatestr, 'utm':layer.locus.utm, 'mgrsid':layer.locus.mgrsid, 'orbitid':layer.locus.orbitid}
        
        if layer.datum.acqdate:
        
            queryD['acqdate'] = layer.datum.acqdate
            
            queryD['doy'] = layer.datum.doy    
        
        session._CheckInsertSingleRecord(queryD, layer.comp.system, 'layers')
        
    def InsertMODISLayer():
        '''
        '''
        
        queryD = {'compid': layer.comp.compid, 'source':layer.comp.source ,'product':layer.comp.product, 'suffix':layer.comp.suffix, 
                  'acqdatestr':layer.datum.acqdatestr, 'htile':layer.locus.htile, 'vtile':layer.locus.vtile, 'hvtile':layer.locus.locus}
        
        if layer.datum.acqdate:
        
            queryD['acqdate'] = layer.datum.acqdate
            
            queryD['doy'] = layer.datum.doy   

        session._CheckInsertSingleRecord(queryD, layer.comp.system, 'layers')
        
    def InsertEASELayer():
        '''
        '''
        
        queryD = {'compid': layer.comp.compid, 'source':layer.comp.source ,'product':layer.comp.product, 'suffix':layer.comp.suffix, 
                  'acqdatestr':layer.datum.acqdatestr, 'xtile':layer.locus.xtile, 'vtile':layer.locus.ytile, 'xytile':layer.locus.locus}
        
        if layer.datum.acqdate:
        
            queryD['acqdate'] = layer.datum.acqdate
            
            queryD['doy'] = layer.datum.doy   

        session._CheckInsertSingleRecord(queryD, layer.comp.system, 'layers')
        
    def InsertSpecimenLayer():
        '''
        '''
        
        FIX
        
    def InsertLandsatLayer():
        
        queryD = {'compid': layer.comp.compid, 'source':layer.comp.source ,'product':layer.comp.product, 'suffix':layer.comp.suffix, 
                  'acqdatestr':layer.datum.acqdatestr, 'wrspath':layer.locus.wrspath, 'wrsrow':layer.locus.wrsrow, 'wrspr':layer.locus.wrspr}
        
        if layer.datum.acqdate:
        
            queryD['acqdate'] = layer.datum.acqdate
            
            queryD['doy'] = layer.datum.doy   

        session._CheckInsertSingleRecord(queryD, layer.comp.system, 'layers')
            
    InsertCompDef(session, layer.comp)
    
    InsertCompProd(session, layer.comp)
        
    if layer.comp.system == 'system':
    
        InsertRegionLayer()    
    
    elif layer.comp.system == 'regions':
        
        InsertRegionLayer()
   
    elif layer.comp.system == 'ancillary':
        #Ancillary is a kind of regional data
        InsertRegionLayer()
        
    elif layer.comp.system == 'smap':
        #Smap is a kind of regional data (no tiles)
        InsertRegionLayer()
        
    elif layer.comp.system == 'specimen':
        
        InsertSpecimenLayer(layer.comp.system,layer)
        
    elif layer.comp.system == 'landsat':
        
        InsertLandsatLayer()
        
    elif layer.comp.system == 'modis':
        
        InsertMODISLayer()
        
    elif layer.comp.system == 'sentinel':
        
        InsertSentinelLayer()
        
    elif layer.comp.system[0:4] == 'ease':
        
        InsertEaseLayer()
        
    else:
        
        exitstr = 'unknown system (compositions.py: InsertLayer): %s' %(layer.comp.system)
        
        exit(exitstr)
               
def DeleteLayer(session,layer,overwrite,delete):
    '''
    '''
    
    def DeleteRegionLayer():
        
        query = {'system':layer.comp.system, 'compid': layer.comp.compid, 'source':layer.comp.source ,'product':layer.comp.product, 'suffix':layer.comp.suffix, 'acqdatestr':layer.datum.acqdatestr, 'regionid':layer.locus.locus, 'today':Today()}
        
        session.cursor.execute("SELECT * FROM %(system)s.layer WHERE compid = '%(compid)s' AND product = '%(product)s' AND suffix = '%(suffix)s' AND regionid = '%(regionid)s' AND acqdatestr = '%(acqdatestr)s';" %query)

        record = session.cursor.fetchone()
        
        if record != None and (delete or overwrite):
        
            session.cursor.execute("DELETE FROM %(system)s.layer WHERE compid = '%(compid)s' AND product = '%(product)s' AND suffix = '%(suffix)s' AND regionid = '%(regionid)s' AND acqdatestr = '%(acqdatestr)s';" %query)
            
            if delete:
            
                return
            
        if record == None or overwrite:
            
            session.cursor.execute("INSERT INTO %(system)s.layer (compid, source, product, suffix, regionid, acqdatestr, createdate) VALUES ('%(compid)s', '%(source)s', '%(product)s', '%(suffix)s', '%(regionid)s', '%(acqdatestr)s', '%(today)s')" %query)

            session.conn.commit()
            
            if layer.datum.acqdate:
            
                query['acqdate'] = layer.datum.acqdate
                
                query['doy'] = layer.datum.doy
                
                session.cursor.execute("UPDATE %(system)s.layer SET (acqdate, doy) = ('%(acqdate)s', %(doy)d) WHERE compid = '%(compid)s' AND product = '%(product)s' AND suffix = '%(suffix)s' AND regionid = '%(regionid)s' AND acqdatestr = '%(acqdatestr)s';" %query)
                
                session.conn.commit()
        
        #end InsertRegionLayer
        
    def DeleteSentinelLayer():
        '''
        '''

        queryD = {'compid': layer.comp.compid, 'source':layer.comp.source ,'product':layer.comp.product, 'suffix':layer.comp.suffix, 
                  'acqdatestr':layer.datum.acqdatestr, 'utm':layer.locus.utm, 'mgrsid':layer.locus.mgrsid, 'orbitid':layer.locus.orbitid}
        
        if layer.datum.acqdate:
        
            queryD['acqdate'] = layer.datum.acqdate
            
            queryD['doy'] = layer.datum.doy    
        
        session._CheckDeleteSingleRecord(queryD, layer.comp.system, 'layers')
        
    def DeleteMODISLayer():
        '''
        '''
        
        queryD = {'compid': layer.comp.compid, 'source':layer.comp.source ,'product':layer.comp.product, 'suffix':layer.comp.suffix, 
                  'acqdatestr':layer.datum.acqdatestr, 'htile':layer.locus.htile, 'vtile':layer.locus.vtile, 'hvtile':layer.locus.locus}
        
        if layer.datum.acqdate:
        
            queryD['acqdate'] = layer.datum.acqdate
            
            queryD['doy'] = layer.datum.doy   

        session._CheckDeleteSingleRecord(queryD, layer.comp.system, 'layers')
                    
    if layer.comp.system == 'system':
        
        DeleteRegionLayer()    
    
    elif layer.comp.system == 'regions':
    
        DeleteRegionLayer()
    
    elif layer.comp.system == 'ancillary':
        #Anciallary is a kind of regional data 
        DeleteRegionLayer()
    
    elif layer.comp.system == 'smap':
        #Smap is a kind of regional data (no tiles)
        DeleteRegionLayer()
    
    elif layer.comp.system == 'specimen':
    
        DeleteSpecimenLayer(layer.comp.system,layer)
    
    elif layer.comp.system == 'landsat':
        
        DeleteLandsatLayer(layer.comp.system,layer)
    
    elif layer.comp.system == 'modis':
    
        DeleteMODISLayer()
    
    elif layer.comp.system == 'sentinel':
    
        DeleteSentinelLayer()
    
    else:
    
        exitstr = 'unknown system (compositions.py: InsertLayer): %s' %(layer.comp.system)
     
        exit(exitstr)
               
def DeleteComposition(session,schema,compD):
    '''
    '''
    
    #Test for compprod first
    
    selectQuery = {}
    
    for item in ['compid','source','product','suffix']:
    
        col = 'D.%s' %(item)
        
        selectQuery[col] = {'op':'=', 'val': compD[item]}
    
    wherestatement = session._DictToSelect(selectQuery)
    
    selectQuery = {'schema':schema,'select': wherestatement}
    
    query = "SELECT D.compid, D.source, D.product, D.suffix FROM %(schema)s.compprod D LEFT JOIN %(schema)s.layer L USING (compid) \
            %(select)s  AND L.compid IS NULL;" %selectQuery

    session.cursor.execute(query)
    
    records = session.cursor.fetchall()
    
    if len(records) == 1:
        #Delete the compprod, it has no associated layers
        
        query = "DELETE FROM %(schema)s.compprod D%(select)s;" %selectQuery
    
        print ( query )
        
        BUELL
        
        session.cursor.execute(query)
        
    session.conn.commit()
                     
def SelectCompleteComp(session, system, queryD, searchItemL):
    '''
    '''
    queryD['system'] = system
    
    querystem = 'SELECT C.source, C.product, B.content, B.layerid, B.prefix, C.suffix, C.masked, C.cellnull, C.celltype, B.measure, B.scalefac, B.offsetadd, B.dataunit '   
    
    query ='FROM %(system)s.compdef AS B ' %queryD
    
    querystem = '%s %s ' %(querystem, query)
    
    query ='INNER JOIN %(system)s.compprod AS C ON (B.compid = C.compid)' %queryD
    
    querystem = '%s %s ' %(querystem, query)
    
    selectQuery = {}
    
    for item in searchItemL:
    
        selectQuery[item] = {'col': item, 'op':'=', 'val': queryD[item]}

    wherestatement = session._DictToSelect(selectQuery)

    querystem = '%s %s;' %(querystem, wherestatement)
    
    session.cursor.execute(querystem)
    
    record = session.cursor.fetchone()
    
    if record != None:
        
        params = ['source', 'product', 'content', 'layerid', 'prefix', 'suffix', 'masked', 'cellnull', 'celltype', 'measure', 'scalefac', 'offsetadd', 'dataunit'] 

        return dict(zip(params,record))
            
    return record

def SelectCompAlt(session,compQ,inclL):
    '''
    '''
    exit('replaced with _SelectCompleteComposition')
        
def SelectComp(session, compQ):
    '''
    '''
    params = ['source', 'product', 'content', 'layerid', 'prefix', 'suffix', 'masked', 'cellnull', 'celltype', 'measure', 'scalefac', 'offsetadd', 'dataunit']
    
    searhItemL = ['content','layerid']

    records,query = SelectCompAlt(session,compQ,searhItemL)
    
    if len(records) == 1:
        
        recD = dict(zip(params,records[0]))
        
        if self.verbose > 1:
            print ('recD',recD)
            print ('compQ',compQ)
        if 'source' in compQ and not recD['source'] == compQ['source']:
            print ('Error in source',recD['source'], compQ['source'])
            print (query)
            SNULLE
        if 'product' in compQ and not recD['product'] == compQ['product']:
            print ('Error in product',recD['product'], compQ['product'])
            print (query)
            SNULLE
        if 'suffix' in compQ and not recD['suffix'] == compQ['suffix']:
            print ('Error in suffix',recD['suffix'], compQ['suffix'])
            print (query)
            SNULLE
            
        return recD
    
    else:
        
        inclL = ['content','layerid', 'source', 'product', 'suffix']
        
        records,query = SelectCompAlt(session,compQ,inclL)
        if len(records) == 1:
            recD = dict(zip(params,records[0]))
            if verbose > 1:
                print ('recD',recD)
                print ('compQ',compQ)
                print ('recD',recD)
                print ('compQ',compQ)
            if not recD['source'] == compQ['source']:
                print ('Error in source',recD['source'], compQ['source'])
            if not recD['product'] == compQ['product']:
                print ('Error in product',recD['product'], compQ['product'])
            if not recD['suffix'] == compQ['suffix']:
                print ('Error in suffix',recD['suffix'], compQ['suffix'])
            return recD
        else:
            print ('session', session.name)
            print ('records', records)
            print ('compQ',compQ)
            print ('query',query)
            SNULLEBULLE

def SelectSystemCompOnRegionIdOld(session, compQ, inclL):
    '''
    '''
    
    params = ['source', 'product', 'content', 'layerid', 'prefix', 'suffix', 'masked', 'cellnull', 'celltype', 'measure', 'scalefac', 'offsetadd', 'dataunit']

    records,query = SelectCompAlt(session,compQ,inclL)
    
    if len(records) == 1:
    
        return dict(zip(params,records[0]))
   
    
    print ('records', records)
    
    print ('query',compQ)
    
    SNULLEBULLE
    
    
            
def SelectLayerOld(session, compQ):
    '''
    '''
    
    params = ['source', 'product', 'content', 'layerid', 'prefix', 'suffix', 'masked', 'cellnull', 'celltype', 'measure', 'scalefac', 'offsetadd', 'dataunit']

    inclL = ['compid','layerid']
    
    records,query = SelectCompAlt(session,compQ,inclL)
    
    if len(records) == 1:
        
        return dict(zip(params,records[0]))
    
    else:
        
        inclL = ['content','layerid', 'suffix']
        
        records,query = SelectCompAlt(session,compQ,inclL)
        
        if len(records) == 1:
            
            return dict(zip(params,records[0]))
        
        else:
            
            print ('records', records)
            
            print ('query',compQ)
            
            SNULLE