'''
Created on 21 feb. 2018

@author: thomasgumbricht
'''

# Standard library imports

import netrc

from base64 import b64encode, b64decode

from string import whitespace

# Third party imports

import psycopg2

class CommonSearch():
    '''
    '''
    
    def __init__(self):
        '''
        '''
        
        pass
                 
                 
    def _SingleSearch(self,queryD, paramL, schema, table):
        #self._GetTableKeys(schema, table)
        selectQuery = {}
        for item in queryD:

            if isinstance(queryD[item],dict):
                #preset operator and value
                selectQuery[item] = queryD[item]
            else:
                selectQuery[item] = {'op':'=', 'val':queryD[item]}
        wherestatement = self._DictToSelect(selectQuery)  
        cols =  ','.join(paramL)
        selectQuery = {'schema':schema, 'table':table, 'select': wherestatement, 'cols':cols}
        
        sql = "SELECT %(cols)s FROM %(schema)s.%(table)s %(select)s" %selectQuery

        self.cursor.execute(sql)
        
        self.record = self.cursor.fetchone()
        
        if self.record == None:
            
            inforstr = '            %s' %(sql)
            
            print (inforstr)
            
        return self.record
          
    def _MultiSearch(self,queryD, paramL, schema, table):  
        ''' Select multiple records from any schema.table
        '''

        selectQuery = {}
        for item in queryD:

            if isinstance(queryD[item],dict):
                #preset operator and value
                selectQuery[item] = queryD[item]
            else:
                selectQuery[item] = {'op':'=', 'val':queryD[item]}
        wherestatement = self._DictToSelect(selectQuery) 

        if len(paramL) == 1:
            cols = paramL[0]
        else:
            cols =  ','.join(paramL)
        selectQuery = {'schema':schema, 'table':table, 'select': wherestatement, 'cols':cols}      
        query = "SELECT %(cols)s FROM %(schema)s.%(table)s %(select)s" %selectQuery
 
        if self.verbose > 1:
        
            print (query)
        
        self.cursor.execute(query)
        
        self.records = self.cursor.fetchall()
        
        return self.records 

    def _SelectAdjacentTiles(self, regionid, centertile, paramL, schema, table):
        '''
        '''
        selectQuery = {}
                
        selectQuery['regionid'] = {'op':'=', 'val':regionid}
        
        if schema == 'modis':
            
            tileid = ['htile','vtile']
            
        elif schema[0:4] == 'ease':
            
            tileid = ['xtile','ytile']
            
            
        selectQuery[ tileid[0] ] = {'op':'>=', 'val':centertile[0]-1}
        
        selectQuery[ '#'+tileid[0] ] = {'op':'<=', 'val':centertile[0]+1}
        
        selectQuery[ tileid[1] ] = {'op':'>=', 'val':centertile[1]-1}
        
        selectQuery[ '#'+tileid[1] ] = {'op':'<=', 'val':centertile[1]+1}
        

        wherestatement = self._DictToSelect(selectQuery) 

        if len(paramL) == 1:
            cols = paramL[0]
        else:
            cols =  ','.join(paramL)
        selectQuery = {'schema':schema, 'table':table, 'select': wherestatement, 'cols':cols}      
        query = "SELECT %(cols)s FROM %(schema)s.%(table)s %(select)s" %selectQuery
 
        if self.verbose > 1:
        
            print (query)
        
        self.cursor.execute(query)
        
        self.records = self.cursor.fetchall()
        
        return self.records

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
    
class PGsession(CommonSearch):
    '''
    Connects the postgres database.
    '''

    def __init__(self, name='unknown'):
        '''
        Constructor that opens the session, expects a dictionary with keys for 'db', 'user' and 'pswd', and a
        name [optional]
        '''
        self.name = name
        
        CommonSearch.__init__(self)
        
    def _GetCredentials(self, HOST):
        ''' Get the credentiials using netrc
        '''
                
        secrets = netrc.netrc()
        
        username, account, password = secrets.authenticators( HOST )
        
        pswd = b64encode(password.encode())
        
        #create a query dictionary for connecting to the Postgres server
        return {'user':username,'pswd':pswd, 'account':account}
        
    def _Connect(self,query):
        '''
        '''
        query['pswd'] = b64decode(query['pswd']).decode('ascii')
        
        conn_string = "host='localhost' dbname='%(db)s' user='%(user)s' password='%(pswd)s'" %query
        
        self.conn = psycopg2.connect(conn_string)
        
        self.cursor = self.conn.cursor() 

        self.verobse = 0
        
        
    def _Close(self):
        '''
        '''
        self.cursor.close()
        self.conn.close()
        
        
    def _SetVerbosity(self, verbose):
        '''
        '''
        self.verbose = verbose
        
        
    def _CheckWhitespace(self,s):
        '''
        '''
        return True in [c in s for c in whitespace]
                
    def _CheckDeleteSingleRecord(self, queryD, schema, table, tabkeys = [[]]):
        self._GetTableKeys(schema, table)
        selectQuery = {}
        if len(self.tabkeys) == 0:
            self.tabkeys = tabkeys

        for item in self.tabkeys:
            '''
            val = '\'%(v)s\'' %{'v':queryD[item[0]]}
            selectQuery[item[0]] = {'op':'=', 'val':val}
            '''
            selectQuery[item[0]] = {'op':'=', 'val':queryD[item[0]]}
        wherestatement = self._DictToSelect(selectQuery)
        selectQuery = {'schema':schema, 'table':table, 'select': wherestatement}
        query = "SELECT * FROM %(schema)s.%(table)s %(select)s" %selectQuery
        
        if self.verbose > 1:
            
            print ('    Query for PGsessin._CheckDeleteSingleRecord query\n        %s' %(query) )
            

        self.cursor.execute(query)
        records = self.cursor.fetchall()
        if len(records) == 1:

            query = "DELETE FROM %(schema)s.%(table)s %(select)s" %selectQuery
            if self.verbose > 1:
            
                print ('        PGsession._CheckDeleteSingleRecord query:\n        %s' %(query) )
            
            self.cursor.execute(query)
            self.conn.commit()

    def _CheckInsertSingleRecord(self, queryD, schema, table, tabkeys = [[]], overwrite=False, delete=False):
        '''
        '''
        self._GetTableKeys(schema, table)

        selectQuery = {}

        if len(self.tabkeys) == 0:
            
            self.tabkeys = tabkeys

        for item in self.tabkeys:

            selectQuery[item[0]] = {'op':'=', 'val':queryD[item[0]]}
            
        wherestatement = self._DictToSelect(selectQuery)
        
        selectQuery = {'schema':schema, 'table':table, 'select': wherestatement}
        
        query = "SELECT * FROM %(schema)s.%(table)s %(select)s" %selectQuery

        self.cursor.execute(query)
        
        rec = self.cursor.fetchone()

        if rec != None and delete:
            
            query = "DELETE FROM %(schema)s.%(table)s %(select)s" %selectQuery
            
            self.cursor.execute(query)
            
            self.conn.commit()
            
            return rec
        
        elif rec != None and overwrite:
            
            sql = "DELETE FROM %(schema)s.%(table)s %(select)s" %selectQuery

            self.cursor.execute(sql)
            
            self.conn.commit()
            
            self._InsertRecord(queryD, schema, table)
            
            return rec
        
        elif rec == None and not delete:
            
            self._InsertRecord(queryD, schema, table)

        return rec
            
    def _SingleSearchOld(self,queryD, paramL, schema, table):
        #self._GetTableKeys(schema, table)
        selectQuery = {}
        for item in queryD:

            if isinstance(queryD[item],dict):
                #preset operator and value
                selectQuery[item] = queryD[item]
            else:
                selectQuery[item] = {'op':'=', 'val':queryD[item]}
        wherestatement = self._DictToSelect(selectQuery)  
        cols =  ','.join(paramL)
        selectQuery = {'schema':schema, 'table':table, 'select': wherestatement, 'cols':cols}
        
        query = "SELECT %(cols)s FROM %(schema)s.%(table)s %(select)s" %selectQuery
        
        if self.verbose > 1:
            
            print ('        PGsession._SingleSearch query:\n        %s' %(query) )
            
        self.cursor.execute(query)
        self.records = self.cursor.fetchone()
        return self.records

    def _MultiSearchOld(self, queryD, paramL, schema, table):
        #self._GetTableKeys(schema, table)
        '''
        queryD = {}
            queryD['compid'] = {'val':campid, 'op':'=' }
            queryD['regionid'] = {'val':regionid, 'op':'=' }
        '''
        selectQuery = {}
        for item in queryD:
            '''
            val = '\'%(v)s\'' %{'v':queryD[item]}
            val = val.replace("''","'")
            selectQuery[item] = {'op':'=', 'val':val}
            '''
            if isinstance(queryD[item],dict):
                #preset operator and value
                selectQuery[item] = queryD[item]
            else:
                selectQuery[item] = {'op':'=', 'val':queryD[item]}
        wherestatement = self._DictToSelect(selectQuery) 

        if len(paramL) == 1:
            cols = paramL[0]
        else:
            cols =  ','.join(paramL)
        selectQuery = {'schema':schema, 'table':table, 'select': wherestatement, 'cols':cols}      
        query = "SELECT %(cols)s FROM %(schema)s.%(table)s %(select)s" %selectQuery
        
        if self.verbose > 1:
            
            print ('       PGsession._MultiSearch query:\n        %s' %(query) )

        self.cursor.execute(query)
        
        self.records = self.cursor.fetchall()
        
        return self.records
    
    def _InsertRecord(self, queryD, schema, table):
        '''
        '''
        
        self._DictToColumnsValues(queryD, schema, table)
        
        sql = "INSERT INTO %(schema)s.%(table)s (%(columns)s) VALUES (%(values)s);" %self.query
        
        if self.verbose > 1:
            
            print ('        PGsession._InsertRecrod query:\n        %s' %(sql) )
        
        self.cursor.execute(sql)
        
        self.conn.commit()
        
    def _UpdateRecord(self, queryD, schema, table, searchD):
        '''
        '''
        
        selectQuery = {}
        for item in searchD:
            selectQuery[item] = {'op':'=', 'val':searchD[item]}
        
        wherestatement = self._DictToSelect(selectQuery)
        
        self._DictToColumnsValues(queryD, schema, table)
        
        self.query['where'] = wherestatement
        
        sql = "UPDATE %(schema)s.%(table)s SET (%(columns)s) = (%(values)s) %(where)s;" %self.query

        if self.verbose > 1:
            
            print ('        PGsession._UpdateRecord query:\n        %s' %(sql) )
            
        self.cursor.execute( sql )
        self.conn.commit()
    
    def _DictToColumnsValues(self,queryD,schema,table):   
        '''
        Converts a dictionary, a schema and a table to a query
        ''' 
        cols = queryD.keys()
        vals = queryD.values()
        columns =  ','.join(cols)
        values =  ','.join(map(lambda x: "'" + str(x) +"'", vals))
        self.query ={'schema':schema,'table':table,'columns':columns,'values':values}  
        
    def _DictToSelect(self, queryD):
        '''
        Converts a dictionary to Select statement 
        '''
        selectL = []
        
        for key in queryD:
            
            #statement = key operator value
            statement = ' %(col)s %(op)s \'%(val)s\'' %{'col':key.replace('#',''), 'op':queryD[key]['op'], 'val':queryD[key]['val']}
           
            selectL.append(statement)
       
        self.select_query = "WHERE %(where)s" %{'where':' AND '.join(selectL)}  
        
        return self.select_query
        
    def _SeleatAllSchema(self):
        self.cursor.execute("SELECT schema_name FROM information_schema.schemata;")
        #records = self.cursor.fetchone()
        records = self.cursor.fetchall()
        schemaL = []
        for rec in records:

            if rec[0][0:2] == 'pg' or rec[0] == 'information_schema':
                continue
            schemaL.append(rec[0])
        return schemaL
        
    def _SelectAllSchemaTables(self,schema):
        query = {'schema':schema}
        self.cursor.execute("""SELECT table_name FROM information_schema.tables WHERE table_schema = '%(schema)s'""" %query)
        return [row[0] for row in self.cursor]
        
    def _GetTableKeys(self,schema,table):
        #TGTODO duplicate in specimen
        query = "SELECT column_name FROM information_schema.table_constraints \
                JOIN information_schema.key_column_usage USING (constraint_catalog, constraint_schema, constraint_name, table_catalog, table_schema, table_name) \
                WHERE constraint_type = 'PRIMARY KEY' \
                AND (table_schema, table_name) = ('%s', '%s')" %(schema,table)

        self.cursor.execute(query)
        
        self.tabkeys = self.cursor.fetchall()

        return self.tabkeys

    def _GetTableColumns(self,schema,table):
        query = {'table':table,'schema':schema}
        self.cursor.execute("SELECT column_name, data_type, character_maximum_length, numeric_precision, column_default FROM information_schema.columns WHERE table_schema = '%(schema)s' and table_name='%(table)s';" %query)
        return self.cursor.fetchall()
    
    def _InsertQuery(self,query):
        self.cursor.execute ("INSERT INTO %(schematab)s (%(cols)s) VALUES (%(values)s);" %query)
        self.conn.commit()
        
    def _GetTableColumnsComplete(self,schema,table):
        query = {'table':table,'schema':schema}
        self.cursor.execute("SELECT * FROM information_schema.columns where table_schema = '%(schema)s' and table_name='%(table)s';" %query)
        return self.cursor.fetchall()
    
    def _SelectLayoutItem(self,query,paramL,table):
        '''
        '''
        query['table'] = table 
        
        query['cols'] = ",".join(paramL )
        
        sql = "SELECT  %(cols)s FROM layout.%(table)s \
        WHERE compid = '%(compid)s';" %query
        
        self.cursor.execute(sql)

        recs = self.cursor.fetchall()
        
        if len(recs) == 1:
            
            return recs[0]

        sql = "SELECT  %(cols)s FROM layout.%(table)s \
            WHERE compid = '%(compid)s' AND suffix = '%(suffix)s';" %query
        
        self.cursor.execute(sql)
        
        rec = self.cursor.fetchone()
        
        if rec == None:
            
            print (sql) 
                   
        return rec
        
    def IniSelectScaling(self, comp):
        '''
        '''
        compid = '%(f)s_%(b)s' %{'f':comp.content.lower(),'b':comp.layerid.lower()}
        
        query = {'compid':compid,'source':comp.source, 
                 'product':comp.product, 
                 'suffix':comp.suffix}
        paramL = ['log', 'power', 'powerna', 'mirror0', 'scalefac' ,'offsetadd', 'srcmin','srcmax','dstmin','dstmax']
        
        rec = self._SelectLayoutItem(query,paramL,'scaling')
        
        if rec == None:
            
            exitstr = 'No scaling for compid %s' %(compid)

            exit(exitstr)
            
        scalingD = dict(zip(paramL,rec))
        for item in scalingD:
            if scalingD[item] == 'N':
                scalingD[item] = False
            elif scalingD[item] == 'Y':
                scalingD[item] = True
        return scalingD
         
    def IniSelectLegendOld(self,compD):
        compid = '%(f)s_%(b)s' %{'f':compD['folder'].lower(),'b':compD['band'].lower()}
        query = {'compid':compid,'source':compD['source'], 
                 'product':compD['product'], 
                 'suffix':compD['suffix']}

        paramL = ['palmin','palmax','two51','two52','two53','two54','two55','height','width',
                  'measure','buffer','separatebuffer','frame','framecolor','label','font',
                  'fontcolor','fontsize','sticklen','compresslabels','columns','matrix',
                  'columntext','rowtext','columnhead','rowhead', 'precision']
        rec = self._SelectLayoutItem(query,paramL,'legend')
        if rec == None:
            exitstr = 'No legend for compid %s' %(self.compid)
            print (exitstr)
            ADDLEGEND
            exit(exitstr)
        legendD = dict(zip(paramL,rec))
        for item in legendD:
            if legendD[item] == 'N':
                legendD[item] = False
            elif legendD[item] == 'Y':
                legendD[item] = True 
        return legendD
    
    def IniSelectLegend(self,compD):
        '''
        '''
        print ('compD',compD)
        compid = '%(f)s_%(b)s' %{'f':compD['folder'].lower(),'b':compD['band'].lower()}
        query = {'compid':compid,'source':compD['source'], 
                 'product':compD['product'], 
                 'suffix':compD['suffix']}

        paramL = ['palmin','palmax','two51','two52','two53','two54','two55','height','width',
                  'soloheight','pngwidth','pngheight',
                  'measure','buffer','margin','textpadding','separatebuffer','framestrokewidth','framecolor',
                  'framefill','label','font','fontcolor','fontsize','fonteffect',
                  'titlefont','titlefontcolor','titlefontsize','titlefonteffect',
                  'sticklen','compresslabels','precision','columns','matrix',
                  'columntext','rowtext','columnhead','rowhead']
        rec = self._SelectLayoutItem(query,paramL,'legend')
        if rec == None:
            exitstr = 'No legend for compid %s' %(compid)
            print (exitstr)
            ADDLEGEND
            exit(exitstr)
        legendD = dict(zip(paramL,rec))
        for item in legendD:
            if legendD[item] == 'N':
                legendD[item] = False
            elif legendD[item] == 'Y':
                legendD[item] = True 
        return legendD
    
    def IniSelectLegendStruct(self,comp):
        '''
        '''
       
        #compid = '%(f)s_%(b)s' %{'f':compD['folder'].lower(),'b':compD['band'].lower()}
        query = {'compid':comp.compid,'source':comp.source, 
                 'product':comp.product, 
                 'suffix':comp.suffix}

        paramL = ['palmin','palmax','two51','two52','two53','two54','two55','height','width',
                  'soloheight','pngwidth','pngheight',
                  'measure','buffer','margin','textpadding','separatebuffer','framestrokewidth','framecolor',
                  'framefill','label','font','fontcolor','fontsize','fonteffect',
                  'titlefont','titlefontcolor','titlefontsize','titlefonteffect',
                  'sticklen','compresslabels','precision','columns','matrix',
                  'columntext','rowtext','columnhead','rowhead']
        
        rec = self._SelectLayoutItem(query,paramL,'legend')
        
        if rec == None:
            exitstr = 'No legend for compid %s' %(comp.compid)
            print (exitstr)
            ADDLEGEND
            exit(exitstr)
        legendD = dict(zip(paramL,rec))
        for item in legendD:
            if legendD[item] == 'N':
                legendD[item] = False
            elif legendD[item] == 'Y':
                legendD[item] = True 
        return legendD
    
    def _SelectCount(self,schema,table):
        schematab = '%s.%s' %(schema,table)
        query = {'schematab':schematab}
        self.cursor.execute("SELECT COUNT(*) FROM %(schematab)s;" %query)
        return self.cursor.fetchone()

        