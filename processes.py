'''
Created on 21 feb. 2018

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.postgresdb import PGsession

from geoimagine.support import Today

class SelectProcess(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self, db):
        """ The constructor connects to the database"""
        
        HOST = 'karttur'
        
        query = self._GetCredentials( HOST )

        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'SelectProcess')
        
    
    def _MultiSearchRecs(self, queryD, paramL, table, schema='process'):
        ''' Select multiple records
        '''
        
        return self._MultiSearch( queryD, paramL, schema, table)

    def _SelectStratum(self,query):
        self.cursor.execute("SELECT minuserstratum FROM process.subprocesses WHERE subprocid = '%(subprocid)s';" %query)
        record = self.cursor.fetchone()
        if record == None:
            print ("SELECT minuserstratum FROM process.subprocesses WHERE subprocid = '%(subprocid)s';" %query)
        return record

    def _SelectRootProcess(self,query):
        self.cursor.execute("SELECT rootprocid FROM process.subprocesses WHERE subprocid = '%(subprocid)s';" %query)
        record = self.cursor.fetchone()
        return record

    def _SelectProcessSystems(self,query):
        self.cursor.execute("SELECT system, srcsystem, dstsystem, srcdivision, dstdivision, srcepsg, dstepsg FROM process.procsys WHERE subprocid = '%(subprocid)s';" %query)
        records = self.cursor.fetchall()
        if len(records) == 0:
            print ("SELECT system, srcsystem, dstsystem, srcdivision, dstdivision, srcepsg, dstepsg  FROM process.procsys WHERE subprocid = '%(subprocid)s';" %query)

        return records

    

    def _SelectCompBands(self,subprocess,parent,tag):
            query = {'sub':subprocess, 'par':parent, 'tag':tag}
            self.cursor.execute("SELECT bandid FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s' AND element = '%(tag)s' AND paramid = 'band';" %query)
            records = self.cursor.fetchall()
            if len(records) == 0:
                print ("SELECT bandid FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s' AND element = '%(tag)s' AND paramid = 'band';" %query)
            return records

    def _SelectProcessCompTagAttr(self, subprocess, parent, tag, bandid):
        query = {'sub':subprocess, 'par':parent, 'tag':tag,'bandid':bandid}
        "SELECT tagorattr, paramid, paramtyp, required, defaultvalue FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s' AND element = '%(tag)s' AND bandid = '%(bandid)s';" %query
        self.cursor.execute("SELECT tagorattr, paramid, paramtyp, required, defaultvalue FROM process.processparams WHERE subprocid = '%(sub)s' AND parent = '%(par)s' AND element = '%(tag)s' AND bandid = '%(bandid)s';" %query)
        records = self.cursor.fetchall()
        return records



    def _SelectTractDefRegion(self,tract):
        #First check if this region is itself a defregion
        query = {'tract': tract}
        self.cursor.execute("SELECT regionid FROM regions.defregions WHERE regionid = '%(tract)s';" %query)
        rec = self.cursor.fetchone()
        if rec != None:
            return (rec[0],'T')

        self.cursor.execute("SELECT parentid FROM regions.tracts WHERE tractid = '%(tract)s';" %query)
        return (self.cursor.fetchone()[0],'D')

    def _SelectUserTract(self,userid,tract):
        query = {'userid':userid, 'tract':tract}
        self.cursor.execute("SELECT tractid FROM regions.tracts WHERE tractid = '%(tract)s' AND owner = '%(userid)s';" %query)
        return self.cursor.fetchone()

    def _SelectTractDefregid(self, userid, tract):
        rec = self._SelectUserTract(userid, tract)
        if rec == None:
            return False
        query = {'tract':tract}
        self.cursor.execute("SELECT D.regionid, D.regioncat FROM regions.tracts AS T LEFT JOIN regions.defregions AS D ON (T.parentid = D.regionid) WHERE T.tractid = '%(tract)s';" %query)
        return self.cursor.fetchone()

class ManageProcess(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self, db):
        """ The constructor connects to the database"""
        
        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,'ManageProcess')
                
        # Set the HOST name for this process
        HOST = 'karttur'
        
        # Get the credentioals for the HOST
        query = self._GetCredentials( HOST )
        
        query['db'] = db

        #Connect to the Postgres Server
        self._Connect(query)

    def _ManageRootProcess(self, process, queryD):
        ''' Insert, update or delete root process
        '''
        
        self.cursor.execute("SELECT * FROM process.rootprocesses WHERE rootprocid = '%(rootprocid)s';" %queryD)
        
        records = self.cursor.fetchone()
        
        if records == None and not process.delete:
        
            self.cursor.execute("INSERT INTO process.rootprocesses (rootprocid, title, label, creator) VALUES \
                    ('%(rootprocid)s', '%(title)s', '%(label)s', '%(creator)s');" %queryD)
            
            self.conn.commit()
        
        elif process.overwrite:
        
            self.cursor.execute("UPDATE process.rootprocesses SET (title,label) = ('%(title)s', '%(label)s') WHERE rootprocid = '%(rootprocid)s';" %queryD)
            
            self.conn.commit()

        elif process.delete:
            
            self.cursor.execute("SELECT * FROM process.subprocesses WHERE rootprocid = '%(rootprocid)s';" %queryD)
            
            records = self.cursor.fetchone()
            
            if records == None:
            
                self.cursor.execute("DELETE FROM process.rootprocesses WHERE rootprocid = '%(rootprocid)s';" %queryD)
                
                self.conn.commit()
            
            else:
            
                exitstr = 'A Root process can only be deleted if there are no subprocesses assigned to it'
                
                exit(exitstr)

    def _ManageSubProcess(self, process, queryD):
        ''' Insert, update or delete sub process
        '''
  
        def _AddSystem():
            ''' Inner function for adding systems related to the subprocess
            '''
      
            if not hasattr(process, 'system'):
                
                exitstr = 'system definition lacking for %(rootprocid)s %(subprocid)s' %queryD
                
                exit(exitstr)
                
            # Loop over the list systems on which the process can operate
            for s in process.system:
                
                # Convert the struct to a dictionary (otherwise the processid can not be added)
                keys = ["system", "srcsystem", "dstsystem", "srcdivision", "dstdivision",'srcepsg','dstepsg']
                
                values = [s.system, s.srcsystem, s.dstsystem,
                          s.srcdivision, s.dstdivision, s.srcepsg,s. dstepsg]
                
                qsD = dict(zip(keys, values))
                
                # Add the subprocid to the dict defining this system
                qsD['subprocid'] = queryD['subprocid'] 
                
                sql = "SELECT * FROM process.procsys WHERE subprocid = '%(subprocid)s' AND system = '%(system)s'\
                        AND srcsystem = '%(srcsystem)s' AND dstsystem = '%(dstsystem)s'\
                        AND srcdivision = '%(srcdivision)s' AND dstdivision = '%(dstdivision)s' ;" %qsD
                        
                # Check if this system is already associated with the sub process
                self.cursor.execute(sql)
                
                rec = self.cursor.fetchone()

                if rec == None:
                    
                    # Add this system to the sub process
                    sql = "INSERT INTO process.procsys (subprocid, system, srcsystem, dstsystem, srcdivision, dstdivision, srcepsg, dstepsg) VALUES \
                            ('%(subprocid)s', '%(system)s', '%(srcsystem)s', '%(dstsystem)s', '%(srcdivision)s', '%(dstdivision)s', %(srcepsg)d, %(dstepsg)d)" %qsD
                    
                    self.cursor.execute(sql)
                    
                    self.conn.commit()
              
        # Add todays date to queryD 
        queryD['today'] = Today()
        
        # Checking the compdef (not for all processes)
 
        if process.processid not in ['organizelandsat','explodelandsatscene','organizeancillary']:
            
            if hasattr(process, 'dstcomp'):
                
                for comp in process.dstcomp:

                    self.ManageCompDefs(process, process.parameters.version, process.proj.system, paramCDL)
        
        # Check that the rootprocess exists

        sql = "SELECT * FROM process.rootprocesses WHERE rootprocid = '%(rootprocid)s';" %queryD
     
        self.cursor.execute(sql)
        
        records = self.cursor.fetchone()
        
        if records == None:
            
            exitstr = 'The root process %(rootprocid)s is not defined, can not add sub process %(subprocid)s' %queryD
            
            exit(exitstr)
            
        # Check if this subprocess is already defined

        sql = "SELECT * FROM process.subprocesses WHERE rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND version = '%(version)s';" %queryD
        
        self.cursor.execute(sql)
        
        records = self.cursor.fetchone()
        
        # if no record exists and not delete
        if records == None and not process.delete:
                            
            # Check that the name of the subprocess does not already exist  
            
            sql =  "SELECT rootprocid FROM process.subprocesses WHERE subprocid = '%(subprocid)s' AND version = '%(version)s';" %queryD
            
            self.cursor.execute( sql )
                
            record = self.cursor.fetchone()
            
            if record:
                
                print (sql)
                
                exitstr = 'EXITING - the subprocess %s is already defined, but under root %s (%s)' %(process.paramsD['subprocid'],record[0],process.paramsD['rootprocid'])
                
                exit(exitstr)
               
            # Insert the subprocess container 
            sql = "INSERT INTO process.subprocesses (rootprocid, subprocid, version, minuserstratum, title, label, creator, createdate) VALUES \
                    ('%(rootprocid)s', '%(subprocid)s', '%(version)s', '%(minuserstratum)s', '%(title)s', '%(label)s', '%(creator)s', '%(today)s')" %queryD               
            
            self.cursor.execute(sql)
            
            # Add systems related to the subprocess 
            _AddSystem()
            
        elif process.overwrite:
            
            # Update the process metadata
            sql =  "UPDATE process.subprocesses SET (minuserstratum,title,label) = (%(minuserstratum)s, '%(title)s', '%(label)s') WHERE \
                rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND version = '%(version)s';"  %queryD
            
            self.cursor.execute(sql)
            
            self.conn.commit()
            
            # Recreate the system association
            
            sql = "DELETE FROM process.procsys WHERE subprocid = '%(subprocid)s';" %queryD
            
            self.cursor.execute(sql)
            
            self.conn.commit()
            
            # Add systems related to the subprocess  
            _AddSystem()
            
        elif process.delete:
            
            sql = "DELETE FROM process.subprocesses WHERE subprocid = '%(subprocid)s' AND version = '%(version)s'" %queryD
            
            self.cursor.execute(sql)

            self.conn.commit()
            
        # Start processing the actual parameters and metadata of the subprocesses
        if process.delete or process.overwrite:
            
            # Delete all the processparams
            
            sql = "DELETE FROM process.processparams WHERE subprocid = '%(subprocid)s' AND version = '%(version)s';" %queryD
            
            self.cursor.execute(sql)
            
            self.conn.commit()
            
            # Delete all the processparamSetValues
            
            sql = "DELETE FROM process.processparamSetValues WHERE subprocid = '%(subprocid)s' AND version = '%(version)s';" %queryD
            
            self.cursor.execute(sql)
            
            self.conn.commit()
            
            # Delete all the processparamSetMinMax
            
            sql = "DELETE FROM process.processparamSetMinMax WHERE subprocid = '%(subprocid)s' AND version = '%(version)s';" %queryD
            
            self.cursor.execute(sql)
            
            self.conn.commit()
            
            if process.delete:
                
                #return - all deleted nothing to add
                return
            
        try:
            
            for node in process.nodes:
                
                pass
                
        except:
            
            exitstr = 'EXITING - process.nodes Error for  %(rootprocid)s %(subprocid)s\n     Most likely "node" instead of "nodes" or  "[]" missing ' %queryD

            exit (exitstr)
                    
        # Loop over all the parameter elements
        for node in process.nodes:
            
            bandid = False
            
            if node.parent in ['dstcomp','srccomp']:
                
                # This node defines a band, check that the band has a valid default
                
                if not hasattr(node, 'parameter'):
                
                    if hasattr(node, 'params'):
                        
                        exitstr = 'EXITING - rename params[] to parameter[] in  %(rootprocid)s %(subprocid)s ' %queryD
                    
                    else:
                        
                        exitstr = 'EXITING - parameter[] missing for  %(rootprocid)s %(subprocid)s ' %queryD
                
                    exit(exitstr)
                    
                try:
                    for param in node.parameter:
                        
                        pass
                        
                except:
                    
                    exitstr = 'EXITING - parameter Error for  %(rootprocid)s %(subprocid)s ' %queryD
        
                    print (exitstr)
                                                    
                for param in node.parameter:
                    
                    if param.paramid == 'layerid' and param.defaultvalue != '':
                    
                        bandid = param.defaultvalue
                
                if not bandid:
                    
                    exitstr = 'All compositions must have a layerid/bandid (the defaultvalue must be set) %(rootprocid)s %(subprocid)s ' %queryD
                    
                    exit(exitstr)
            
            if not hasattr(node, 'parameter'):
                
                if hasattr(node, 'params'):
                    
                    exitstr = 'EXITING - rename params[] to parameter[] in  %(rootprocid)s %(subprocid)s ' %queryD
                
                else:
                    
                    exitstr = 'EXITING - parameter[] missing for  %(rootprocid)s %(subprocid)s ' %queryD
            
                exit(exitstr)
                
            try:
                
                for param in node.parameter:
                                        
                    pass
                    
            except:
                
                exitstr = 'Error in "nodes - parameter" at node.parent/element: %s / %s, or no list (array) set ' %(node.parent, node.element)
                
                exitstr += '\n    for  %(rootprocid)s %(subprocid)s ' %queryD
    
                exit (exitstr)
                                
            for param in node.parameter: 
                                        
                if hasattr(param, 'setvalue'): 
        
                    setValueL = param.setvalue
                                            
                else:
                        
                    setValueL = []
                        
                if hasattr(param, 'minmax'):
                    
                    minMaxD = {'min':param.minmax.min,'max':param.minmax.max}
                    
                else:
                    
                    minMaxD = {}
                   
                if not hasattr(node, 'element'):
                    
                    exitstr = 'EXITING - element missing for  %(rootprocid)s %(subprocid)s ' %queryD
                    
                    print ('param',param)
                    print (exitstr)
                    SNULLE
                    exit(exitstr)
                                                      
                # Convert the parameters struct to a dictionary while adding key:value pairs                
                keys = ['rootprocid','subprocid','version',
                        'parent','element','paramid',
                        'paramtyp','required','defaultvalue', 
                        'bandid', 'hint']
    
                if hasattr(param, 'hint'):
                    
                    values = [queryD['rootprocid'], queryD['subprocid'], queryD['version'],
                          node.parent, node.element, param.paramid,
                          param.paramtyp, param.required, param.defaultvalue,
                        bandid,param.hint]
                
                else:
                    
                    values = [queryD['rootprocid'], queryD['subprocid'], queryD['version'],
                          node.parent, node.element, param.paramid,
                          param.paramtyp, param.required, param.defaultvalue,
                          bandid,'to be completed']
                
                qpD = dict(zip(keys, values))
                                
                if node.parent in ['srccomp','dstcomp'] and bandid:
    
                    sql = "SELECT * FROM process.processparams WHERE rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND paramid = '%(paramid)s' AND version = '%(version)s' AND parent = '%(parent)s' AND element = '%(element)s' AND defaultvalue = '%(defaultvalue)s' AND bandid = '%(bandid)s';" %qpD
                    
                else:
    
                    sql = "SELECT * FROM process.processparams WHERE rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND paramid = '%(paramid)s' AND version = '%(version)s' AND parent = '%(parent)s' AND element = '%(element)s';" %qpD
                
                self.cursor.execute(sql)
                
                records = self.cursor.fetchone()
    
                if records == None:
                                    
                    try:  
                        sql = "INSERT INTO process.processparams (rootprocid, subprocid, version, parent, element, paramid, paramtyp, required, defaultvalue, hint, bandid) VALUES \
                            ('%(rootprocid)s','%(subprocid)s','%(version)s','%(parent)s','%(element)s','%(paramid)s','%(paramtyp)s','%(required)s','%(defaultvalue)s','%(hint)s','%(bandid)s');" %qpD
                            
                        self.cursor.execute(sql)
                    
                        self.conn.commit()
                        
                    except:
                        
                        exitstr = 'EXITING, failed sql\n%s' %(sql)
                        
                        exit ( exitstr )
                    
                try:
                    
                    for setValue in setValueL:
                        
                        pass
                        
                except:
                    exitstr = 'EXITING - setValue Error for  %(rootprocid)s %(subprocid)s %(paramid)s' %qpD

                    print (exitstr)
                    
                    SNULLE
                    
                # Insert predefined value alternatives
                for setValue in setValueL:
                                            
                    qpD['value'] = setValue.value
                    
                    qpD['valuelabel'] = setValue.label
                    
                    sql = "SELECT * FROM process.processparamSetValues WHERE \
                        rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND paramid = '%(paramid)s' AND \
                        parent = '%(parent)s'  AND element = '%(element)s' AND value = '%(value)s' AND version = '%(version)s';" %qpD
                    
                    self.cursor.execute(sql)
                    
                    records = self.cursor.fetchone()
                    
                    if records == None:
                                              
                        self.cursor.execute("INSERT INTO process.processparamSetValues (rootprocid, subprocid, version, paramid, parent, element, value, label) VALUES \
                        ('%(rootprocid)s','%(subprocid)s','%(version)s','%(paramid)s','%(parent)s','%(element)s', '%(value)s', '%(valuelabel)s');" %qpD)
                        self.conn.commit()
                
                # Insert predefined min/max levels
                if minMaxD:
                    
                    self.cursor.execute("SELECT * FROM process.processparamSetMinMax WHERE \
                            rootprocid = '%(rootprocid)s' AND subprocid = '%(subprocid)s' AND paramid = '%(paramid)s' AND \
                            parent = '%(parent)s'  AND element = '%(element)s' AND version = '%(version)s';" %qpD)
                    
                    records = self.cursor.fetchone()
                    
                    if records == None:
                        
                        # Add the min and max levels to the query
                        qpD['min'] =  minMaxD['min'];  qpD['max'] =  minMaxD['max']
                        
                        self.cursor.execute("INSERT INTO process.processparamSetMinMax (rootprocid, subprocid, version, paramid, parent, element, minval, maxval) VALUES \
                        ('%(rootprocid)s','%(subprocid)s','%(version)s','%(paramid)s','%(parent)s','%(element)s',%(min)s,%(max)s);" %qpD)
                        
                        self.conn.commit()

    def _SelectRootProcess(self,queryD):
        '''
        '''
        querystem = 'SELECT rootprocid, title, label FROM process.rootprocesses'
        selectQuery = {}
        for item in queryD:
            selectQuery[item] = {'col': item, 'op':'=', 'val': queryD[item]}
        if selectQuery:
            wherestatement = self.session._DictToSelect(selectQuery)
        else:
            wherestatement = ''

        querystem = '%s %s;' %(querystem, wherestatement)

        self.cursor.execute(querystem)
        records = self.cursor.fetchall()
        return records

    def _SelectSubProcess(self,queryD):
        '''
        '''
        querystem = 'SELECT rootprocid, subprocid, title, label FROM process.subprocesses'
        selectQuery = {}
        for item in queryD:
            selectQuery[item] = {'col': item, 'op':'=', 'val': queryD[item]}
        if selectQuery:
            wherestatement = self._DictToSelect(selectQuery)
        else:
            wherestatement = ''


        querystem = '%s %s;' %(querystem, wherestatement)

        self.cursor.execute(querystem)
        records = self.cursor.fetchall()
        return records

    def _SelectProcessParams(self,queryD):
        '''
        '''
        querystem = 'SELECT rootprocid, subprocid, version, parent, element, paramid, paramtyp, tagorattr, required, defaultvalue, bandid FROM process.processparams'
        selectQuery = {}
        for item in queryD:
            selectQuery[item] = {'col': item, 'op':'=', 'val': queryD[item]}
        if selectQuery:
            wherestatement = self._DictToSelect(selectQuery)
        else:
            wherestatement = ''

        querystem = '%s %s;' %(querystem, wherestatement)

        self.cursor.execute(querystem)
        records = self.cursor.fetchall()
        return records
