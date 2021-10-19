'''
Created on 21 Oct 2018

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.postgresdb import PGsession

class ManageExport(PGsession):
    '''
    DB support for managing regions
    '''
    
    def __init__(self):
        """ The constructor connects to the database"""
        
        HOST = 'karttur'
        
        query = self._GetCredentials( HOST )

        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'ManageExport')
        
    def _SelectDefaultRegion(self,defregid):
        '''
        '''
        
        query = {'regionid':defregid}
        
        self.cursor.execute("SELECT regioncat FROM system.defregions WHERE regionid = '%(regionid)s';" %query)
        
        rec = self.cursor.fetchone()
        
        return rec

    def _SelectComp(self, compQ):
        '''
        '''
        
        querystem = 'SELECT C.source, C.product, B.folder, B.band, B.prefix, C.suffix, C.masked, C.cellnull, C.celltype, B.measure, B.scalefac, B.offsetadd, B.dataunit '
        
        query ='FROM %(system)s.compdefs AS B ' %compQ
        
        querystem = '%s %s ' %(querystem, query)
        
        query ='INNER JOIN %(system)s.compprod AS C ON (B.compid = C.compid)' %compQ
        
        querystem = '%s %s ' %(querystem, query)
                
        querypart = "WHERE B.folder = '%(folder)s' AND B.band = '%(band)s'" %compQ
        
        querystem = '%s %s' %(querystem, querypart)
        
        self.cursor.execute(querystem)
        
        records = self.cursor.fetchall()
        
        params = ['source', 'product', 'folder', 'band', 'prefix', 'suffix', 'masked', 'cellnull', 'celltype', 'measure', 'scalefac', 'offsetadd', 'dataunit']

        if len(records) == 1:
        
            return dict(zip(params,records[0]))
        
        elif len(records) > 1:
        
            querypart = "AND C.product = '%(product)s'" %compQ
            
            querystem = '%s %s' %(querystem, querypart)
            
            self.cursor.execute(querystem)
            
            records = self.cursor.fetchall()
            
            if len(records) == 1:
            
                return dict(zip(params,records[0]))
            
            else:
            
                querypart = "AND C.suffix = '%(suffix)s'" %compQ
                
                querystem = '%s %s' %(querystem, querypart)
                
                self.cursor.execute(querystem)
                
                records = self.cursor.fetchall()
                
                if len(records) == 1:
                
                    return dict(zip(params,records[0]))
                
                else:
                    print ('querystem',querystem)
                    print ('records',records)
                    ERRORINEXPORT

        else:
            print ('querystem',querystem)
            print ('records',records)
            ERRORINEXPORT

    def _SelectScalingOld(self,comp):
        '''
        '''
        
        scalingD = self.session.IniSelectScaling(self.process.proc.comp.paramsD[comp])
        
        self.scaling = lambda: None
        
        for key, value in scalingD.items():
        
            setattr(self.scaling, key, value)

    def _SelectMovieClock(self,query,paramL):
        '''
        '''
        
        query['cols'] = ",".join(paramL)
        
        self.cursor.execute("SELECT  %(cols)s FROM layout.movieclock \
                WHERE name = '%(name)s';" %query)
        
        rec = self.cursor.fetchone()
        
        if rec == None:
        
            print ("SELECT  %(cols)s FROM layout.movieclock \
                    WHERE name = '%(name)s';" %query)
            
            exit('No record for movieclock')
                    
        return dict(zip(paramL,rec))

    def _SelectLegend(self,comp):
        '''
        '''
        
        legendD = self.session.IniSelectLegend(self.process.proc.comp.paramsD[comp])
        
        self.legend = lambda: None
        
        for key, value in legendD.items():
            setattr(self.legend, key, value)


    def _SelectModisRegionTiles(self,query):
        '''DUPLICATE FROM modis
        '''
        paramL = ['htile','vtile']
        queryD = {'regionid':query['siteid'], 'regiontype':'site'}

        tiles = self._MultiSearch(queryD, paramL, 'modis', 'regions')
        if len(tiles) > 0:
            return tiles

        queryD = {'regionid':query['tractid'], 'regiontype':'tract'}

        tiles = self._MultiSearch(queryD, paramL, 'modis', 'regions')
        if len(tiles) > 0:
            return tiles

        queryD = {'regionid':query['regionid'], 'regiontype':'default'}

        tiles = self._MultiSearch(queryD, paramL, 'modis', 'regions')
        if len(tiles) > 0:
            return tiles

    def _SelectRegionLonLatExtent(self, regionid, regiontype):
        '''
        '''
        
        query = {'id':regionid}
        
        if regiontype == 'T':
        
            self.cursor.execute("SELECT ullon, lllon, ullat, urlat, urlon, lrlon, lllat, lrlat FROM regions.regions WHERE regionid = '%(id)s';" %query)
        
        elif regiontype == 'D':
            
            self.cursor.execute("SELECT ullon, lllon, ullat, urlat, urlon, lrlon, lllat, lrlat FROM system.regions WHERE regionid = '%(id)s';" %query)
        
        rec = self.cursor.fetchone()
        
        if rec == None:
        
            print ("SELECT ullon, lllon, ullat, urlat, urlon, lrlon, lllat, lrlat FROM regions.regions WHERE regionid = '%(id)s';" %query)
        
        return rec
