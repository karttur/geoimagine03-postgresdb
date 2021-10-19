'''
Created on 29 Jun 2018

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.postgresdb import PGsession

class ManageSoilMoisture(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self):
        """ The constructor connects to the database"""
        
        HOST = 'karttur'
        
        query = self._GetCredentials( HOST )

        #Connect to the Postgres Server
        self.session = PGsession.__init__(self,query,'ManageSoilMoisture')


    def SelectFirstLastDate(self,tab,stationid):
        query = {'tab':tab, 'stn':stationid}
        self.cursor.execute("SELECT datum FROM  soilmoisture.%(tab)s WHERE stationid = '%(stn)s' ORDER BY datum" %query)
        records = self.cursor.fetchall()
        return records

    def DeleteStnDates(self,tab,stationid,firstdate,lastdate):
        query = {'tab':tab, 'stn':stationid, 'fd':firstdate, 'ld':lastdate}
        self.cursor.execute("DELETE FROM  soilmoisture.%(tab)s WHERE stationid = '%(stn)s' AND datum >= '%(fd)s' AND datum <= '%(ld)s'" %query)
        self.conn.commit()

    def CopyInsert(self,csvFPN,tab):
        '''
        '''
        copy_sql = "COPY %s.%s FROM stdin WITH CSV HEADER DELIMITER as ','" %('soilmoisture',tab)
        print (copy_sql)
        with open(csvFPN, 'r') as f:
            self.cursor.copy_expert(sql=copy_sql, file=f)
            self.conn.commit()
