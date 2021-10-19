'''
Created on 21 feb. 2018

@author: thomasgumbricht
'''

# Package application imports

from geoimagine.postgresdb import PGsession

class SelectUser(PGsession):
    '''
    DB support for setting up processes
    '''

    def __init__(self,pgdb):
        """ The constructor connects to the database"""
        
        HOST = 'karttur'
        
        query = self._GetCredentials( HOST )

        #Connect to the Postgres Server       
        PGsession.__init__(self,query,'SelectUser')

    def _SelectUserCreds(self, userid, pswd):
        '''
        '''
        if len(pswd) > 0:

            self.userpswd = pswd
            
            self.userid = userid
            
        else:
            HOST = userid
            secrets = netrc.netrc()
            secrets = secrets.authenticators( HOST )
            if secrets == None:
                exitstr = 'No authentication found for %(a)s' %{'a':HOST}
                exit(exitstr)
            self.userid, self.account, self.userpswd = secrets

        query = {'id': self.userid, 'pswd':self.userpswd}
        
        print ("SELECT userid,usercat,stratum FROM userlocale.users WHERE userid = '%(id)s' and userpswd = '%(pswd)s';" %query)
        
        self.cursor.execute("SELECT userid, usercat, stratum FROM userlocale.users WHERE userid = '%(id)s' and userpswd = '%(pswd)s';" %query)
        
        rec = self.cursor.fetchone()
        
        if rec == None:
        
            return rec
        
        self.cursor.execute("SELECT projid FROM userlocale.userprojects WHERE owner = '%(id)s';" %query)
        
        userProjs = self.cursor.fetchall()
        
        self.cursor.execute("SELECT tractid FROM regions.tracts WHERE owner = '%(id)s';" %query)
        
        userTracts = self.cursor.fetchall()
        
        self.cursor.execute("SELECT siteid FROM regions.sites WHERE owner = '%(id)s';" %query)
        
        userSites = self.cursor.fetchall()
        
        return (rec[0], rec[1], rec[2], userProjs, userTracts, userSites)
