import distutils.version
from include import HydrusConstants as HC
from include import HydrusData
from include import HydrusExceptions
from include import HydrusGlobals as HG
from include import HydrusPaths
from include import HydrusText
import os
import queue
import sqlite3
import traceback
import time
import include.HydrusDB as dbsynchronous
import asyncio
CONNECTION_REFRESH_TIME = 60 * 30

def CanVacuum( db_path, stop_time = None ):
    return dbsynchronous.CanVacuum(db_path,stop_time)
    
async def ReadLargeIdQueryInSeparateChunks( cursor, select_statement, chunk_size, on_complete=None ):
    r= dbsynchronous.ReadLargeIdQueryInSeparateChunks(cursor,select_statement,chunk_size)
    if on_complete is not None:
        on_complete(r)
    else:
        return r
    
async def VacuumDB( db_path ,on_complete=None):
    r = dbsynchronous.VacuumDB(db_path)
    if on_complete is not None:
        on_complete(r)
    else:
        return r
        
    
class HydrusDB( object ):
    
    READ_WRITE_ACTIONS = []
    UPDATE_WAIT = 2
    
    TRANSACTION_COMMIT_TIME = 10
    

    def __init__( self, controller, db ):
      '''Use an existing database controler as the backend
      You should do this epecially if you already have a connection
      as you are porting the old synchronous code'''
      assert(isinstance(db,dbsynchronous.HydrusDB))
      self._db=db
                      
    def _DoCallback(self,on_complete,result):
        if on_complete is not None:
            on_complete(result)
        else:
            return result

    
    async def _AttachExternalDatabases( self ):
        return self._db._AttachExternalDatabases()
        
    
    async def _BeginImmediate( self ):
        return self._db._BeginImmediate()
            
        
    
    async def _CleanUpCaches( self ):
        return self._db._BeginImmediate()
        
    
    async def _CloseDBCursor( self ):
        return self._db._CloseDBCursor()
            

    async def _Commit( self, on_complete=None ):
      r = self._db._Commit()
      self._DoCallback(on_complete,r)

        
    
    async def _CreateDB( self, on_complete=None ):
      r = self._db._CreateDB
      self._DoCallback(on_complete,r)
    
    async def _CreateIndex( self, table_name, columns, unique = False, on_complete=None ):
      r = self._db._CreateIndex(table_name,columns,unique)
      self._DoCallback(on_complete,r)
        
    
    async def _DisplayCatastrophicError( self, text, on_complete=None ):
      r = self._db._DisplayCatastrophicError(text)
      self._DoCallback(on_complete,r)
        
    
    async def _GetRowCount( self, on_complete=None ):
      r = self._db._GetRowCount()
      self._DoCallback(on_complete,r)
        
    
    async def _InitCaches( self, on_complete=None ):
      r = self._db._InitCaches()
      self._DoCallback(on_complete,r)
        
    
    async def _InitDB( self , on_complete=None):
      r = self._db._InitDB()
      self._DoCallback(on_complete,r)
            
        
    
    async def _InitDBCursor( self, on_complete=None ):
      self._db._InitDBCursor()
      self._DoCallback(on_complete,r)
            
        
    
    async def _InitDiskCache( self, on_complete=None ):
      self._db._InitDiskCache()
      self._DoCallback(on_complete,r)
        
    
    async def _InitExternalDatabases( self, on_complete=None ):
      self._db._InitExternalDatabases()
      self._DoCallback(on_complete,r)
        
    
    async def _ManageDBError( self, job, e, on_complete=None ):
      r = self._db._ManageDBError(job,e)
      self._DoCallback(on_complete,r)
        
    
    async def _ProcessJob( self, job, on_complete=None ):
      self._db._ProcessJob(job)
      self._DoCallback(on_complete,r)
            
        
    
    async def _Read( self, action, on_complete=None, *args, **kwargs ):
      r = self._db._Read(action,args,kwargs)
      self._DoCallback(on_complete,r)
        
    
    async def _RepairDB( self, on_complete=None ):
      r = self._db._RepairDB
      self._DoCallback(on_complete,r)
        
    
    async def _ReportOverupdatedDB( self, version, on_complete=None ):
      r = self._db._ReportOverupdatedDB(version)
      self._DoCallback(on_complete,r)
        
    
    async def _ReportUnderupdatedDB( self, version, on_complete=None ):
      r = self._db._ReportUnderupdatedDB(version)
      self._DoCallback(on_complete,r)
        
    
    async def _ReportStatus( self, text, on_complete=None ):
      r = self._db._ReportStatus(text)
      self._DoCallback(on_complete,r)
        
    
    async def _Rollback( self, on_complete=None ):
      r = self._db._Rollback()
      self._DoCallback(on_complete,r)         
        
    
    async def _Save( self, on_complete=None ):
      r = self._db._Save()
      self._DoCallback(on_complete,r)
        
    
    async def _SelectFromList( self, select_statement, xs, on_complete=None ):
      r = self._db._SelectFromList(select_statement,xs)
      self._DoCallback(on_complete,r)
                
            
        
    
    async def _SelectFromListFetchAll( self, select_statement, xs, on_complete=None ):
      r = self._db._SelectFromList(select_statement,xs)
      self._DoCallback(on_complete,r)
        
    
    async def _ShrinkMemory( self, on_complete=None ):
      r = self._db._ShrinkMemory()
      self._DoCallback(on_complete,r)
    
    async def _STI( self, iterable_cursor, on_complete=None ):
      r = self._db._STI(iterable_cursor)
      self._DoCallback(on_complete,r)
        
    
    async def _STL( self, iterable_cursor, on_complete=None ):
      r = self._db._STL(iterable_cursor)
      self._DoCallback(on_complete,r)
        
    
    async def _STS( self, iterable_cursor, on_complete=None ):
      r = self._db._STS(iterable_cursor)
      self._DoCallback(on_complete,r)
        
    
    async def _UpdateDB( self, version, on_complete=None ):
      r = self._db._UpdateDB(version)
      self._DoCallback(on_complete,r)
        
    
    async def _Write( self, action, on_complete=None, *args, **kwargs ):
      r = self._db._Write(action,args,kwargs)
      self._DoCallback(on_complete,r)
        
    
    async def pub_after_job( self, topic, on_complete=None, *args, **kwargs ):
      r = self._db.pub_after_job(topic,args,kwargs)
      self._DoCallback(on_complete,r)       
    
    async def publish_status_update( self, on_complete=None ):
      r = self._db.publish_status_update()
      self._DoCallback(on_complete,r)     
    
    async def CurrentlyDoingJob( self, on_complete=None ):
        ##TODO is this sensible with async mode?
      r = self._db.CurrentlyDoingJob()
      self._DoCallback(on_complete,r)
        
    
    async def GetApproxTotalFileSize( self , on_complete=None):
      r = self._db.GetApproxTotalFileSize()
      self._DoCallback(on_complete,r)
        
    
    def GetStatus( self ):
        return self._db.GetStatus()
        
    
    def IsDBUpdated( self ):
        return self._db.IsDBUpdated()
        
    
    def IsFirstStart( self ):
        return self._db.IsFirstStart()
        
    
    def LoopIsFinished( self ):
        return self._loop.isclosed()
        
    
    def JobsQueueEmpty( self ):
        return self._db.JobsQueueEmpty()
        
    
    def MainLoop( self ):
        
        try:
            self._loop.run_until_complete(self._db.MainLoop())
        finally:
            self._loop.close()
        
    
    async def Read( self, action, on_complete=None, *args, **kwargs ):
        return self._db.Read(action,args,kwargs)
        
    
    async def ReadyToServeRequests( self, on_complete=None ):
        return self._db.ReadyToServeRequests()
        
    
    async def Shutdown( self, on_complete=None ):
        return self._db.Shutdown()
        
    
    async def Write( self, action, synchronous, on_complete=None, *args, **kwargs ):
        return self._db.Write(action,synchronous,args,kwargs)
        
    
class TemporaryIntegerTable( object ):
    
    def __init__( self, cursor, integer_iterable, column_name ):
        
        self._cursor = cursor
        self._integer_iterable = integer_iterable
        self._column_name = column_name
        
        self._table_name = 'mem.tempint' + os.urandom( 32 ).hex()
        
    
    def __enter__( self ):
        
        self._cursor.execute( 'CREATE TABLE ' + self._table_name + ' ( ' + self._column_name + ' INTEGER PRIMARY KEY );' )
        
        self._cursor.executemany( 'INSERT INTO ' + self._table_name + ' ( ' + self._column_name + ' ) VALUES ( ? );', ( ( i, ) for i in self._integer_iterable ) )
        
        return self._table_name
        
    
    def __exit__( self, exc_type, exc_val, exc_tb ):
        
        self._cursor.execute( 'DROP TABLE ' + self._table_name + ';' )
        
        return False
        
    
