import collections
import gc
import os
import random
import sys
import threading
import time

from hydrus.core import HydrusConstants as HC
from hydrus.core import HydrusData
from hydrus.core import HydrusExceptions
from hydrus.core import HydrusGlobals as HG
from hydrus.core import HydrusNATPunch
from hydrus.core import HydrusPaths
from hydrus.core import HydrusPubSub
from hydrus.core import HydrusThreading

class HydrusController( object ):
    '''The abstract base class of the Application controllers for both the client and server applications.'''
    
    def __init__( self, db_dir ):
        
        HG.controller = self
        
        self._name = 'hydrus'
        
        self._last_shutdown_was_bad = False
        self._i_own_running_file = False
        
        self.db_dir = db_dir
        
        self.db = None
        
        pubsub_valid_callable = self._GetPubsubValidCallable()
        
        self._pubsub = HydrusPubSub.HydrusPubSub( self, pubsub_valid_callable )
        self._daemons = []
        self._daemon_jobs = {}
        self._caches = {}
        self._managers = {}
        
        self._fast_job_scheduler = None
        self._slow_job_scheduler = None
        
        self._thread_slots = {}
        
        self._thread_slots[ 'misc' ] = ( 0, 10 )
        
        self._thread_slot_lock = threading.Lock()
        
        self._call_to_threads = []
        self._long_running_call_to_threads = []
        
        self._thread_pool_busy_status_text = ''
        self._thread_pool_busy_status_text_new_check_time = 0
        
        self._call_to_thread_lock = threading.Lock()
        
        self._timestamps = collections.defaultdict( lambda: 0 )
        
        self._timestamps[ 'boot' ] = HydrusData.GetNow()
        
        self._timestamps[ 'last_sleep_check' ] = HydrusData.GetNow()
        
        self._sleep_lock = threading.Lock()
        
        self._just_woke_from_sleep = False
        
        self._system_busy = False
        
        self._doing_fast_exit = False
        
    
    def _GetCallToThread( self ):
        '''
        If there is a thread in the thread pool that isn't busy return it, return any thread at random.
        '''
        
        with self._call_to_thread_lock:
            
            for call_to_thread in self._call_to_threads:
                
                if not call_to_thread.CurrentlyWorking():
                    
                    return call_to_thread
                    
                
            
            # all the threads in the pool are currently busy
            
            calling_from_the_thread_pool = threading.current_thread() in self._call_to_threads
            
            if calling_from_the_thread_pool or len( self._call_to_threads ) < 200:
                
                call_to_thread = HydrusThreading.THREADCallToThread( self, 'CallToThread' )
                
                self._call_to_threads.append( call_to_thread )
                
                call_to_thread.start()
                
            else:
                
                call_to_thread = random.choice( self._call_to_threads )
                
            
            return call_to_thread
            
        
    
    def _GetCallToThreadLongRunning( self ):
        '''
        Get a worker thread from the long running pool.  
        Unlike _GetCallToThread a different pool is used so that long running jobs do not get interleaved into between short jobs.
        In theory this means that if a short job does not depend on a long job it will be able to proceed without waiting on the long job.
        In a practical sense when Hydrus is under load many jobs have to wait on IO anyway.
        '''
        
        with self._call_to_thread_lock:
            
            for call_to_thread in self._long_running_call_to_threads:
                
                if not call_to_thread.CurrentlyWorking():
                    
                    return call_to_thread
                    
                
            
            call_to_thread = HydrusThreading.THREADCallToThread( self, 'CallToThreadLongRunning' )
            
            self._long_running_call_to_threads.append( call_to_thread )
            
            call_to_thread.start()
            
            return call_to_thread
            
        
    
    def _GetPubsubValidCallable( self ):
        
        return lambda o: True
        
    
    def _GetAppropriateJobScheduler( self, time_delta ):
        
        if time_delta <= 1.0:
            
            return self._fast_job_scheduler
            
        else:
            
            return self._slow_job_scheduler
            
        
    
    def _GetUPnPServices( self ):
        
        return []
        
    
    def _GetWakeDelayPeriod( self ):
        
        return 15
        
    
    def _InitDB( self ):
        
        raise NotImplementedError()
        
    
    def _InitTempDir( self ):
        
        self.temp_dir = HydrusPaths.GetTempDir()
        
    
    def _MaintainCallToThreads( self ):
        
        # we don't really want to hang on to threads that are done as event.wait() has a bit of idle cpu
        # so, any that are in the pools that aren't doing anything can be killed and sent to garbage
        
        with self._call_to_thread_lock:
            
            def filter_call_to_threads( t ):
                
                if t.CurrentlyWorking():
                    
                    return True
                    
                else:
                    
                    t.shutdown()
                    
                    return False
                    
                
            
            self._call_to_threads = list(filter( filter_call_to_threads, self._call_to_threads ))
            
            self._long_running_call_to_threads = list(filter( filter_call_to_threads, self._long_running_call_to_threads ))
            
        
    
    def _PublishShutdownSubtext( self, text ):
        
        pass
        
    
    def _Read( self, action, *args, **kwargs ):
        
        result = self.db.Read( action, *args, **kwargs )
        
        return result
        
    
    def _ReportShutdownDaemonsStatus( self ):
        
        pass
        
    
    def _ShowJustWokeToUser( self ):
        
        HydrusData.Print( 'Just woke from sleep.' )
        
    
    def _ShutdownDaemons( self ):
        
        for job in self._daemon_jobs.values():
            
            job.Cancel()
            
        
        self._daemon_jobs = {}
        
        for daemon in self._daemons:
            
            daemon.shutdown()
            
        
        started = HydrusData.GetNow()
        
        while True in ( daemon.is_alive() for daemon in self._daemons ):
            
            self._ReportShutdownDaemonsStatus()
            
            time.sleep( 0.1 )
            
            if HydrusData.TimeHasPassed( started + 30 ):
                
                break
                
            
        
        self._daemons = []
        
    
    def _Write( self, action, synchronous, *args, **kwargs ):
        
        result = self.db.Write( action, synchronous, *args, **kwargs )
        
        return result
        
    
    def pub( self, topic, *args, **kwargs ):
        
        if HG.model_shutdown:
            
            self._pubsub.pubimmediate( topic, *args, **kwargs )
            
        else:
            
            self._pubsub.pub( topic, *args, **kwargs )
            
        
    
    def pubimmediate( self, topic, *args, **kwargs ):
        
        self._pubsub.pubimmediate( topic, *args, **kwargs )
        
    
    def sub( self, object, method_name, topic ):
        
        self._pubsub.sub( object, method_name, topic )
        
    
    def AcquireThreadSlot( self, thread_type ):
        '''
        Prevents more than a fixed number of a certain job type from executing at once.
        If the controller has no limit on the allowed number of jobs for the named "thread type" always allowed.

        Warning:
            Remember to ReleaseThreadSlot() when finished.

        :returns: True if a slot was successfully aquired, Falseo otherwise.
        
        '''
        
        with self._thread_slot_lock:
            
            if thread_type not in self._thread_slots:
                
                return True # assume no max if no max set
                
            
            ( current_threads, max_threads ) = self._thread_slots[ thread_type ]
            
            if current_threads < max_threads:
                
                self._thread_slots[ thread_type ] = ( current_threads + 1, max_threads )
                
                return True
                
            else:
                
                return False
                
            
        
    
    def CallLater( self, initial_delay, func, *args, **kwargs ):
        
        job_scheduler = self._GetAppropriateJobScheduler( initial_delay )
        
        call = HydrusData.Call( func, *args, **kwargs )
        
        job = HydrusThreading.SingleJob( self, job_scheduler, initial_delay, call )
        
        job_scheduler.AddJob( job )
        
        return job
        
    
    def CallRepeating( self, initial_delay, period, func, *args, **kwargs ) -> HydrusThreading.RepeatingJob:
        
        job_scheduler = self._GetAppropriateJobScheduler( period )
        
        call = HydrusData.Call( func, *args, **kwargs )
        
        job = HydrusThreading.RepeatingJob( self, job_scheduler, initial_delay, period, call )
        
        job_scheduler.AddJob( job )
        
        return job
        
    
    def CallToThread( self, callable, *args, **kwargs ):
        '''
        Invokes the job on an available worker if one is not busy, otherwise it is added to the work queue of a random
        worker.
        '''
        
        if HG.callto_report_mode:
            
            what_to_report = [ callable ]
            
            if len( args ) > 0:
                
                what_to_report.append( args )
                
            
            if len( kwargs ) > 0:
                
                what_to_report.append( kwargs )
                
            
            HydrusData.ShowText( tuple( what_to_report ) )
            
        
        call_to_thread = self._GetCallToThread()
        
        call_to_thread.put( callable, *args, **kwargs )
        
    
    def CallToThreadLongRunning( self, callable, *args, **kwargs ):
        
        if HG.callto_report_mode:
            
            what_to_report = [ callable ]
            
            if len( args ) > 0:
                
                what_to_report.append( args )
                
            
            if len( kwargs ) > 0:
                
                what_to_report.append( kwargs )
                
            
            HydrusData.ShowText( tuple( what_to_report ) )
            
        
        call_to_thread = self._GetCallToThreadLongRunning()
        
        call_to_thread.put( callable, *args, **kwargs )
        
    
    def CleanRunningFile( self ):
        '''The last step after everything hash shutdown.  Removing the running file indicates clean shutdown.
        If the file exists before it was created, the last run of hydrus had unclean shutdown.
        '''
        
        if self._i_own_running_file:
            
            HydrusData.CleanRunningFile( self.db_dir, self._name )
            
        
    
    def ClearCaches( self ):
        
        for cache in list(self._caches.values()): cache.Clear()
        
    
    def CurrentlyIdle( self ):
        
        return True
        
    
    def CurrentlyPubSubbing( self ):
        
        return self._pubsub.WorkToDo() or self._pubsub.DoingWork()
        
    
    def DBCurrentlyDoingJob( self ):
        
        if self.db is None:
            
            return False
            
        else:
            
            return self.db.CurrentlyDoingJob()
            
        
    
    def DebugShowScheduledJobs( self ):
        
        summary = self._fast_job_scheduler.GetPrettyJobSummary()
        
        HydrusData.ShowText( 'fast scheduler:' )
        HydrusData.ShowText( summary )
        
        summary = self._slow_job_scheduler.GetPrettyJobSummary()
        
        HydrusData.ShowText( 'slow scheduler:' )
        HydrusData.ShowText( summary )
        
    
    def DoingFastExit( self ) -> bool:
        
        return self._doing_fast_exit
        
    
    def GetBootTime( self ):
        
        return self._timestamps[ 'boot' ]
        
    
    def GetDBDir( self ) -> str:
        
        return self.db_dir
        
    
    def GetDBStatus( self ):
        
        return self.db.GetStatus()
        
    
    def GetCache( self, name ):
        
        return self._caches[ name ]
        
    
    def GetManager( self, name ):
        
        return self._managers[ name ]
        
    
    def GetThreadPoolBusyStatus( self ):
        
        if HydrusData.TimeHasPassed( self._thread_pool_busy_status_text_new_check_time ):
            
            with self._call_to_thread_lock:
                
                num_threads = sum( ( 1 for t in self._call_to_threads if t.CurrentlyWorking() ) )
                
            
            if num_threads < 4:
                
                self._thread_pool_busy_status_text = ''
                
            elif num_threads < 10:
                
                self._thread_pool_busy_status_text = 'working'
                
            elif num_threads < 20:
                
                self._thread_pool_busy_status_text = 'busy'
                
            else:
                
                self._thread_pool_busy_status_text = 'very busy!'
                
            
            self._thread_pool_busy_status_text_new_check_time = HydrusData.GetNow() + 10
            
        
        return self._thread_pool_busy_status_text
        
    
    def GetThreadsSnapshot( self ):
        
        threads = []
        
        threads.extend( self._daemons )
        threads.extend( self._call_to_threads )
        threads.extend( self._long_running_call_to_threads )
        
        threads.append( self._slow_job_scheduler )
        threads.append( self._fast_job_scheduler )
        
        return threads
        
    
    def GoodTimeToStartBackgroundWork( self ) -> bool:
        
        return self.CurrentlyIdle() and not ( self.JustWokeFromSleep() or self.SystemBusy() )
        
    
    def GoodTimeToStartForegroundWork( self ) -> bool:
        
        return not self.JustWokeFromSleep()
        
    
    def JustWokeFromSleep( self ) -> bool:
        
        self.SleepCheck()
        
        return self._just_woke_from_sleep
        
    
    def InitModel( self ):
        
        try:
            
            self._InitTempDir()
            
        except:
            
            HydrusData.Print( 'Failed to initialise temp folder.' )
            
        
        self._fast_job_scheduler = HydrusThreading.JobScheduler( self )
        self._slow_job_scheduler = HydrusThreading.JobScheduler( self )
        
        self._fast_job_scheduler.start()
        self._slow_job_scheduler.start()
        
        self.db = self._InitDB()
        
    
    def InitView( self ):
        
        job = self.CallRepeating( 60.0, 300.0, self.MaintainDB, maintenance_mode = HC.MAINTENANCE_IDLE )
        
        job.WakeOnPubSub( 'wake_idle_workers' )
        job.ShouldDelayOnWakeup( True )
        
        self._daemon_jobs[ 'maintain_db' ] = job
        
        job = self.CallRepeating( 10.0, 120.0, self.SleepCheck )
        
        self._daemon_jobs[ 'sleep_check' ] = job
        
        job = self.CallRepeating( 10.0, 60.0, self.MaintainMemoryFast )
        
        self._daemon_jobs[ 'maintain_memory_fast' ] = job
        
        job = self.CallRepeating( 10.0, 300.0, self.MaintainMemorySlow )
        
        self._daemon_jobs[ 'maintain_memory_slow' ] = job
        
        upnp_services = self._GetUPnPServices()
        
        self.services_upnp_manager = HydrusNATPunch.ServicesUPnPManager( upnp_services )
        
        job = self.CallRepeating( 10.0, 43200.0, self.services_upnp_manager.RefreshUPnP )
        
        self._daemon_jobs[ 'services_upnp' ] = job
        
    
    def IsFirstStart( self ) -> bool:
        '''Returns true if this is the first run of this copy of Hydrus'''
        if self.db is None:
            
            return False
            
        else:
            
            return self.db.IsFirstStart()
            
        
    
    def LastShutdownWasBad( self ) -> bool :
        '''Returns true if last shutdown was unclean, detected by whther the PID file was cleaned up'''
        return self._last_shutdown_was_bad
        
    
    def MaintainDB( self, maintenance_mode = HC.MAINTENANCE_IDLE, stop_time = None ):
        '''GNDN'''
        pass
        
    
    def MaintainMemoryFast( self ):
        
        sys.stdout.flush()
        sys.stderr.flush()
        
        self.pub( 'memory_maintenance_pulse' )
        
        self._fast_job_scheduler.ClearOutDead()
        self._slow_job_scheduler.ClearOutDead()
        
    
    def MaintainMemorySlow( self ):
        
        gc.collect()
        
        HydrusPaths.CleanUpOldTempPaths()
        
        self._MaintainCallToThreads()
        
    
    def PrintProfile( self, summary, profile_text ):
        
        boot_pretty_timestamp = time.strftime( '%Y-%m-%d %H-%M-%S', time.localtime( self._timestamps[ 'boot' ] ) )
        
        profile_log_filename = self._name + ' profile - ' + boot_pretty_timestamp + '.log'
        
        profile_log_path = os.path.join( self.db_dir, profile_log_filename )
        
        with open( profile_log_path, 'a', encoding = 'utf-8' ) as f:
            
            prefix = time.strftime( '%Y/%m/%d %H:%M:%S: ' )
            
            f.write( prefix + summary )
            f.write( os.linesep * 2 )
            f.write( profile_text )
            
        
    
    def Read( self, action, *args, **kwargs ):
        
        return self._Read( action, *args, **kwargs )
        
    
    def RecordRunningStart( self ):
        '''
        Check if we crashed by seeing if the PID file from the previous run was cleaned up ( it should be on clean shutdown ).
        Then create a new PID file.
        '''
        
        self._last_shutdown_was_bad = HydrusData.LastShutdownWasBad( self.db_dir, self._name )
        
        self._i_own_running_file = True
        
        HydrusData.RecordRunningStart( self.db_dir, self._name )
        
    
    def ReleaseThreadSlot( self, thread_type ):
        
        with self._thread_slot_lock:
            
            if thread_type not in self._thread_slots:
                
                return
                
            
            ( current_threads, max_threads ) = self._thread_slots[ thread_type ]
            
            self._thread_slots[ thread_type ] = ( current_threads - 1, max_threads )
            
        
    
    def ReportDataUsed( self, num_bytes ):
        
        pass
        
    
    def ReportRequestUsed( self ):
        
        pass
        
    
    def ResetIdleTimer( self ):
        
        self._timestamps[ 'last_user_action' ] = HydrusData.GetNow()
        
    
    def SetDoingFastExit( self, value: bool ):
        
        self._doing_fast_exit = value
        
    
    def ShouldStopThisWork( self, maintenance_mode, stop_time = None ):
        
        if maintenance_mode == HC.MAINTENANCE_IDLE:
            
            if not self.CurrentlyIdle():
                
                return True
                
            
        elif maintenance_mode == HC.MAINTENANCE_SHUTDOWN:
            
            if not HG.do_idle_shutdown_work:
                
                return True
                
            
        
        if stop_time is not None:
            
            if HydrusData.TimeHasPassed( stop_time ):
                
                return True
                
            
        
        return False
        
    
    def ShutdownModel( self ):
        
        if self.db is not None:
            
            self.db.Shutdown()
            
            while not self.db.LoopIsFinished():
                
                self._PublishShutdownSubtext( 'waiting for db to finish up\u2026' )
                
                time.sleep( 0.1 )
                
            
        
        if self._fast_job_scheduler is not None:
            
            self._fast_job_scheduler.shutdown()
            
            self._fast_job_scheduler = None
            
        
        if self._slow_job_scheduler is not None:
            
            self._slow_job_scheduler.shutdown()
            
            self._slow_job_scheduler = None
            
        
        if hasattr( self, 'temp_dir' ):
            
            HydrusPaths.DeletePath( self.temp_dir )
            
        
        with self._call_to_thread_lock:
            
            for call_to_thread in self._call_to_threads:
                
                call_to_thread.shutdown()
                
            
            for long_running_call_to_thread in self._long_running_call_to_threads:
                
                long_running_call_to_thread.shutdown()
                
            
        
        HG.model_shutdown = True
        
        self._pubsub.Wake()
        
    
    def ShutdownView( self ):
        
        HG.view_shutdown = True
        
        self._ShutdownDaemons()
        
    
    def ShutdownFromServer( self ):
        
        raise Exception( 'This hydrus application cannot be shut down from the server!' )
        
    
    def SleepCheck( self ):
        
        with self._sleep_lock:
            
            last_sleep_check = self._timestamps[ 'last_sleep_check' ]
            
            if HydrusData.TimeHasPassed( last_sleep_check + 300 ): # it has been way too long since this method last fired, so we've prob been asleep
                
                self._just_woke_from_sleep = True
                
                self.ResetIdleTimer() # this will stop the background jobs from kicking in as soon as the grace period is over
                
                wake_delay_period = self._GetWakeDelayPeriod()
                
                self._timestamps[ 'now_awake' ] = HydrusData.GetNow() + wake_delay_period # enough time for ethernet to get back online and all that
                
                self._ShowJustWokeToUser()
                
            elif self._just_woke_from_sleep and HydrusData.TimeHasPassed( self._timestamps[ 'now_awake' ] ):
                
                self._just_woke_from_sleep = False
                
            
            self._timestamps[ 'last_sleep_check' ] = HydrusData.GetNow()
            
        
    
    def SimulateWakeFromSleepEvent( self ):
        
        with self._sleep_lock:
            
            self._timestamps[ 'last_sleep_check' ] = HydrusData.GetNow() - 3600
            
        
        self.SleepCheck()
        
    
    def SystemBusy( self ):
        
        return self._system_busy
        
    
    def WaitUntilDBEmpty( self ):
        ''' Wait unti the **QUEUED** database requests have completed.''' 
        while True:
            
            if HG.model_shutdown:
                
                raise HydrusExceptions.ShutdownException( 'Application shutting down!' )
                
            elif self.db.JobsQueueEmpty() and not self.db.CurrentlyDoingJob():
                
                return
                
            else:
                
                time.sleep( 0.00001 )
                
            
        
    
    def WaitUntilModelFree( self ):
        '''Wait until nothing is pending in the DB work queue and no messgaes are being sent.'''
        
        self.WaitUntilPubSubsEmpty()
        
        self.WaitUntilDBEmpty()
        
    
    def WaitUntilPubSubsEmpty( self ):
        '''Await pubsub termination quiet.'''
        
        while True:
            
            if HG.model_shutdown:
                
                raise HydrusExceptions.ShutdownException( 'Application shutting down!' )
                
            elif not self.CurrentlyPubSubbing():
                
                return
                
            else:
                
                time.sleep( 0.00001 )
                
            
        
    
    def WakeDaemon( self, name ):
        
        if name in self._daemon_jobs:
            
            self._daemon_jobs[ name ].Wake()
            
        
    
    def Write( self, action, *args, **kwargs ):
        
        return self._Write( action, False, *args, **kwargs )
        
    
    def WriteSynchronous( self, action, *args, **kwargs ):
        
        return self._Write( action, True, *args, **kwargs )
        
    
