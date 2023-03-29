/*******************************************************************************
 * @file    MFramework.cpp
 * @brief   Contains defination of MFramework Class
 * @todo
 ******************************************************************************/

/******************************************************************************
 *   includes:
 *    - system includes
 *    - application includes
 *****************************************************************************/
#include <signal.h>
#include <sys/file.h>
#include <fcntl.h>

#include <MFramework.h>
#include <ProtocolFactory.h>
#include <Utility.h>
#include <ConfigLoader.h>
#include <Debug.h>
#include <AMQPListener.h>
#include <AMQPProducer.h>

//! ABL Includes
#include <ABL_Connection.h>
#include <ABL_Statement.h>
#include <ABL_ResultSet.h>
#include <ABL_ThreadMacro.h>

using namespace MSYS;

/******************************************************************************
 *   definitions:
 *    - macro definitions
 *    - type definitions
 *    - struct definitions
 *****************************************************************************/

/******************************************************************************
 *   constants & variables:
 *    - constants
 *    - external variables
 *    - global variables
 *    - static variables
 *****************************************************************************/

/******************************************************************************
 *   function prototypes:
 *****************************************************************************/

/******************************************************************************
 *   global functions:
 *****************************************************************************/

namespace MSYS
{
/******************************************************************************
 *   constructor/destructor:
 *****************************************************************************/
	MFramework :: MFramework()
	{

	}

	MFramework :: ~MFramework()
	{
		Debug_msys("Framework deletion called");
	}

/******************************************************************************
 *   initializer/finalizer
 *****************************************************************************/

/******************************************************************************
 *   protected functions:
 *****************************************************************************/

/******************************************************************************
 *   private functions:
 *****************************************************************************/

/******************************************************************************
 *   public functions:
 *****************************************************************************/

/******************************************************************************
 *  Returns true is if Framework Run is started or stopped successfully 
 *****************************************************************************/
	bool MFramework :: Run(GlobalDataInfo* p_GDIPtr)
	{
		//! Start Queue Reader Thread
		pthread_t l_QueueReaderThreadID = 0;

		//! Start Queue Writer Thread
		pthread_t l_QueueWriterThreadID = 0;

		pthread_t l_ControlThreadID = 0;

		pthread_t l_MonitorThreadID = 0;

		SignalTranslator<InteruptException> g_objInteruptExceptionTranslator;
		SignalTranslator<SegmentationFault> g_objSegmentationFaultTranslator;

		if(NULL == p_GDIPtr)
		{
			// Error, No Global Data Info set
			return false;
		}

		try
		{
			//! Check if Component TYPE is set
			if(this->m_GetComponentType().empty())
			{
				throw ABL_Exception(2001, "Component type not set, use m_SetComponentType interface before calling Run");
			}

			//! Check if Component TYPE is set
			if(this->m_GetConfigFileName().empty())
			{
				throw ABL_Exception(2001, "Local configuration file not set, use m_SetConfigFileName interface before calling Run");
			}

			_m_GlobalDataInfoPtr = p_GDIPtr;

			//! Create SOContainer Object
			_m_GlobalDataInfoPtr->m_SOContainerObjPtr = new SOContainer;

			if(!m_InitConfigurations())
			{
				//! Error, Could not Initialise configurations
				return false;
			}

			if(!m_InitSessionConfigurations())
			{
				//! Error, Could not Initialise configurations
				return false;
			}

			Debug_msys("------------------------------------");
			Debug_msys("Loading Protocol Factory");
			if(this->_m_bIsProtocolFactoryRequired)
			{
				Debug_msys("ProtocolFactory is selected");

				//! Initialize ProtocolFactory Object in GlobalDataInfo
				_m_GlobalDataInfoPtr->m_ProtocolFactoryObjPtr = new ProtocolFactory(_m_GlobalDataInfoPtr->m_SOContainerObjPtr);

				//! Start Registring Protocols in ProtocolFactory
				//! FTP Protocol registration
				_m_GlobalDataInfoPtr->m_ProtocolFactoryObjPtr->m_RegisterProtocol(d_ftp_protocol_name, d_ftp_so_name);
				_m_GlobalDataInfoPtr->m_ProtocolFactoryObjPtr->m_RegisterProtocol(d_sftp_protocol_name, d_sftp_so_name);
		 		_m_GlobalDataInfoPtr->m_ProtocolFactoryObjPtr->m_RegisterProtocol(d_ftps_protocol_name, d_ftps_so_name);
				_m_GlobalDataInfoPtr->m_ProtocolFactoryObjPtr->m_RegisterProtocol(d_local_protocol_name, d_local_so_name);
			}
			else
			{
				Debug_msys("ProtocolFactory is not selected");
			}
			Debug_msys("------------------------------------");

			Debug_msys("------------------------------------");
			//! Initialize Reader Writer threads mandatorily as Objects are not initialized
			Debug_msys("Optional Queue Reader thread selected");

			this->m_MoveForwardInterfaceObj.m_SetMoveForward();
			ExecuteInNewThread0(&l_QueueReaderThreadID, NULL, MFramework, *this, void, &MFramework::m_StartQueueReaders);
			this->m_MoveForwardInterfaceObj.m_CanMoveForward(false);

			if(l_QueueReaderThreadID != 0)
			{
				_m_GlobalDataInfoPtr->m_UpdateThreadsRunning(l_QueueReaderThreadID);
			}
			Debug_msys("Optional Queue Writer thread selected");

			//! Start Queue Interface Thread
			_m_GlobalDataInfoPtr->m_OPQueueInterfaceObjPtr = new QueueInterface;

			this->m_MoveForwardInterfaceObj.m_SetMoveForward();
			ExecuteInNewThread0(&l_QueueWriterThreadID, NULL, MFramework, *this, void, &MFramework::m_StartQueueWriters);
			this->m_MoveForwardInterfaceObj.m_CanMoveForward(false);

			if(l_QueueWriterThreadID != 0)
			{
				_m_GlobalDataInfoPtr->m_UpdateThreadsRunning(l_QueueWriterThreadID);
			}

			//! Start Sessions
			m_StartSessions();
			Debug_msys("------------------------------------");

			Debug_msys("------------------------------------");

		/*	if(_m_GlobalDataInfoPtr->m_IsQueueReaderRequired())
			{
				Debug_msys("Optional Queue Reader thread selected");

				this->m_MoveForwardInterfaceObj.m_SetMoveForward();
				ExecuteInNewThread0(&l_QueueReaderThreadID, NULL, MFramework, *this, void, &MFramework::m_StartQueueReaders);
				this->m_MoveForwardInterfaceObj.m_CanMoveForward(false);

				if(l_QueueReaderThreadID != 0)
				{
					_m_GlobalDataInfoPtr->m_UpdateThreadsRunning(l_QueueReaderThreadID);
				}
			}
			else
			{
				Debug_msys("Optional Queue Reader thread is not selected");
			}
			Debug_msys("------------------------------------");

			Debug_msys("------------------------------------");
			if(_m_GlobalDataInfoPtr->m_IsQueueWriterRequired())
			{
				Debug_msys("Optional Queue Writer thread selected");

				//! Start Queue Interface Thread
				_m_GlobalDataInfoPtr->m_OPQueueInterfaceObjPtr = new QueueInterface;

				this->m_MoveForwardInterfaceObj.m_SetMoveForward();
				ExecuteInNewThread0(&l_QueueWriterThreadID, NULL, MFramework, *this, void, &MFramework::m_StartQueueWriters);
				this->m_MoveForwardInterfaceObj.m_CanMoveForward(false);

				if(l_QueueWriterThreadID != 0)
				{
					_m_GlobalDataInfoPtr->m_UpdateThreadsRunning(l_QueueWriterThreadID);
				}
			}
			else
			{
				Debug_msys("Optional Queue Writer thread is not selected");
			}*/
			Debug_msys("------------------------------------");

			Debug_msys("------------------------------------");
			Debug_msys("Starting Control thread");

			//! Start Control Thread
			this->m_MoveForwardInterfaceObj.m_SetMoveForward();
			ExecuteInNewThread0(&l_ControlThreadID, NULL, MFramework, *this, void, &MFramework::m_StartControlThread);
			
			this->m_MoveForwardInterfaceObj.m_CanMoveForward(false);

			if(l_ControlThreadID != 0)
			{
				_m_GlobalDataInfoPtr->m_UpdateThreadsRunning(l_ControlThreadID);
			}
			Debug_msys("------------------------------------");

			Debug_msys("------------------------------------");
			Debug_msys("Start Monitoring");

			//! Start Monitoring
			this->m_StartMonitoring();

			Debug_msys("------------------------------------");
			
			Debug_msys("MFramework::Run - Stop signal received in one of the sessions, exiting other sessions");

			_m_SessionDataMapMtx.mb_lock();
			for(SessionDataMap::iterator l_SessionDataMapIter = _m_SessionDataMap.begin();
				l_SessionDataMapIter != _m_SessionDataMap.end();
				l_SessionDataMapIter++)
			{
				if(l_SessionDataMapIter->second->m_GetThreadStatus())
				{
					l_SessionDataMapIter->second->m_MoveForwardInterfaceObj.m_SetMoveForward();
					l_SessionDataMapIter->second->m_StopThread();
					l_SessionDataMapIter->second->m_MoveForwardInterfaceObj.m_CanMoveForward(false);
				}
			}
			_m_SessionDataMapMtx.mb_unlock();

			this->WaitForThreads();
		}
		catch(ABL_Exception &e)
		{
			sprintf(_m_cDebugMessage, "MFramework :: Run -> Exception : %d | %s", e.mb_getErrorCode(), e.mb_getMessage().c_str());
			Debug_msys(_m_cDebugMessage);
		}
		catch (InteruptException &)
		{
			Debug_msys("INTERUPT RECIEVED in MFramework::Run");

			_m_GlobalDataInfoPtr->m_StopSystem();

			Debug_msys("MFramework::Run Stop System Called");

			_m_SessionDataMapMtx.mb_lock();
			for(SessionDataMap::iterator l_SessionDataMapIter = _m_SessionDataMap.begin();
				l_SessionDataMapIter != _m_SessionDataMap.end();
				l_SessionDataMapIter++)
			{
				if(l_SessionDataMapIter->second->m_GetThreadStatus())
				{
					l_SessionDataMapIter->second->m_MoveForwardInterfaceObj.m_SetMoveForward();
					l_SessionDataMapIter->second->m_StopThread();
					l_SessionDataMapIter->second->m_MoveForwardInterfaceObj.m_CanMoveForward(false);
				}
			}
			_m_SessionDataMapMtx.mb_unlock();

			if(l_QueueReaderThreadID != 0)
			{
				_m_GlobalDataInfoPtr->m_UpdateThreadsRunning(l_QueueReaderThreadID, false);
			}

			if(l_QueueWriterThreadID != 0)
			{
				_m_GlobalDataInfoPtr->m_UpdateThreadsRunning(l_QueueWriterThreadID, false);
			}

			if(l_ControlThreadID != 0)
			{
				
				_m_GlobalDataInfoPtr->m_UpdateThreadsRunning(l_ControlThreadID, false);
			}

			sleep(2);

			this->WaitForThreads();
		}
		catch (SegmentationFault &)
		{
			cout << "\n\nSegmentation fault RECIEVED in MFramework::Run\n\n" << endl;
			_m_GlobalDataInfoPtr->m_StopSystem();
			this->WaitForThreads();
		}
		catch(...)
		{
			Debug_msys("MFramework :: Run -> Unknown Exception");
		}
	}


/******************************************************************************
 *  Set the AMQP Brokers present in Database
 *****************************************************************************/
	void MFramework :: m_SetAMQPBrokerMap()
	{
		// Read from DB
		ABL_Connection      l_ABLConnectionObj;
		ABL_Statement       l_ABLStatementObj;
		ABL_ResultSet       l_ABLResultSetObj;

		try
		{
			l_ABLConnectionObj.mb_createConnection(_m_GlobalDataInfoPtr->m_ABLServiceObj);

			//! Prepare query to get broker details, execute the query to get result set to prepare objects of type Broker 
			//! Close the statemet and result set objects
			l_ABLStatementObj   = l_ABLConnectionObj.mb_createStatement(d_select_broker_query);

			l_ABLResultSetObj   = l_ABLStatementObj.mb_executeQuery();

			Broker* l_BrokerObjPtr;
			_m_GlobalDataInfoPtr->m_BrokersMap.clear();

			//! Fetching broker details
			while(l_ABLResultSetObj.mb_fetch())
			{
				l_BrokerObjPtr = new Broker();
				l_BrokerObjPtr->m_sBrokerName	= l_ABLResultSetObj.mb_getString(1).mb_getSTLString();
				l_BrokerObjPtr->m_sBrokerIP		= l_ABLResultSetObj.mb_getString(2).mb_getSTLString();
				l_BrokerObjPtr->m_iPort			= l_ABLResultSetObj.mb_getInt(3);
				l_BrokerObjPtr->m_sStatus		= l_ABLResultSetObj.mb_getString(4).mb_getSTLString();

				_m_GlobalDataInfoPtr->m_BrokersMap[l_BrokerObjPtr->m_sBrokerName] = l_BrokerObjPtr;
				
				l_BrokerObjPtr = NULL;
			}

			l_ABLStatementObj.mb_closeResultSet(l_ABLResultSetObj);
			l_ABLConnectionObj.mb_terminateStatement(l_ABLStatementObj);

			if(0 == _m_GlobalDataInfoPtr->m_BrokersMap.size())
			{
				throw ABL_Exception(1004, "No Brokers configured in M_BROKER table");
			}
		}
		catch(ABL_Exception &e)
		{
			throw ABL_Exception(e.mb_getErrorCode(), e.mb_getMessage());
		}
	}


/******************************************************************************
 *  Sets System Settings required for the System
 *****************************************************************************/
	void MFramework :: m_SetSystemSettings()
	{
		// Read from DB
		ABL_Connection      l_ABLConnectionObj;
		ABL_Statement       l_ABLStatementObj;
		ABL_ResultSet       l_ABLResultSetObj;

		try
		{
			l_ABLConnectionObj.mb_createConnection(_m_GlobalDataInfoPtr->m_ABLServiceObj);

			//! Prepare query to get broker details, execute the query to get result set to prepare objects of type Broker 
			//! Close the statemet and result set objects
			l_ABLStatementObj   = l_ABLConnectionObj.mb_createStatement(d_select_system_settings_query);

			l_ABLResultSetObj   = l_ABLStatementObj.mb_executeQuery();

			//! Clear System Settings Map
			_m_GlobalDataInfoPtr->m_SystemSettingsMap.clear();

			//! Fetching broker details
			while(l_ABLResultSetObj.mb_fetch())
			{
				_m_GlobalDataInfoPtr->m_SystemSettingsMap[l_ABLResultSetObj.mb_getString(1).mb_getSTLString()] = l_ABLResultSetObj.mb_getString(2).mb_getSTLString();
			}

			if(0 == _m_GlobalDataInfoPtr->m_SystemSettingsMap.size())
			{
				Debug_msys("There are no System Settings Defined for the Component");
			}
		}
		catch(ABL_Exception &e)
		{
			Debug_msys("M_SYSTEM_SETTINGS table not present or DB Connection lost, not loading System Settings");
		}
		catch(...)
		{
			Debug_msys("Unknown Exception while loading System Settings, not loading System Settings");
		}
	}


/******************************************************************************
 *  Initialize file configurations in the Home Path config directory
 *****************************************************************************/
	bool MFramework :: m_InitConfigurations()
	{
		try
		{
			//! MSYS Home path
			std::string l_sHomePath = _m_GlobalDataInfoPtr->m_GetHomePath();
			long l_iGlobalLogLevel;
			struct flock lock;

			//! Check if the home path is set
			if(l_sHomePath.empty())
			{
				//! Error, Home path not set
				throw ABL_Exception(6000, "Home path not set");
			}

			//! Initialize Utility Object in GlobalDataInfo
			UtilityCreator l_UtilityCreator;
			l_UtilityCreator = _m_GlobalDataInfoPtr->m_SOContainerObjPtr->m_GetCreatorFunction<UtilityCreator>(d_utility_so_name);
			_m_GlobalDataInfoPtr->m_UtilityObjPtr = l_UtilityCreator(); //! Utility Object Loaded

			//! Global Data Info Assignments
			//! Done to give easy readability
			_m_UtilityObjPtr = _m_GlobalDataInfoPtr->m_UtilityObjPtr;

			//! Start Loading Configuration Files

			std::string l_sLocalFileName; //! Holds the local configuration file name with path

			l_sLocalFileName = this->m_GetConfigFileName();
			Debug_msys("Local configuration file to start is : "+l_sLocalFileName);

			//! Check if Local configuration file is present
			if(!_m_UtilityObjPtr->m_IsFilePresent(l_sLocalFileName))
			{
				//! Error, Configuration file is not present
				throw ABL_Exception(6001, "Local configuration file : '"+l_sLocalFileName+"' not present");
			}

			int l_iLocalFileDesc;
			l_iLocalFileDesc = open(l_sLocalFileName.c_str(), O_RDONLY, 0);
			if(-1 == l_iLocalFileDesc)
			{
				throw ABL_Exception(6002, l_sLocalFileName+" : Local configuration could not be opened");
			}

			int l_iLock; //!< Stores whether config file has been locked or not

			//! Loop for Local file Lock
			while(true)
			{
				l_iLock = 0;

				//! Get exclusive lock (LOCK_EX) on file with Non Blocking Option LOCK_NB
				l_iLock = flock(l_iLocalFileDesc, LOCK_EX | LOCK_NB);

				if(-1 == l_iLock)
				{
					Debug_msys("Local configuration file could not be locked");

					int l_iLockingInterval = d_lock_file_interval;
					while(l_iLockingInterval > 0)
					{
						sleep(1);
						l_iLockingInterval--;
					}

					continue;
				}
				else
				{
					Debug_msys("Local configuration file locked");

					//! Create ConfigLoader object to load Local file configurations
					ConfigLoader l_ConfigLoaderObj;
					if(-1 == l_ConfigLoaderObj.LoadConfig(l_sLocalFileName.c_str(), '='))
					{
						throw ABL_Exception(6001, "Unable to load local configuration file : \'"+l_sLocalFileName+"\' in ConfigLoader");
					}

					//! Load Session Table name
					std::string l_sSessionTableName;
					l_ConfigLoaderObj.GetScalarSTLString(l_sSessionTableName, d_session_table_name.c_str());
					if(l_sSessionTableName.empty())
					{
						throw ABL_Exception(6002, "Session table name is not set");
					}
					_m_GlobalDataInfoPtr->m_SetSessionTableName(l_sSessionTableName);

					//! Load Session Table name
					std::string l_sComponentPrivateName;
					l_ConfigLoaderObj.GetScalarSTLString(l_sComponentPrivateName, d_component_private_name.c_str());
					if(l_sComponentPrivateName.empty())
					{
						throw ABL_Exception(6002, "Component Private name not set in configuration file");
					}
					
					l_ConfigLoaderObj.GetScalarMatch<long>(l_iGlobalLogLevel, d_global_log_level.c_str());
					if(l_iGlobalLogLevel > d_log_level_alert || l_iGlobalLogLevel < d_log_level_info)
					{
						Debug_msys("Log level setting mismatch, setting to default 31");
						l_iGlobalLogLevel = 31;
					}
	
					//! Get Global Configuration File Name
                    std::string l_sGlobalFileName;
                    l_ConfigLoaderObj.GetScalarSTLString(l_sGlobalFileName, d_global_config_file_path.c_str());
					if(l_sGlobalFileName.empty())
					{
						Debug_msys("No global lock file mentioned in configuration, ignoring Global Locking mechanism");
					}
					else
					{
                    	Debug_msys("Global configuration file to lock is : "+l_sGlobalFileName);
						
						//! Check if Global configuration file is present
						if(!_m_UtilityObjPtr->m_IsFilePresent(l_sGlobalFileName))
						{
							//! Error, Global Configuration file is not present
							throw ABL_Exception(6001, "Global configuration file : \'"+l_sGlobalFileName+"\' not present");
                    	}

						int l_iGlobalFileDesc;
						l_iGlobalFileDesc = open(l_sGlobalFileName.c_str(), O_WRONLY, 0);
						if(-1 == l_iGlobalFileDesc)
						{
							throw ABL_Exception(6002, l_sLocalFileName+" : Global configuration could not be opened");
						}
						else
						{
							//! Loop for Global File Lock
							while(true)
							{
								l_iLock = 0;

								//! Get exclusive lock (LOCK_EX) on file with Non Blocking Option LOCK_NB
								//l_iLock = flock(l_iGlobalFileDesc, LOCK_EX | LOCK_NB);

								memset (&lock, 0, sizeof(lock));
								lock.l_type = F_WRLCK;
								l_iLock = fcntl(l_iGlobalFileDesc, F_SETLK, &lock);
								if(-1 == l_iLock)
								{
									Debug_msys("Global configuration file could not be locked");
									int l_iLockingInterval = d_lock_file_interval;
									while(l_iLockingInterval > 0)
									{
										sleep(1);
										l_iLockingInterval--;
									}
								
									continue;
								}
								else
								{
									//! Global file locked. Break from loop
									Debug_msys("Global configuration file locked"); 
								
									break;
								}	

								sleep(1);
							} //! Loop for Global File Lock
						}
					} 

					//! Set ABL_Servive object to get the DB Connection
					std::string l_sDBFileName = l_sHomePath+"/Config/"+d_db_config_file;
					if(!_m_UtilityObjPtr->m_IsFilePresent(l_sDBFileName))
					{
						throw ABL_Exception(6001, "DB configuration file : \'"+l_sDBFileName+"\' not present");
					}
					_m_GlobalDataInfoPtr->m_ABLServiceObj.mb_initServices(l_sDBFileName);

					//! Load Component Details Here
					if(!this->m_SetComponentDetails(l_sComponentPrivateName))
					{
						return false;
					}

					Debug_msys("Initializing Broker Map");
					this->m_SetAMQPBrokerMap();
					
					Debug_msys("Initializing System Settings");
					this->m_SetSystemSettings();

					if(this->_m_bIsLoggerWritterRequired)
					{
						//! Starting LoggerWriter
						Debug_msys("Initializing LoggerWriter");
						_m_GlobalDataInfoPtr->m_LoggerObj.m_InitLoggerWriter(l_iGlobalLogLevel, m_GetComponentName(), _m_GlobalDataInfoPtr);
					}
					else
					{
						Debug_msys("LoggerWriter not selected");
					}

					break;
				} //! If Local configuration file locked
			} //! Loop for Local File Lock

			return true;
		}
		catch(ABL_Exception &e)
		{
			sprintf(_m_cDebugMessage, "MFramework :: m_InitConfigurations -> Exception : %d | %s", e.mb_getErrorCode(), e.mb_getMessage().c_str());
			Debug_msys(_m_cDebugMessage);
			return false;
		}
	} //! MFramework :: m_InitConfigurations()


/******************************************************************************
 *  Initialize Component Details: 
 *	Load Control Queue Name, Component Private Name and status of the component
 *****************************************************************************/
	bool MFramework :: m_SetComponentDetails(std::string p_sComponentPrivateName)
	{
		try
		{
			//! Set Component Name
			this->m_SetComponentName(p_sComponentPrivateName);

			//! Get the Database connection
			ABL_Connection  l_ABLConnectionObj;
			l_ABLConnectionObj.mb_createConnection (_m_GlobalDataInfoPtr->m_ABLServiceObj);

			char l_cSelectComponentQuery[d_query_buffer_size];
			sprintf(l_cSelectComponentQuery, d_component_load_query.c_str(), this->m_GetComponentType().c_str(), p_sComponentPrivateName.c_str());
			
			Debug_msys("Loading component details : Query : "+_m_UtilityObjPtr->m_ToString(l_cSelectComponentQuery));

			//! Create ABL Statement
			ABL_Statement l_ABLStatementObj;
			l_ABLStatementObj = l_ABLConnectionObj.mb_createStatement(l_cSelectComponentQuery);

			ABL_ResultSet l_ABLResultSetObj;
			l_ABLResultSetObj = l_ABLStatementObj.mb_executeQuery();
			
			bool l_bIsComponentSet = false;
			if(l_ABLResultSetObj.mb_fetch())
			{
				//! Set Component ID
				this->m_SetComponentID(l_ABLResultSetObj.mb_getInt(1));
				Debug_msys("Component ID is : "+_m_UtilityObjPtr->m_ToString(this->m_GetComponentID()));

				//! Set Control Queue Name
				this->m_SetControlQueueName(l_ABLResultSetObj.mb_getString(2).c_str());
				Debug_msys("Control Queue Name is : "+this->m_GetControlQueueName());

				l_bIsComponentSet = true;
			} //! End of While

			//! Close Result Set and Terminate the statement
			l_ABLConnectionObj.mb_terminateStatement(l_ABLStatementObj);

			if(!l_bIsComponentSet)
			{
				throw ABL_Exception(7000, "No Component details to load for component : "+m_GetComponentName()+" in M_COMPONENT table");
			}

			//! Loading of Sessions have been done. Return back success
			return true;
		}
		catch(ABL_Exception &e)
		{
			sprintf(_m_cDebugMessage, "MFramework :: m_GetSessionDetails -> Exception : %d | %s", e.mb_getErrorCode(), e.mb_getMessage().c_str());
			Debug_msys(_m_cDebugMessage);
			return false;
		}
	} //! MFramework :: m_GetSessionDetails()

/******************************************************************************
 *  Initialize session configurations: 
 *	Load Session Objects, Call initialize in them, and Call Run
 *****************************************************************************/
	bool MFramework :: m_InitSessionConfigurations()
	{
		try
		{
			//! Call GetSessionDetails to load the required Session ID and Session SO Name
			//! The function returns true only if there are sessions to Load
			//! Incase of any exceptions or no data to load it will give false
			if(!m_GetSessionDetails())
			{
				return false;
			}

			Debug_msys("MFramework :: m_InitSessionConfigurations -> Session start-up data loaded");

			_m_SessionDataMap.clear(); //! Clear the sessions map

			//! Session creator function
			SessionCreator l_SessionCreator;

			//! Base Data pointer
			AbstractSessionBase* l_SessionBaseObjPtr;

			for(SessionLoadDetailsMap::iterator l_SessionLoadDetailsMapIter = _m_SessionLoadDetailsMap.begin();
				l_SessionLoadDetailsMapIter != _m_SessionLoadDetailsMap.end();
				l_SessionLoadDetailsMapIter++)
			{
				//Debug_msys(l_SessionLoadDetailsMapIter->first);
				//Debug_msys(l_SessionLoadDetailsMapIter->second);
				Debug_msys(l_SessionLoadDetailsMapIter->second->m_sSessionSOName);
				//! Session Object Creator function pointer
				l_SessionCreator = \
				_m_GlobalDataInfoPtr->m_SOContainerObjPtr->m_GetCreatorFunction<SessionCreator>(l_SessionLoadDetailsMapIter->second->m_sSessionSOName);

				//! Get the session object
				l_SessionBaseObjPtr = NULL;
				l_SessionBaseObjPtr = l_SessionCreator(); //! Call the Creator function to get Session object
				if(NULL == l_SessionBaseObjPtr)
				{
					Debug_msys("Unable to create session object, invalid handle");
					continue;
				}
				
				//! Add the session object to SessionDataMap
				//! Set Session ID before starting so that session SO knows which ID to load
				//! from its session table
				l_SessionBaseObjPtr->m_SetSessionID(l_SessionLoadDetailsMapIter->first);
				
				l_SessionBaseObjPtr->m_ClearLinkedSessionsLst();

				for(LinkedSessionsLst::iterator l_LinkedSessionsLstIter = l_SessionLoadDetailsMapIter->second->m_LinkedSessionsLst.begin();
					l_LinkedSessionsLstIter != l_SessionLoadDetailsMapIter->second->m_LinkedSessionsLst.end();
					l_LinkedSessionsLstIter++)
				{
					l_SessionBaseObjPtr->m_SetLinkedSessions(*l_LinkedSessionsLstIter);
				}
		
				l_SessionBaseObjPtr->m_SetSessionSOName(l_SessionLoadDetailsMapIter->second->m_sSessionSOName);
				l_SessionBaseObjPtr->m_SetSessionName(l_SessionLoadDetailsMapIter->second->m_sSessionName);

				_m_SessionDataMap[l_SessionLoadDetailsMapIter->first] = l_SessionBaseObjPtr;
			} //! for

			return true;
		}
		catch(ABL_Exception &e)
		{
			sprintf(_m_cDebugMessage, "MFramework :: m_InitSessionConfigurations -> Exception : %d | %s", \
					 e.mb_getErrorCode(), e.mb_getMessage().c_str());
			Debug_msys(_m_cDebugMessage);
			return false;
		}
	} //! MFramework :: m_InitSessionConfigurations()


/******************************************************************************
 *  Start the session threads: 
 *	Create Local threads using m_StartSessionThread
 *****************************************************************************/
	void MFramework :: m_StartSessions()
	{
		pthread_t l_SessionThreadID;
		std::string l_sSessionName = "";

		Debug_msys("Starting Session threads");

		try
		{
			_m_SessionDataMapMtx.mb_lock();
 
			std::string l_sHomePath = _m_GlobalDataInfoPtr->m_GetHomePath();
			l_sHomePath.append("/FWorkCheckpoint/");
					
			std::string l_sFWorkCheckpoint = "";
			for(SessionDataMap::iterator l_SessionDataMapIter = _m_SessionDataMap.begin();
				l_SessionDataMapIter != _m_SessionDataMap.end();
				l_SessionDataMapIter++)
			{
				if(("Scheduler" != (m_GetComponentName())) && ("Logger" != (m_GetComponentName())) && ("QueueManager" != (m_GetComponentName())))
				{
					AbstractSessionBase* l_SessionBaseObjPtr;

					l_sSessionName = l_SessionDataMapIter->second->m_GetSessionName();

					l_sFWorkCheckpoint.clear();
					l_sFWorkCheckpoint.assign(l_sHomePath);
					l_sFWorkCheckpoint.append(_m_UtilityObjPtr->m_ToString(l_SessionDataMapIter->second->m_GetSessionID()));

					if(!_m_UtilityObjPtr->m_CreateDirectory(l_sFWorkCheckpoint.c_str()))
					{
						std::cout << "FWork Checkpoint directory creation failed " << std::endl;
					}
					else
					{
						std::string l_sSubChkPointDir = "";
						l_sSubChkPointDir.assign(l_sFWorkCheckpoint);

						l_sSubChkPointDir.append("/Dir1");
						
						if(!_m_UtilityObjPtr->m_CreateDirectory(l_sSubChkPointDir.c_str()))
						{
							std::cout << "FWork Checkpoint directory creation failed " << std::endl;
						}

						l_sSubChkPointDir.clear();
						l_sSubChkPointDir.assign(l_sFWorkCheckpoint);
						l_sSubChkPointDir.append("/Dir2");
						
						if(!_m_UtilityObjPtr->m_CreateDirectory(l_sSubChkPointDir.c_str()))
						{
							std::cout << "FWork Checkpoint directory creation failed " << std::endl;
						}
					}
				}
								
				l_SessionDataMapIter->second->m_MoveForwardInterfaceObj.m_SetMoveForward(); //! Reset moveForwardSem
				l_SessionDataMapIter->second->m_InputQueueObjPtr = new QueueInterface; //! Start the Queuing Interface
				
				ExecuteInNewThread1(&l_SessionThreadID, NULL, MFramework, *this, void, &MFramework::m_StartSessionThread, AbstractSessionBase*, l_SessionDataMapIter->second);

				l_SessionDataMapIter->second->m_MoveForwardInterfaceObj.m_CanMoveForward(false); //! Wait until moveForwardSem is released

				if(l_SessionDataMapIter->second->m_GetThreadStatus())
				{
					l_SessionDataMapIter->second->m_SetThreadID(l_SessionThreadID);
					_m_GlobalDataInfoPtr->m_UpdateThreadsRunning(l_SessionThreadID);
				}
				else
				{
					Debug_msys("MFramework :: StartSessions : Unable to start session "+l_SessionDataMapIter->second->m_GetSessionName());
				}
			}
			_m_SessionDataMapMtx.mb_unlock();
		}
		catch(ABL_Exception &e)
		{
			sprintf(_m_cDebugMessage, "MFramework :: StartSessions -> Exception : %d | %s", \
					 e.mb_getErrorCode(), e.mb_getMessage().c_str());
			Debug_msys(_m_cDebugMessage);
		}			
	} //! MFramework :: m_StartSessions()


/******************************************************************************
 *  Start the session threads: 
 *	Call the Run function of the session thread
 *****************************************************************************/
	void MFramework :: m_StartSessionThread(AbstractSessionBase* p_SessionBaseObjPtr)
	{
		try
		{
			if(("Scheduler" != (m_GetComponentName())) && ("Logger_1" != (m_GetComponentName())) && ("QueueManager" != (m_GetComponentName())))
			{
				std::string l_sSourceDirPath = "";
				std::string l_sDestDirPath = "";

				l_sSourceDirPath.assign(_m_GlobalDataInfoPtr->m_GetHomePath());
				l_sSourceDirPath.append("/FWorkCheckpoint/");
				l_sSourceDirPath.append(_m_UtilityObjPtr->m_ToString(p_SessionBaseObjPtr->m_GetSessionID()));
				l_sSourceDirPath.append("/Dir1/");

				l_sDestDirPath.assign(_m_GlobalDataInfoPtr->m_GetHomePath());
				l_sDestDirPath.append("/FWorkCheckpoint/");
				l_sDestDirPath.append(_m_UtilityObjPtr->m_ToString(p_SessionBaseObjPtr->m_GetSessionID()));
				l_sDestDirPath.append("/Dir2/");

				if(_m_UtilityObjPtr->m_DirCopy(l_sSourceDirPath, l_sDestDirPath))
				{
					//throw ABL_Exception(4014, "Failed to copy files from Dir1 to Dir2");
				}
			}
			//! Call the Run Function of Session Base
			p_SessionBaseObjPtr->Run(_m_GlobalDataInfoPtr);

			pthread_exit(NULL);
		}
		catch(ABL_Exception &e)
		{
			sprintf(_m_cDebugMessage, "MFramework :: m_StartSessionThread -> Exception : %d | %s", \
					 e.mb_getErrorCode(), e.mb_getMessage().c_str());
			Debug_msys(_m_cDebugMessage);
		}
	} //! MFramework :: StartSessionThread(AbstractSessionBase* p_SessionBaseObjPtr)


/******************************************************************************
 *  Initialize session configurations: 
 *	Load Session Objects, Call initialize in them, and Call Run
 *****************************************************************************/
	bool MFramework :: m_GetSessionDetails(int p_iSessionID)
	{
		try
		{
			//! Linked Session Object Pointer
			LinkedSessions* l_LinkedSessionsObjPtr;

			//! Get the Database connection
			ABL_Connection  l_ABLConnectionObj;
			l_ABLConnectionObj.mb_createConnection (_m_GlobalDataInfoPtr->m_ABLServiceObj);

			char l_cSelectSessionQuery[d_query_buffer_size];

			if(p_iSessionID == -1)
			{
				_m_SessionLoadDetailsMap.clear();
				sprintf(l_cSelectSessionQuery, d_data_load_query.c_str(), _m_GlobalDataInfoPtr->m_GetSessionTableName().c_str(), m_GetComponentID());
			}
			else
			{
				sprintf(l_cSelectSessionQuery, d_new_data_load_query.c_str(), _m_GlobalDataInfoPtr->m_GetSessionTableName().c_str(), p_iSessionID, m_GetComponentID());
			}
			
			Debug_msys("Loading session details : Query : "+_m_UtilityObjPtr->m_ToString(l_cSelectSessionQuery));
			
			//! Create ABL Statement
			ABL_Statement l_ABLStatementObj;
			l_ABLStatementObj = l_ABLConnectionObj.mb_createStatement(l_cSelectSessionQuery);

			ABL_ResultSet l_ABLResultSetObj;
			l_ABLResultSetObj = l_ABLStatementObj.mb_executeQuery();
			
			bool l_bIsDataLoaded = false;

			SessionLoadDetails* l_SessionLoadDetailsObjPtr = NULL;

			int l_iLinkedSessionID;

			while(l_ABLResultSetObj.mb_fetch())
			{
				//! Session Load Details Object to store
				l_SessionLoadDetailsObjPtr = new SessionLoadDetails;

				//! Get the Session ID
				l_SessionLoadDetailsObjPtr->m_iSessionID = l_ABLResultSetObj.mb_getInt(1);
				Debug_msys("Session ID : "+_m_UtilityObjPtr->m_ToString(l_SessionLoadDetailsObjPtr->m_iSessionID));

				//! Get the Sessions SO to be Loaded
				l_SessionLoadDetailsObjPtr->m_sSessionSOName.assign(l_ABLResultSetObj.mb_getString(2).c_str());
				Debug_msys("Loading SO Name : "+l_SessionLoadDetailsObjPtr->m_sSessionSOName);

				//! Get the Sessions SO to be Loaded
				l_SessionLoadDetailsObjPtr->m_sSessionName.assign(l_ABLResultSetObj.mb_getString(3).c_str());
				Debug_msys("Session Name "+l_SessionLoadDetailsObjPtr->m_sSessionName);
				
				//! Clear the linked session ID list
				l_SessionLoadDetailsObjPtr->m_LinkedSessionsLst.clear();

				//! Linked Session ID
				l_iLinkedSessionID = l_ABLResultSetObj.mb_getInt(4);
				if(l_iLinkedSessionID != 0)
				{
					//! Load Details from DB M_LINKED_SESSIONS
					//! Create ABL Statement
					memset(l_cSelectSessionQuery, '\0', d_query_buffer_size);
					sprintf(l_cSelectSessionQuery, d_linked_session_load_query.c_str(), l_iLinkedSessionID);

					Debug_msys("Loading linked session ID details : Query : "+_m_UtilityObjPtr->m_ToString(l_cSelectSessionQuery));

					ABL_Statement l_ABLLinkedStatementObj;
					l_ABLLinkedStatementObj = l_ABLConnectionObj.mb_createStatement(l_cSelectSessionQuery);

					ABL_ResultSet l_ABLLinkedResultSetObj;
					l_ABLLinkedResultSetObj = l_ABLLinkedStatementObj.mb_executeQuery();

					while(l_ABLLinkedResultSetObj.mb_fetch())
					{
						l_LinkedSessionsObjPtr = new LinkedSessions;
						l_LinkedSessionsObjPtr->m_iNextComponentSessionID = l_ABLLinkedResultSetObj.mb_getInt(1);
						l_LinkedSessionsObjPtr->m_sNextComponentType = l_ABLLinkedResultSetObj.mb_getString(2).c_str();

						//! Get the Linked Sessions
						l_SessionLoadDetailsObjPtr->m_LinkedSessionsLst.push_back(l_LinkedSessionsObjPtr);
					}
				}
				else
				{
					Debug_msys("No Linked Session IDs Configured");
					
				}

				//! Put SessionLoadDetails object in _m_SessionLoadDetailsMap
				_m_SessionLoadDetailsMap[l_SessionLoadDetailsObjPtr->m_iSessionID] = l_SessionLoadDetailsObjPtr;

				l_bIsDataLoaded = true;

				//! Reset SessionLoadDetails object to NULL
				//delete l_LinkedSessionsObjPtr;
				//delete l_SessionLoadDetailsObjPtr;
				l_SessionLoadDetailsObjPtr = NULL;
			} //! End of While

			//! Close Result Set and Terminate the statement
			l_ABLConnectionObj.mb_terminateStatement(l_ABLStatementObj);

			if(0 == _m_SessionLoadDetailsMap.size() || !l_bIsDataLoaded)
			{
				std::string l_sTempString;

				if(p_iSessionID == -1)
				{
					l_sTempString = "No sessions to load for component : "+m_GetComponentName();
				}
				else
				{
					l_sTempString = "Session ID "+_m_UtilityObjPtr->m_ToString(p_iSessionID)+" is either in D status or Database entry not present in component : "+m_GetComponentName();
				}

				throw ABL_Exception(7000, l_sTempString);
			}

			//! Loading of Sessions have been done. Return back success
			return true;
		}
		catch(ABL_Exception &e)
		{
			sprintf(_m_cDebugMessage, "MFramework :: m_GetSessionDetails -> Exception : %d | %s", e.mb_getErrorCode(), e.mb_getMessage().c_str());
			Debug_msys(_m_cDebugMessage);
			return false;
		}
	} //! MFramework :: m_GetSessionDetails()


/******************************************************************************
 *  Starts the All Queue Reader Threads
 *****************************************************************************/
	void MFramework :: m_StartQueueReaders()
	{
		Debug_msys("Starting Queue Readers");

		while(true)
		{
			try
			{
				QueueReaderThreadInfo* l_QueueReaderThreadInfoObjPtr;

				//! Get the Database connection
				ABL_Connection  l_ABLConnectionObj;
				l_ABLConnectionObj.mb_createConnection(_m_GlobalDataInfoPtr->m_ABLServiceObj);

				char l_cSelectInputQueueQuery[d_query_buffer_size];
				sprintf(l_cSelectInputQueueQuery, d_input_queue_query.c_str(), this->m_GetComponentType().c_str());
				
				//! Create ABL Statement
				ABL_Statement l_ABLStatementObj;
				l_ABLStatementObj = l_ABLConnectionObj.mb_createStatement(l_cSelectInputQueueQuery);

				ABL_ResultSet l_ABLResultSetObj;
				l_ABLResultSetObj = l_ABLStatementObj.mb_executeQuery();
				
				_m_QueueReaderThreadInfoMap.clear();

				while(l_ABLResultSetObj.mb_fetch())
				{
					l_QueueReaderThreadInfoObjPtr = new QueueReaderThreadInfo;

					l_QueueReaderThreadInfoObjPtr->m_sInputQueueName.assign(l_ABLResultSetObj.mb_getString(1).c_str());
					l_QueueReaderThreadInfoObjPtr->m_bThreadStatus = false;

					_m_QueueReaderThreadInfoMap[l_QueueReaderThreadInfoObjPtr->m_sInputQueueName] = l_QueueReaderThreadInfoObjPtr;
					delete l_QueueReaderThreadInfoObjPtr;
				} //! End of While

				//! Close Result Set and Terminate the statement
				l_ABLConnectionObj.mb_terminateStatement(l_ABLStatementObj);

				if(0 == _m_QueueReaderThreadInfoMap.size())
				{
					throw ABL_Exception(7001, "No input data queues configured for component type : "+this->m_GetComponentType());
				}

				break;
			}
			catch (ABL_Exception &e)
			{
				Debug_msys("Unable to get Input Queue List for Queue Reader, retrying after "\
					+_m_UtilityObjPtr->m_ToString(d_get_input_queue_list_interval)+" seconds");
			}

			//sleep(d_get_input_queue_list_interval);
		} // while

		pthread_t l_QueueReaderThreadID;
		QueueReaderThreadInfo* l_QueueReaderThreadInfoObjPtr;

		//! Start threads to read on respective input queue by
		//! Looping through Input Queue list
		for(QueueReaderThreadInfoMap::iterator l_QueueReaderThreadInfoMapIter = _m_QueueReaderThreadInfoMap.begin();
			l_QueueReaderThreadInfoMapIter != _m_QueueReaderThreadInfoMap.end();
			l_QueueReaderThreadInfoMapIter++)
		{
			l_QueueReaderThreadInfoObjPtr = l_QueueReaderThreadInfoMapIter->second;
			Debug_msys("Starting thread for Input Queue : "+l_QueueReaderThreadInfoMapIter->second->m_sInputQueueName);

			l_QueueReaderThreadInfoObjPtr->m_MoveForwardInterfaceObj.m_SetMoveForward();
			ExecuteInNewThread1(&l_QueueReaderThreadID, NULL, MFramework, *this, void, &MFramework::m_StartQueueReaderThread, QueueReaderThreadInfo*, l_QueueReaderThreadInfoObjPtr);
			l_QueueReaderThreadInfoObjPtr->m_MoveForwardInterfaceObj.m_CanMoveForward(false);

			l_QueueReaderThreadInfoObjPtr->m_ThreadID = l_QueueReaderThreadID;
		}

		this->m_MoveForwardInterfaceObj.m_CanMoveForward();

		//! Looping through Input Queue list and join on thread IDs
		for(QueueReaderThreadInfoMap::iterator l_QueueReaderThreadInfoMapIter = _m_QueueReaderThreadInfoMap.begin();
			l_QueueReaderThreadInfoMapIter != _m_QueueReaderThreadInfoMap.end();
			l_QueueReaderThreadInfoMapIter++)
		{
			l_QueueReaderThreadInfoObjPtr = l_QueueReaderThreadInfoMapIter->second;
			pthread_join(l_QueueReaderThreadInfoObjPtr->m_ThreadID, NULL);
		}

		Debug_msys("QueueReader Threads exited gracefully");
	} //! MFramework :: m_StartQueueReaders()


/******************************************************************************
 *  Starts Queue Reader functionality
 *****************************************************************************/
	void MFramework :: m_StartQueueReaderThread(QueueReaderThreadInfo* p_QueueReaderThreadInfoObjPtr)
	{
		AMQPListener* l_AMQPListenerObjPtr;

		try
		{
			//! Connect to the AMQP Producer using the information got on top
			AMQPListenerCreator l_AMQPListenerCreator;
			l_AMQPListenerCreator = _m_GlobalDataInfoPtr->m_SOContainerObjPtr->m_GetCreatorFunction<AMQPListenerCreator>(d_amqp_listener_so_name);

			l_AMQPListenerObjPtr = l_AMQPListenerCreator(); //! Generate AMQP Producer object pointer using function pointer

			if(l_AMQPListenerObjPtr)
			{
				Broker* l_Broker;

				for(BrokersMap::iterator l_BrokersMapIter = _m_GlobalDataInfoPtr->m_BrokersMap.begin(); 
					l_BrokersMapIter != _m_GlobalDataInfoPtr->m_BrokersMap.end(); 
					l_BrokersMapIter++)
				{
					l_Broker = l_BrokersMapIter->second;

					if(l_AMQPListenerObjPtr->m_InitializeListener(l_Broker->m_sBrokerIP, l_Broker->m_iPort, p_QueueReaderThreadInfoObjPtr->m_sInputQueueName, 1))
					{
						
						Debug_msys("m_StartQueueReaderThread is connected to AMQP Broker : "+l_Broker->m_sBrokerIP+" on Port : "+_m_UtilityObjPtr->m_ToString(l_Broker->m_iPort));
						break;
					}
					else
					{
						
						Debug_msys("m_StartQueueReaderThread is unable to connect to AMQP Broker : "+l_Broker->m_sBrokerIP+" on Port :"+_m_UtilityObjPtr->m_ToString(l_Broker->m_iPort));
					}
				}
			}
			else
			{
				throw ABL_Exception(1005, "m_StartQueueReaderThread is unable to get AMQP Listener object");
			}
			
			p_QueueReaderThreadInfoObjPtr->m_bThreadStatus = true;
			p_QueueReaderThreadInfoObjPtr->m_MoveForwardInterfaceObj.m_CanMoveForward(true);

			std::string l_sQueueData;
			DataMap* l_InputDataMap;
			DataMap l_ReceivedDataMap;
			DataMap::iterator l_DataMapIter;
			DataMap::iterator l_DataMapIter1;

			//! Temporary Variables used for Parsing Input Data Packet
			std::string l_sSeparator = "\n";
			std::string l_sOutString;
			int l_iStart = 0;
			int l_iEnd = 0;

			//! Iterator to find if Session ID sent is present in Loaded Sessions
			SessionDataMap::iterator l_SessionDataMapIter;

			while(true)
			{
				if(0 == _m_GlobalDataInfoPtr->m_IsStopSystem())
				{
					Debug_msys("Stop signal received for QueueReader thread, exiting");
					break;
				}

				//! Start Reading from Queue
				l_sQueueData = l_AMQPListenerObjPtr->m_ReadQueue();

				//! If Packet is not empty
				if(!l_sQueueData.empty())
				{
					//! Reset start and end positions for parsing
					l_iStart = 0;
					l_iEnd = 0;

					//! Clear the temporary output string
					l_sOutString.clear();

					//! Clear the recevied Data Map
					l_ReceivedDataMap.clear();

					//! Start parsing of Input Data on New Line seperator
					while ((l_iEnd = l_sQueueData.find (l_sSeparator, l_iStart)) != std::string::npos)
					{
						l_sOutString.assign(l_sQueueData.substr(l_iStart, l_iEnd-l_iStart));
						if(l_sOutString.c_str()[1] == '=')
						{
							l_ReceivedDataMap.insert(pair<char, std::string> (l_sOutString.c_str()[0], l_sOutString.substr(2)));
						}

						l_iStart = l_iEnd + l_sSeparator.size();
					} //! end of While

					l_DataMapIter = l_ReceivedDataMap.find('s');
					if(l_DataMapIter == l_ReceivedDataMap.end())
					{
						Debug_msys("Invalid data packet, Session ID not sent");
						l_ReceivedDataMap.clear();
					}
					else
					{
						std::string l_sSessionName = "";
						for(l_DataMapIter = l_ReceivedDataMap.begin(); l_DataMapIter != l_ReceivedDataMap.end(); l_DataMapIter++)
						{
							if(l_DataMapIter->first == 's')
							{
								// Data found
								Debug_msys("Packet for session : "+l_DataMapIter->second);
	
								std::string l_sFileName = "";
								if(("Scheduler" != (m_GetComponentName())) && ("Logger" != (m_GetComponentName())) && ("QueueManager" != (m_GetComponentName())))
								{
									l_DataMapIter1 = l_ReceivedDataMap.find('f');
									l_sFileName = l_DataMapIter1->second;
								}

								//! Create new object of InputData MultiMap 
								l_InputDataMap = new DataMap;
					
								*l_InputDataMap = l_ReceivedDataMap;
								l_InputDataMap->erase('s');
								l_InputDataMap->insert(pair<char, std::string> ('s', l_DataMapIter->second));

								_m_SessionDataMapMtx.mb_lock();
								l_SessionDataMapIter = _m_SessionDataMap.find(_m_UtilityObjPtr->m_FromString<int>(l_DataMapIter->second));
								
								std::string l_sHomePath = "";

								l_sSessionName.clear();
								l_sHomePath.clear();
								if(l_SessionDataMapIter != _m_SessionDataMap.end() && l_SessionDataMapIter->second->m_GetThreadStatus())
								{
									l_sSessionName = l_SessionDataMapIter->second->m_GetSessionName();
									
									if(("Scheduler" != (m_GetComponentName())) && ("Logger_1" != (m_GetComponentName())) && ("QueueManager" != (m_GetComponentName())))
									{
										l_sHomePath	= _m_GlobalDataInfoPtr->m_GetHomePath();
										l_sHomePath.append("/");
										l_sHomePath.append("FWorkCheckpoint/");
										l_sHomePath.append(_m_UtilityObjPtr->m_ToString(l_SessionDataMapIter->second->m_GetSessionID()));
										l_sHomePath.append("/Dir1/");
										l_sHomePath.append(l_sFileName);

										
										FILE*			l_FilePtr;
										if(NULL != (l_FilePtr = fopen (l_sHomePath.c_str(),"w")))
										{
											fclose(l_FilePtr);										
										}
										l_sHomePath.clear();
									}

									//! Insert in Input Queue only if Thread status is running
									Debug_msys("Data Inserted in Session Queue");
									l_SessionDataMapIter->second->m_InputQueueObjPtr->m_WriteQueue(l_InputDataMap);
								}
								else
								{
									//! Session ID not present in Session Data Map
									Debug_msys("Session ID : "+l_DataMapIter->second+" not present/ not in active state in Session Map");
								}

								_m_SessionDataMapMtx.mb_unlock();
							}
						}
					}
				} //! If received Data is not empty
			} //! End of While Infinite Loop
		} //! End of Try
		catch(...)
		{
			Debug_msys("QueueReader Unknown exception");
			p_QueueReaderThreadInfoObjPtr->m_bThreadStatus = false;
			p_QueueReaderThreadInfoObjPtr->m_MoveForwardInterfaceObj.m_CanMoveForward(true);
		}
	} //! MFramework :: m_StartQueueReaderThread()


/******************************************************************************
 *  Starts the Queue Writer Thread
 *****************************************************************************/
	void MFramework :: m_StartQueueWriters()
	{
		//! Create ABL Statement
		ABL_Statement l_ABLStatementObj;

		ABL_ResultSet l_ABLResultSetObj;

		char l_cSelectOutputQueueQuery[1024];

		Debug_msys("Starting Queue Writers");

		QueueWriterThreadInfo* l_QueueWriterThreadInfoObjPtr;

		std::string l_sComponentType;

		_m_QueueWriterThreadInfoMap.clear();

		pthread_t l_QueueWriterThreadID;
		DataMap* l_QueueData;

		QueueWriterThreadInfoMap::iterator l_QueueWriterThreadInfoMapIter;
		DataMap::iterator l_DataMapIter;

		this->m_MoveForwardInterfaceObj.m_CanMoveForward();

		while(true)
		{
			if(0 == _m_GlobalDataInfoPtr->m_IsStopSystem())
			{
				Debug_msys("Stop signal received for Main QueueWriter thread, exiting");
				break;
			}

			l_QueueData = _m_GlobalDataInfoPtr->m_OPQueueInterfaceObjPtr->m_ReadQueue();

			if(NULL != l_QueueData)
			{
				l_sComponentType.clear();

				l_DataMapIter = l_QueueData->find('t');
				if(l_DataMapIter != l_QueueData->end())
				{
					//! ComponentType found in Map
					l_sComponentType.assign(l_DataMapIter->second);
				}
				
				if(!l_sComponentType.empty())
				{
					l_QueueWriterThreadInfoObjPtr = NULL;

					//! Check if the component type exists in QueueWriterThreadInfo Map
					l_QueueWriterThreadInfoMapIter = _m_QueueWriterThreadInfoMap.find(l_sComponentType);
					if(l_QueueWriterThreadInfoMapIter == _m_QueueWriterThreadInfoMap.end())
					{
						Debug_msys("Unknown component type '"+l_sComponentType+"' sent for QueueWriter, checking and setting up queues");

						Debug_msys("Setting up Output Queues to Component Type : "+l_sComponentType);
						
						while(true)
						{
							try
							{
								//! Get Output AMQP Queues from Database
								//! Get the Database connection
								ABL_Connection  l_ABLConnectionObj;
								l_ABLConnectionObj.mb_createConnection(_m_GlobalDataInfoPtr->m_ABLServiceObj);
								
								memset(l_cSelectOutputQueueQuery, '\0', 1024);
								sprintf(l_cSelectOutputQueueQuery, d_output_queue_query.c_str(), l_sComponentType.c_str());
								Debug_msys("m_StartQueueWriters :: Output Queue Select Query : "+_m_UtilityObjPtr->m_ToString(l_cSelectOutputQueueQuery));
								
								l_ABLStatementObj = l_ABLConnectionObj.mb_createStatement(l_cSelectOutputQueueQuery);

								l_ABLResultSetObj = l_ABLStatementObj.mb_executeQuery();
								
								l_QueueWriterThreadInfoObjPtr = new QueueWriterThreadInfo;
								l_QueueWriterThreadInfoObjPtr->m_sServingComponentName.assign(l_sComponentType.c_str());

								l_QueueWriterThreadInfoObjPtr->m_ComponentQueueLst.clear();
								while(l_ABLResultSetObj.mb_fetch())
								{
									//! Push the output queue names to the Component Queue List
									l_QueueWriterThreadInfoObjPtr->m_ComponentQueueLst.push_back(l_ABLResultSetObj.mb_getString(1).c_str());
								} //! End of While

								if(0 == l_QueueWriterThreadInfoObjPtr->m_ComponentQueueLst.size())
								{
									throw ABL_Exception(7001, "No output data queues configured for component type : "+l_sComponentType);
								}

								//! Close Result Set and Terminate the statement
								l_ABLConnectionObj.mb_terminateStatement(l_ABLStatementObj);

								break;
							}
							catch (ABL_Exception &e)
							{
								Debug_msys("Unable to get Output Queue List for Queue Writer, retrying after "\
									+_m_UtilityObjPtr->m_ToString(d_get_output_queue_list_interval)+" seconds");
							}

							//sleep(d_get_output_queue_list_interval);
						}

						//! Add in Queue Writer Threads Map
						_m_QueueWriterThreadInfoMap[l_sComponentType] = l_QueueWriterThreadInfoObjPtr;

						//! Start a thread to serve the Output Queue
						Debug_msys("Starting thread for Output Component Type : "+l_QueueWriterThreadInfoObjPtr->m_sServingComponentName);

						l_QueueWriterThreadInfoObjPtr->m_MoveForwardInterfaceObj.m_SetMoveForward();
						ExecuteInNewThread1(&l_QueueWriterThreadID, NULL, MFramework, *this, void, &MFramework::m_StartQueueWriterThread, QueueWriterThreadInfo*, l_QueueWriterThreadInfoObjPtr);
						l_QueueWriterThreadInfoObjPtr->m_MoveForwardInterfaceObj.m_CanMoveForward(false);
						l_QueueWriterThreadInfoObjPtr->m_ThreadID = l_QueueWriterThreadID;

						if(l_QueueWriterThreadInfoObjPtr->m_bThreadStatus)
						{
							Debug_msys("Thread started successfully for Output Component Type : "+l_QueueWriterThreadInfoObjPtr->m_sServingComponentName);
						}
						else
						{
							Debug_msys("Unable to start Thread for Output Component Type : "+l_QueueWriterThreadInfoObjPtr->m_sServingComponentName);
						}
					}
					else
					{
						l_QueueWriterThreadInfoObjPtr = l_QueueWriterThreadInfoMapIter->second;
					}
					

					//! Writer found, place the data on Queue
					l_QueueWriterThreadInfoObjPtr->m_QueueInterfaceObj.m_WriteQueue(l_QueueData);
					Debug_msys("Data successfully sent to Component Type "+l_sComponentType);
				}
				else
				{
					Debug_msys("Component Type missing for QueueWriter, invalid packet");
				}
			}

			usleep(2000);
		}
		
		for(QueueWriterThreadInfoMap::iterator l_QueueWriterThreadInfoMapIter = _m_QueueWriterThreadInfoMap.begin();
			l_QueueWriterThreadInfoMapIter != _m_QueueWriterThreadInfoMap.end();
			l_QueueWriterThreadInfoMapIter++)
		{
			l_QueueWriterThreadInfoObjPtr = l_QueueWriterThreadInfoMapIter->second;
			pthread_join(l_QueueWriterThreadInfoObjPtr->m_ThreadID, NULL);
		}

		Debug_msys("QueueWriter Threads exited gracefully");
	} //! MFramework :: m_StartQueueWriters()


/******************************************************************************
 *  Starts the Queue Writer Thread
 *****************************************************************************/	
	void MFramework :: m_StartQueueWriterThread(QueueWriterThreadInfo* p_QueueWriterThreadInfoObjPtr)
	{
		AMQPProducer* l_AMQPProducerObjPtr;
		//! Connect to the AMQP Producer using the information got on top
		AMQPProducerCreator l_AMQPProducerCreator;

		l_AMQPProducerCreator = _m_GlobalDataInfoPtr->m_SOContainerObjPtr->m_GetCreatorFunction<AMQPProducerCreator>(d_amqp_producer_so_name);

		p_QueueWriterThreadInfoObjPtr->m_OutputQueueProducerVec.clear();

		for(ComponentQueueLst::iterator l_ComponentQueueLstIter = p_QueueWriterThreadInfoObjPtr->m_ComponentQueueLst.begin();
			l_ComponentQueueLstIter != p_QueueWriterThreadInfoObjPtr->m_ComponentQueueLst.end();
			l_ComponentQueueLstIter++)
		{
			l_AMQPProducerObjPtr = l_AMQPProducerCreator(); //! Generate AMQP Producer object pointer using function pointer

			if(l_AMQPProducerObjPtr)
			{
				Broker* l_Broker;
				for(BrokersMap::iterator l_BrokersMapIter = _m_GlobalDataInfoPtr->m_BrokersMap.begin(); 
					l_BrokersMapIter != _m_GlobalDataInfoPtr->m_BrokersMap.end(); 
					l_BrokersMapIter++)
				{
					l_Broker = l_BrokersMapIter->second;

					if(l_AMQPProducerObjPtr->m_InitializeProducer(l_Broker->m_sBrokerIP, l_Broker->m_iPort, *l_ComponentQueueLstIter))
					{
						Debug_msys("QueueWriter is connected to AMQP Broker : "+l_Broker->m_sBrokerIP+" on Port : "+_m_UtilityObjPtr->m_ToString(l_Broker->m_iPort)+" for Queue "+(*l_ComponentQueueLstIter));
						break;
					}
					else
					{
						Debug_msys("QueueWriter is unable to connect to AMQP Broker : "+l_Broker->m_sBrokerIP+" on Port :"+_m_UtilityObjPtr->m_ToString(l_Broker->m_iPort)+" for Queue "+(*l_ComponentQueueLstIter));
					}
				}

				p_QueueWriterThreadInfoObjPtr->m_OutputQueueProducerVec.push_back(l_AMQPProducerObjPtr);
			}
		} // For

		p_QueueWriterThreadInfoObjPtr->m_bThreadStatus = true;
		p_QueueWriterThreadInfoObjPtr->m_MoveForwardInterfaceObj.m_CanMoveForward();
		
		DataMap* l_QueueData;
		DataMap::iterator l_DataMapIter;
		std::string l_sQueueData;
		int l_iRRIndexSize = p_QueueWriterThreadInfoObjPtr->m_OutputQueueProducerVec.size();
		int l_iRRIndex = 0;

		while(true)
		{
			if(0 == _m_GlobalDataInfoPtr->m_IsStopSystem())
			{
				Debug_msys("Stop signal received for QueueWriter thread, exiting");
				break;
			}

			l_QueueData = p_QueueWriterThreadInfoObjPtr->m_QueueInterfaceObj.m_ReadQueue(false);
			
			if(NULL != l_QueueData)
			{
				//! Prepare Output string
				l_sQueueData.clear();

				for(l_DataMapIter = l_QueueData->begin();
					l_DataMapIter != l_QueueData->end();
					l_DataMapIter++)
				{
					l_sQueueData.append(_m_UtilityObjPtr->m_ToString(l_DataMapIter->first)+"="+l_DataMapIter->second+"\n");
				}
				
				delete l_QueueData;
				l_QueueData = NULL;

				p_QueueWriterThreadInfoObjPtr->m_OutputQueueProducerVec[l_iRRIndex]->m_WriteIntoQ(l_sQueueData);
				l_iRRIndex++;

				if(l_iRRIndex == l_iRRIndexSize)
				{
					l_iRRIndex = 0;
				}
			}

			usleep(10000);
		}
	}

/******************************************************************************
 *  Starts the Control Thread
 *****************************************************************************/
	void MFramework :: m_StartControlThread()
	{
		Debug_msys("Control thread started");

		AMQPListener* l_AMQPListenerObjPtr;

		try
		{
			//! Connect to the AMQP Producer using the information got on top
			AMQPListenerCreator l_AMQPListenerCreator;
			l_AMQPListenerCreator = _m_GlobalDataInfoPtr->m_SOContainerObjPtr->m_GetCreatorFunction<AMQPListenerCreator>(d_amqp_listener_so_name);

			l_AMQPListenerObjPtr = l_AMQPListenerCreator(); //! Generate AMQP Producer object pointer using function pointer

			if(l_AMQPListenerObjPtr)
			{
				Broker* l_Broker;
				
				for(BrokersMap::iterator l_BrokersMapIter = _m_GlobalDataInfoPtr->m_BrokersMap.begin(); 
					l_BrokersMapIter != _m_GlobalDataInfoPtr->m_BrokersMap.end(); 
					l_BrokersMapIter++)
				{

					l_Broker = l_BrokersMapIter->second;

					if(l_AMQPListenerObjPtr->m_InitializeListener(l_Broker->m_sBrokerIP, l_Broker->m_iPort, this->m_GetControlQueueName(), 1))
					{						
						Debug_msys("m_StartControlThread is connected to AMQP Broker : "+l_Broker->m_sBrokerIP+" on Port : "+_m_UtilityObjPtr->m_ToString(l_Broker->m_iPort));
						break;
					}
					else
					{
						Debug_msys("m_StartControlThread is unable to connect to AMQP Broker : "+l_Broker->m_sBrokerIP+" on Port :"+_m_UtilityObjPtr->m_ToString(l_Broker->m_iPort));
						
					}
				}
								
			}
			else
			{
				throw ABL_Exception(1005, "m_StartControlThread is unable to get AMQP Listener object");
			}
			
			this->m_MoveForwardInterfaceObj.m_CanMoveForward();

			std::string l_sQueueData;
			DataMap l_InputDataMap;

			//! Temporary Variables used for Parsing Input Data Packet
			std::string l_sSeparator = "\n";
			std::string l_sOutString;
			int l_iStart = 0;
			int l_iEnd = 0;

			//! Iterator to find if Session ID sent is present in Loaded Sessions
			SessionDataMap::iterator l_SessionDataMapIter;
			
			int l_iSessionID;
			std::string l_sAction;

			bool l_bIsSessionFound;

			while(true)
			{
				if(0 == _m_GlobalDataInfoPtr->m_IsStopSystem())
				{
					Debug_msys("Stop signal received for Control thread, exiting");
					break;
				}

				//! Start Reading from Queue
				l_sQueueData = l_AMQPListenerObjPtr->m_ReadQueue();

				//! If Packet is not empty
				if(!l_sQueueData.empty())
				{
					//! Reset Session ID and Action
					l_iSessionID = -1;
					l_sAction.clear();

					//! Reset start and end positions for parsing
					l_iStart = 0;
					l_iEnd = 0;

					//! Clear the temporary output string
					l_sOutString.clear();

					//! Start parsing of Input Data
					while ((l_iEnd = l_sQueueData.find (l_sSeparator, l_iStart)) != std::string::npos)
					{
						l_sOutString.assign(l_sQueueData.substr(l_iStart, l_iEnd-l_iStart));
						if(l_sOutString.c_str()[0] == 's' && l_sOutString.c_str()[1] == '=')
						{
							l_iSessionID = _m_UtilityObjPtr->m_FromString<int>(l_sOutString.substr(2));
						}

						if(l_sOutString.c_str()[0] == 'a' && l_sOutString.c_str()[1] == '=')
						{
							l_sAction.assign(l_sOutString.substr(2));
						}

						l_iStart = l_iEnd + l_sSeparator.size();
					} //! end of While
					
					l_bIsSessionFound = false;
					
					if(l_sAction == "startsession")
					{
						this->m_HandleControlStart(l_iSessionID);
					}
					else
					{
						//! If Session ID is coming as 0 then action should be performed on all the
						//! sessions present in SessionDataMap and SessionLoadDetailsMap
						if(0 == l_iSessionID)
						{
							std::list<int> l_SessionIDList;

							l_SessionIDList.clear();

							_m_SessionDataMapMtx.mb_lock();
							for(l_SessionDataMapIter = _m_SessionDataMap.begin();
								l_SessionDataMapIter != _m_SessionDataMap.end();
								l_SessionDataMapIter++)
							{
								l_SessionIDList.push_back(l_SessionDataMapIter->first);
							}

							for(std::list<int>::iterator l_SessionIDListIter = l_SessionIDList.begin();
								l_SessionIDListIter != l_SessionIDList.end();
								l_SessionIDListIter++)
							{
								if(l_sAction == "stopsession")
								{
									this->m_HandleControlStop(*l_SessionIDListIter);
								}
								else if(l_sAction == "restartsession")
								{
									this->m_HandleControlStop(*l_SessionIDListIter);
									this->m_HandleControlStart(*l_SessionIDListIter);
								}
							}

							l_SessionIDList.clear();
							_m_SessionDataMapMtx.mb_unlock();
						}
						else
						{
							_m_SessionDataMapMtx.mb_lock();
							//! Check if the session id is present in _m_SessionDataMap
							l_SessionDataMapIter = _m_SessionDataMap.find(l_iSessionID);
							if(l_SessionDataMapIter != _m_SessionDataMap.end())
							{
								//! Insert in Input Queue only if Thread status is running
								l_bIsSessionFound = true;
							}
							_m_SessionDataMapMtx.mb_unlock();

							//! Check if packet has been sent for processing
							if(!l_bIsSessionFound)
							{
								Debug_msys("ControlThread :: Invalid data packet, Session ID not sent/ not present in SessionDataMap for action "+l_sAction);
							}
							else
							{
								if(l_sAction == "stopsession")
								{
									this->m_HandleControlStop(l_iSessionID);
								}
								else if(l_sAction == "restartsession")
								{
									this->m_HandleControlStop(l_iSessionID);
									this->m_HandleControlStart(l_iSessionID);
								}
							}
						}
					}
				} //! If received Data is not empty
			} //! End of While Infinite Loop
		} //! End of Try
		catch(...)
		{
			Debug_msys("ControlThread Unknown exception");

		}

		Debug_msys("Control Thread exited gracefully");
	} //! MFramework :: m_StartControlThread()


/******************************************************************************
 *  Handles New Action came in Control Message
 *****************************************************************************/
	void MFramework :: m_HandleControlStart(int p_iSessionID)
	{
		SessionDataMap::iterator l_SessionDataMapIter;
		l_SessionDataMapIter = _m_SessionDataMap.find(p_iSessionID);

		//! Check if the session id is present in _m_SessionDataMap
		if(l_SessionDataMapIter != _m_SessionDataMap.end())
		{
			if(l_SessionDataMapIter->second->m_GetThreadStatus())
			{
				Debug_msys("Session is already loaded and running, control signal to start new session will be ignored");

				return;
			}
			else
			{
				Debug_msys("Session is already loaded and but not running, restarting session with session id : "+_m_UtilityObjPtr->m_ToString(p_iSessionID));

				if(l_SessionDataMapIter->second != NULL)
				{
					delete l_SessionDataMapIter->second;
					l_SessionDataMapIter->second = NULL;
				}
			}
		}
		else
		{
			//! Load session details
			if(!m_GetSessionDetails(p_iSessionID))
			{
				Debug_msys("Invalid Session ID "+_m_UtilityObjPtr->m_ToString(p_iSessionID));
			}
		}

		//! Load and Make entry in Session Map

		//! Session creator function
		SessionCreator l_SessionCreator;

		//! Base Data pointer
		AbstractSessionBase* l_SessionBaseObjPtr;

		SessionLoadDetailsMap::iterator l_SessionLoadDetailsMapIter;

		l_SessionLoadDetailsMapIter = _m_SessionLoadDetailsMap.find(p_iSessionID);
		
		l_SessionCreator = \
		_m_GlobalDataInfoPtr->m_SOContainerObjPtr->m_GetCreatorFunction<SessionCreator>(l_SessionLoadDetailsMapIter->second->m_sSessionSOName);

		//! Get the session object
		l_SessionBaseObjPtr = NULL;
		l_SessionBaseObjPtr = l_SessionCreator(); //! Call the Creator function to get Session object
		if(NULL == l_SessionBaseObjPtr)
		{
			Debug_msys("Unable to create session object, invalid handle");
		}

		//! Add the session object to SessionDataMap
		//! Set Session ID before starting so that session SO knows which ID to load
		//! from its session table
		l_SessionBaseObjPtr->m_ClearLinkedSessionsLst();

		for(LinkedSessionsLst::iterator l_LinkedSessionsLstIter = l_SessionLoadDetailsMapIter->second->m_LinkedSessionsLst.begin();
			l_LinkedSessionsLstIter != l_SessionLoadDetailsMapIter->second->m_LinkedSessionsLst.end();
			l_LinkedSessionsLstIter++)
		{
			l_SessionBaseObjPtr->m_SetLinkedSessions(*l_LinkedSessionsLstIter);
		}

		l_SessionBaseObjPtr->m_SetSessionID(p_iSessionID);
		l_SessionBaseObjPtr->m_SetSessionName(l_SessionLoadDetailsMapIter->second->m_sSessionName);
		l_SessionBaseObjPtr->m_InputQueueObjPtr = new QueueInterface; //! Start the Queuing Interface

		_m_SessionDataMap[p_iSessionID] = l_SessionBaseObjPtr;

		pthread_t l_SessionThreadID;

		l_SessionBaseObjPtr->m_MoveForwardInterfaceObj.m_SetMoveForward(); //! Reset moveForwardSem
		ExecuteInNewThread1(&l_SessionThreadID, NULL, MFramework, *this, void, &MFramework::m_StartSessionThread, AbstractSessionBase*, l_SessionBaseObjPtr);
		l_SessionBaseObjPtr->m_MoveForwardInterfaceObj.m_CanMoveForward(false); //! Wait until moveForwardSem is released
		
		if(l_SessionBaseObjPtr->m_GetThreadStatus())
		{
			l_SessionBaseObjPtr->m_SetThreadID(l_SessionThreadID);
			_m_GlobalDataInfoPtr->m_UpdateThreadsRunning(l_SessionThreadID);

			Debug_msys("MFramework :: m_HandleControlStart : New Thread Started for Session ID "+_m_UtilityObjPtr->m_ToString(p_iSessionID));		
		}
		else
		{
			Debug_msys("MFramework :: m_HandleControlStart : Unable to start new thread for Session ID "+_m_UtilityObjPtr->m_ToString(p_iSessionID));
		}

		l_SessionBaseObjPtr = NULL;
	} //! MFramework :: m_HandleControlStart(int p_iSessionID)


/******************************************************************************
 *  Handles New Action came in Control Message
 *****************************************************************************/
	void MFramework :: m_HandleControlStop(int p_iSessionID)
	{
		try
		{
			SessionDataMap::iterator l_SessionDataMapIter;
			AbstractSessionBase* l_AbstractSessionBaseObjPtr;

			l_SessionDataMapIter = _m_SessionDataMap.find(p_iSessionID);

			//! Check if the session id is present in _m_SessionDataMap
			if(l_SessionDataMapIter == _m_SessionDataMap.end())
			{
				Debug_msys("Session ID "+_m_UtilityObjPtr->m_ToString(p_iSessionID)+" not present in SessionDataMap, control packet for stop will be ignored");
			}
			else
			{
				l_AbstractSessionBaseObjPtr = l_SessionDataMapIter->second;

				//! Delete entry from SessionLoadDetailsMap
				SessionLoadDetailsMap::iterator l_SessionLoadDetailsMapIter = _m_SessionLoadDetailsMap.find(p_iSessionID);

				delete l_SessionLoadDetailsMapIter->second;
				l_SessionLoadDetailsMapIter->second = NULL;
				_m_SessionLoadDetailsMap.erase(l_SessionLoadDetailsMapIter);

				if(l_AbstractSessionBaseObjPtr->m_GetThreadStatus())
				{
					l_AbstractSessionBaseObjPtr->m_MoveForwardInterfaceObj.m_SetMoveForward();
					l_AbstractSessionBaseObjPtr->m_StopThread();
					l_AbstractSessionBaseObjPtr->m_MoveForwardInterfaceObj.m_CanMoveForward(false);
				}

				//! Delete entry from RunningThreadsList
				_m_GlobalDataInfoPtr->m_UpdateThreadsRunning(l_AbstractSessionBaseObjPtr->m_GetThreadID(), false);

				//! Delete entry from SessionDataMap
				delete l_AbstractSessionBaseObjPtr;
				l_AbstractSessionBaseObjPtr = NULL;
				_m_SessionDataMap.erase(l_SessionDataMapIter);

				Debug_msys("-----------------------------------------------------------------------");
				Debug_msys("Running Thread List Size : "+_m_UtilityObjPtr->m_ToString(_m_GlobalDataInfoPtr->m_GetThreadsRunningSize()));
				Debug_msys("Session Data Map Size : "+_m_UtilityObjPtr->m_ToString(_m_SessionDataMap.size()));
				Debug_msys("Session Load Details Map Size : "+_m_UtilityObjPtr->m_ToString(_m_SessionLoadDetailsMap.size()));
				Debug_msys("-----------------------------------------------------------------------");
			}
		}
		catch(...)
		{
			Debug_msys("Unknown exception");
		}
	} //! MFramework :: m_HandleControlStop(int p_iSessionID)


/******************************************************************************
 *  Starts the Monitor Thread
 *****************************************************************************/
	void MFramework :: m_StartMonitoring()
	{
		Debug_msys("Started monitoring tool for all threads");

		this->m_MoveForwardInterfaceObj.m_CanMoveForward();
		
		while(true)
		{
		//	Debug_msys("Inside While Loop");
			if(0 == _m_GlobalDataInfoPtr->m_IsStopSystem())
			{
				Debug_msys("Stop signal received for Monitor thread, exiting");
				break;
			}
		//	Debug_msys("Inside While Loop1");
			if(0 == _m_SessionDataMap.size())
			{
				Debug_msys("There are no sessions defined for Monitoring");
			}
		//	Debug_msys("Inside While Loop2");
			//! Check if any thread has gone down, if yes then restart
			for(SessionDataMap::iterator l_SessionDataMapIter = _m_SessionDataMap.begin();
				l_SessionDataMapIter != _m_SessionDataMap.end();
				l_SessionDataMapIter++)
			{
		//		Debug_msys("Inside For Loop");
				if(!l_SessionDataMapIter->second->m_GetThreadStatus())
				{
					//! Some thread has stopped working, restart
					
//					l_SessionDataMapIter->second->m_SetThreadStatus(true);
//					this->m_HandleControlStop(l_SessionDataMapIter->first);
					this->m_HandleControlStart(l_SessionDataMapIter->first);
				}
		//		Debug_msys("Inside For Loop2");
			}

			sleep(1);
		}
	} //! MFramework :: m_StartMonitorThread()
} // Namespace MSYS

