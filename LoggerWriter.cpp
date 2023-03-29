/*******************************************************************************
 * @file    LoggerWriter.cpp
 * @brief   Contains defination of MFramework Class
 * @todo
 ******************************************************************************/

/******************************************************************************
 *   includes:
 *    - system includes
 *    - application includes
 *****************************************************************************/
#include <CommonDefs.h>
#include <Utility.h>

#include <AMQPProducer.h>

#include <ABL_Connection.h>
#include <ABL_Statement.h>
#include <ABL_ResultSet.h>

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
	LoggerWriter :: LoggerWriter()
	{
		_m_iGlobalLogLevel = 64;
		_m_sComponentName.clear();
		_m_AMQPProducerObjPtr = NULL;
		_m_LoggerMtx.mb_unlock();
	}

	LoggerWriter :: ~LoggerWriter()
	{
		if(_m_AMQPProducerObjPtr)
		{
			_m_AMQPProducerObjPtr->m_Close();
			delete _m_AMQPProducerObjPtr;
			_m_AMQPProducerObjPtr = NULL;
		}
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
 *  Returns true if LoggerWriter gets initialized successfully 
 *****************************************************************************/
 #define SELECT_QUEUE   "SELECT NAME FROM M_QUEUE WHERE TYPE = 'LOGGER_DATA'"

	void LoggerWriter :: m_InitLoggerWriter(int p_iGlobalLogLevel, std::string p_sComponentName, GlobalDataInfo* p_GDIObjPtr)
	{
		
		//! Assign Log Level and Component Name
		_m_iGlobalLogLevel = p_iGlobalLogLevel;
		_m_sComponentName.assign(p_sComponentName);
		_m_sHostIP.assign(p_GDIObjPtr->m_UtilityObjPtr->m_GetIP());

		_m_GDIObjPtr = p_GDIObjPtr;

		// Read from DB and fill in LoggerWriterData object
		ABL_Connection      l_ABLConnectionObj;
		ABL_Statement       l_ABLStatementObj;
		ABL_ResultSet       l_ABLResultSetObj;
		std::string         l_sSQLQuery;

		try
		{
			l_ABLConnectionObj.mb_createConnection(_m_GDIObjPtr->m_ABLServiceObj);

			// Query to get the details from the Database
			l_sSQLQuery.assign(SELECT_QUEUE);
		
			// Create SQL query using ABL API
			l_ABLStatementObj   = l_ABLConnectionObj.mb_createStatement(l_sSQLQuery.c_str());

			// Execute SQL Query using ABL API
			l_ABLResultSetObj   = l_ABLStatementObj.mb_executeQuery();

			//! Fetching LoggerWriter Details
			while(l_ABLResultSetObj.mb_fetch())
			{
				_m_sQueueName = l_ABLResultSetObj.mb_getString(1).mb_getSTLString();
			}
			
			//! Close result set
			l_ABLStatementObj.mb_closeResultSet(l_ABLResultSetObj);

			//! Terminate the statement
			l_ABLConnectionObj.mb_terminateStatement(l_ABLStatementObj);

			//! Connect to the AMQP Producer using the information got on top
			AMQPProducerCreator l_AMQPProducerCreator;
			l_AMQPProducerCreator = _m_GDIObjPtr->m_SOContainerObjPtr->m_GetCreatorFunction<AMQPProducerCreator>(d_amqp_producer_so_name);

			_m_AMQPProducerObjPtr = l_AMQPProducerCreator(); //! Generate AMQP Producer object pointer using function pointer

			if(_m_AMQPProducerObjPtr)
			{
				Broker* l_Broker;
				for(BrokersMap::iterator l_BrokersMapIter = _m_GDIObjPtr->m_BrokersMap.begin(); 
					l_BrokersMapIter != _m_GDIObjPtr->m_BrokersMap.end(); 
					l_BrokersMapIter++)
				{
					l_Broker = l_BrokersMapIter->second;

					if(_m_AMQPProducerObjPtr->m_InitializeProducer(l_Broker->m_sBrokerIP, l_Broker->m_iPort, _m_sQueueName))
					{
						Debug_msys("LoggerWriter is connected to AMQP Broker : "+l_Broker->m_sBrokerIP+" on Port : "+_m_GDIObjPtr->m_UtilityObjPtr->m_ToString(l_Broker->m_iPort));
						break;
					}
					else
					{
						Debug_msys("LoggerWriter is unable to connect to AMQP Broker : "+l_Broker->m_sBrokerIP+" on Port :"+_m_GDIObjPtr->m_UtilityObjPtr->m_ToString(l_Broker->m_iPort));
					}
				}
			}
			else
			{
				throw ABL_Exception(1005, "Unable to get AMQP Producer object");
			}
		}
		catch(ABL_Exception &e)
		{
			throw ABL_Exception(e.mb_getErrorCode(), e.mb_getMessage());
		}
	} //! LoggerWriter :: m_InitLoggerWriter

/******************************************************************************
 *  Returns true if Logging of the sent message is successful 
 *****************************************************************************/
	bool LoggerWriter :: m_LogMessage(std::string p_sSessionName, int p_iLogLevel, std::string p_sLogMessage)
	{
		std::string l_sPreparedLogMsg;

		_m_LoggerMtx.mb_lock();

		//! Check for Log Level
		if((_m_iGlobalLogLevel & p_iLogLevel))
		{
			std::string l_sLogLevelDesc;

			switch(p_iLogLevel)
			{
			case d_log_level_info:
				l_sLogLevelDesc.assign("I");
				break;
			case d_log_level_debug:
				l_sLogLevelDesc.assign("D");
				break;
			case d_log_level_warning:
				l_sLogLevelDesc.assign("W");
				break;
			case d_log_level_critical:
				l_sLogLevelDesc.assign("C");
				break;
			case d_log_level_error:
				l_sLogLevelDesc.assign("E");
				break;
			case d_log_level_alert:
				l_sLogLevelDesc.assign("A");
				break;
			default:
				l_sLogLevelDesc.assign("N");
				break;
			}

			//! Yes, the message needs to be Logged
			l_sPreparedLogMsg.assign("s=1\n");
			l_sPreparedLogMsg.append("c="+_m_sComponentName+"\n");
			l_sPreparedLogMsg.append("l="+l_sLogLevelDesc+"\n");
			l_sPreparedLogMsg.append("m="+_m_GDIObjPtr->m_UtilityObjPtr->m_GetCurrentLogTime()+"|"); //+","+_m_sHostIP+",");
			l_sPreparedLogMsg.append(p_sSessionName+"|");
			l_sPreparedLogMsg.append(p_sLogMessage+"\n");
		

			_m_AMQPProducerObjPtr->m_WriteIntoQ(l_sPreparedLogMsg);

			l_sPreparedLogMsg.clear();
		}

		_m_LoggerMtx.mb_unlock();

		return true;
	}
} // Namespace MSYS

