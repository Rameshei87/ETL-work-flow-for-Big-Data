/*******************************************************************************
 * @file    Protocols.cpp
 * @brief   Contains virtual functions like connect/download/upload etc for
 *			different protocol types
 * @todo
 ******************************************************************************/

/******************************************************************************
 *   includes:
 *    - system includes
 *    - application includes
 *****************************************************************************/

#include <Debug.h>
#include <ProtocolFactory.h>

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

/*****************************************************************************
 * Function to display output statements on console
 ****************************************************************************/

/******************************************************************************
 *   global functions:
 *****************************************************************************/

/******************************************************************************
 *   constructor/destructor:
 *****************************************************************************/

/******************************************************************************
 *   private functions:
 *****************************************************************************/

/******************************************************************************
 *   initializer/finalizer
 *****************************************************************************/

/******************************************************************************
 *   protected functions:
 *****************************************************************************/

/******************************************************************************
 *   public functions:
 *****************************************************************************/
ProtocolFactory :: ProtocolFactory (SOContainer *p_SOContainerObjPtr)
{
	_m_SOContainerObjPtr = p_SOContainerObjPtr;
}

ProtocolFactory :: ~ProtocolFactory()
{
		
}

/******************************************************************************
 *   Used to register protocol type
 *****************************************************************************/
void ProtocolFactory :: m_RegisterProtocol(std::string p_sProtocolType, std::string p_sProtocolLibrary)
{
	ProtocolCreator l_ProtocolCreator;

	if (_m_FactoryMap.end () == _m_FactoryMap.find(p_sProtocolType))
	{
		try
		{
			//! Get the protocol create function pointer
			Debug_msys("Protocol type '"+p_sProtocolType+"' is loaded in ProtocolFactory");
			l_ProtocolCreator = _m_SOContainerObjPtr->m_GetCreatorFunction<ProtocolCreator>(p_sProtocolLibrary);
			_m_FactoryMap[p_sProtocolType] = l_ProtocolCreator;
		}
		catch(ABL_Exception &e)
		{
			throw e;
		}
	}
}

/******************************************************************************
 *   Used to get protocol type
 *****************************************************************************/
ProtocolConnection* ProtocolFactory :: m_GetProtocol(std::string p_sProtocolType)
{
	ProtocolMap::iterator l_itMapIter;

	//! Find if protocol exists in Map
	l_itMapIter = _m_FactoryMap.find(p_sProtocolType);

	if (l_itMapIter == _m_FactoryMap.end ())
	{
		//! Connection could not be found
		throw ABL_Exception(9000, "Not a registered protocol type : "+p_sProtocolType);
	}
	else
	{
		//! Return Type ProtocolConnection Pointer
		return (*l_itMapIter->second)();
	}
}

