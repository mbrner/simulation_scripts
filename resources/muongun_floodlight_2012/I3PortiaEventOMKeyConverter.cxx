/**
 * copyright  (C) 2004
 * the icecube collaboration
 * $Id: I3PortiaEventOMKeyConverter.cxx 19641 2006-05-10 14:03:22Z dule $
 *
 * @file I3PortiaEventOMKeyConverter.cxx
 * @version $Revision: 1.7 $
 * @date $Date: 2006-05-10 23:03:22 +0900 (水, 10  5月 2006) $
 * @author mase
 */

#include "icetray/I3TrayHeaders.h"
#include "portia/I3PortiaEvent.h"
#include "portia/I3PortiaEventOMKeyConverter.h"
#include "dataclasses/I3Vector.h"

#include <iostream>

using namespace std;

I3_MODULE(I3PortiaEventOMKeyConverter);


/*********************************************************************/
/* Constructor                                                       */ 
/*********************************************************************/
I3PortiaEventOMKeyConverter::I3PortiaEventOMKeyConverter(const I3Context& ctx) : I3ConditionalModule(ctx)
{
  AddOutBox("OutBox");

  inputPortiaEvent_ = "EHEPortiaEventSummary";
  AddParameter("InputPortiaEventName", 
	       "Input name of I3PortiaEvent", inputPortiaEvent_);

  outputOMKeyListName_ = "LargestOMKey";
  AddParameter("OutputOMKeyListName",
	       "Output name of OMKey list", outputOMKeyListName_);
}


/**********************************************************************/
/* Destructor                                                         */
/**********************************************************************/
I3PortiaEventOMKeyConverter::~I3PortiaEventOMKeyConverter(){
}



/**********************************************************************/
/* Configure                                                          */
/**********************************************************************/
void I3PortiaEventOMKeyConverter::Configure()
{
  GetParameter("InputPortiaEventName", inputPortiaEvent_);
  GetParameter("OutputOMKeyListName", outputOMKeyListName_);

  log_info ("Input:  InputPortiaEventName = %s",inputPortiaEvent_.c_str());
  log_info ("Output: OutputOMKeyListName = %s",outputOMKeyListName_.c_str());

}


/* ******************************************************************** */
/* Physics                                                              */
/* ******************************************************************** */
void I3PortiaEventOMKeyConverter::Physics(I3FramePtr frame)
{
  log_debug("Entering Physics...");

  // Get I3OpheliaFirstGuessTrack info from the frame.
  I3PortiaEventConstPtr portiaEvent_ptr
    = frame->Get<I3PortiaEventConstPtr>(inputPortiaEvent_);

  if (!portiaEvent_ptr){
      log_warn("Couldn't find input I3PortiaEvent.");
  }else{
    OMKey omkey = portiaEvent_ptr->GetLargestNPEOMKey();
    log_debug("Largest OM (%d, %d)", omkey.GetString(), omkey.GetOM());

    OMKeyPtr omkeyPtr(new OMKey);
    omkeyPtr->SetString(omkey.GetString());
    omkeyPtr->SetOM(omkey.GetOM());

    /*I3VectorOMKeyPtr omkeyListPtr;*/
    I3VectorOMKeyPtr omkeyListPtr(new I3VectorOMKey());
    omkeyListPtr->push_back(*omkeyPtr);

    // Put it to the frame
    frame->Put(outputOMKeyListName_, omkeyListPtr);
  }
  
  PushFrame(frame,"OutBox");

  log_debug("Exiting Physics.");
}
