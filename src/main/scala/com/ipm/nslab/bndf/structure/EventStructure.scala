package com.ipm.nslab.bndf.structure

/** Constructs the standard structure for events
 * @param EventValue The events values
 * @param Type The corresponding event type
 * @param Id The event Id which matches with signal time
 */
case class EventStructure(EventValue: Long, Type: String, Id: Long)
