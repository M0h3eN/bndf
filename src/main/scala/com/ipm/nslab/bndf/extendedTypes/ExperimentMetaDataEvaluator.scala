package com.ipm.nslab.bndf.extendedTypes

/** Specifies the list of of non-processed experiments
 * @param nonExistenceExperiments The experiments which their corresponding meta-data information is
 *                                not available in MongoDB
 */
case class ExperimentMetaDataEvaluator(nonExistenceExperiments: Array[String])
