package com.ipm.nslab.bndf.spark.analysis

import org.apache.spark.ml.PipelineModel
import org.apache.spark.ml.clustering.GaussianMixtureModel

/** Provides the goodness of fit methods for validating the models
 *
 */
class GoodnessOfFit {

  /** Compute the Bayesian Information Criteria (BIC) for the likelihood based modeling
   * @param model The used model
   * @param st The corresponding stage number in the pipeline
   * @tparam Pipeline Ensuring the correct type for the pipeline
   * @return The BIC value
   */
  def BIC[Pipeline](model: PipelineModel, st: Int) :Double = {

    // Cluster member size
    val clusSize = model
      .stages(st)
      .asInstanceOf[GaussianMixtureModel]
      .summary
      .clusterSizes
    // Number of cluster
    val k = model
      .stages(st)
      .asInstanceOf[GaussianMixtureModel]
      .summary
      .k
    // Number of Total Observations
    val n = clusSize.sum
    // Dimension of features
    val d = model
      .stages(st)
      .asInstanceOf[GaussianMixtureModel]
      .gaussians(0)
      .mean
      .toArray
      .length
    // Log likelihood
    val loglik = model
      .stages(st)
      .asInstanceOf[GaussianMixtureModel]
      .summary
      .logLikelihood
    // Total number of params
    val totalParam = k * (((d*d)+d)/2) + (k * d) + (k - 1)
    // BIC
    val bic = totalParam * scala.math.log(n) - 2 * loglik

    bic

  }

}
