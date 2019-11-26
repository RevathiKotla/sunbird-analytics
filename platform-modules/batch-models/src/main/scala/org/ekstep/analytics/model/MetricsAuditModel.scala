package org.ekstep.analytics.model

import java.io.Serializable

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.ekstep.analytics.framework._
import org.ekstep.analytics.util.SecorMetricUtil



object MetricsAuditModel  extends IBatchModelTemplate[Empty, Empty, V3DerivedEvent, V3DerivedEvent] with Serializable {

    implicit val className = "org.ekstep.analytics.MetricsAuditJob"
    override def name: String = "MetricsAuditJob"

    override def preProcess(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[Empty] = {
        data
    }

    override def algorithm(data: RDD[Empty], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[V3DerivedEvent] = {
       val auditConfig=  config.get("auditConfig")
        val models  = auditConfig.get.asInstanceOf[Map[String,AnyRef]]
        SecorMetricUtil.getSecorMetrics(models.get("azure").get.asInstanceOf[Map[String,AnyRef]]);
    }

    override def postProcess(data: RDD[V3DerivedEvent], config: Map[String, AnyRef])(implicit sc: SparkContext): RDD[V3DerivedEvent] = {
        data
    }
}
