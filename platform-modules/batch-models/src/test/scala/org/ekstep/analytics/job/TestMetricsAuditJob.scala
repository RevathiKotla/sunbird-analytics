package org.ekstep.analytics.job

import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig, Query}
import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.model.SparkSpec

class TestMetricsAuditJob extends SparkSpec(null) {

        "TestMetricsAuditJob" should "execute MetricsAudit job and won't throw any Exception" in {

               val configString ="{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":{\"druid\":{\"queries\":[{\"name\":\"pipeline\",\"query\":\"druid.pipeline_metrics.audit.query\"},{\"name\":\"pipeline\",\"query\":\"druid.pipeline_metrics.audit.query\"}]},\"azure\":{\"models\":{\"channel\":{\"search\":{\"type\":\"azure\",\"queries\":[{\"bucket\":\"dev-data-store\",\"prefix\":\"raw/\",\"startDate\":\"2019-11-06\",\"endDate\":\"2019-11-06\"}]}},\"denorm\":{\"search\":{\"type\":\"azure\",\"queries\":[{\"bucket\":\"dev-data-store\",\"prefix\":\"telemetry-denormalized/raw/\",\"startDate\":\"2019-11-06\",\"endDate\":\"2019-11-06\"}]}},\"failed\":{\"search\":{\"type\":\"azure\",\"queries\":[{\"bucket\":\"dev-data-store\",\"prefix\":\"failed/\",\"startDate\":\"2019-11-06\",\"endDate\":\"2019-11-06\"}]}}}}}},\"output\":[{\"to\":\"kafka\",\"params\":{\"brokerList\":\"localhost:9092\",\"topic\":\"metrics.topic\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}";

       val config_1= JSONUtils.deserialize[JobConfig](configString);
        val config = JobConfig(Fetcher("none", None, None), null, null, "org.ekstep.analytics.model.ExperimentDefinitionModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestExperimentDefinitionJob"))
        MetricsAuditJob.main(configString)(Option(sc));
    }
}