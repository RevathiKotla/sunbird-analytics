package org.ekstep.analytics.model


import org.ekstep.analytics.framework.util.JSONUtils
import org.ekstep.analytics.framework.{Dispatcher, Fetcher, JobConfig}
import org.ekstep.analytics.job.Metrics.MetricsAuditJob
import org.scalamock.scalatest.MockFactory

class TestMetricsAuditModel extends SparkSpec(null) with MockFactory{

  "TestMetricsAuditJob" should "get the metrics for monitoring the data pipeline" in {
    val auditConfig = "{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":[{\"name\":\"denorm\",\"search\":{\"type\":\"azure\",\"queries\":[{\"bucket\":\"dev-data-store\",\"prefix\":\"telemetry-denormalized/raw/\",\"startDate\":\"2019-12-02\",\"endDate\":\"2019-12-02\"}]},\"filters\":[{\"name\":\"flags.user_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.content_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.device_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.dialcode_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.collection_data_retrieved\",\"operator\":\"EQ\",\"value\":true},{\"name\":\"flags.derived_location_retrieved\",\"operator\":\"EQ\",\"value\":true}]},{\"name\":\"failed\",\"search\":{\"type\":\"azure\",\"queries\":[{\"bucket\":\"dev-data-store\",\"prefix\":\"failed/\",\"startDate\":\"2019-12-02\",\"endDate\":\"2019-12-02\"}]}},{\"name\":\"unique\",\"search\":{\"type\":\"azure\",\"queries\":[{\"bucket\":\"dev-data-store\",\"prefix\":\"unique/\",\"startDate\":\"2019-12-02\",\"endDate\":\"2019-12-02\"}]}},{\"name\":\"raw\",\"search\":{\"type\":\"azure\",\"queries\":[{\"bucket\":\"dev-data-store\",\"prefix\":\"raw/\",\"startDate\":\"2019-12-02\",\"endDate\":\"2019-12-02\"}]}},{\"name\":\"telemetry-count\",\"search\":{\"type\":\"druid\",\"druidQuery\":{\"queryType\":\"timeSeries\",\"dataSource\":\"telemetry-events\",\"intervals\":\"LastDay\",\"aggregations\":[{\"name\":\"total_count\",\"type\":\"count\",\"fieldName\":\"count\"}],\"descending\":\"false\"}}},{\"name\":\"summary-count\",\"search\":{\"type\":\"druid\",\"druidQuery\":{\"queryType\":\"timeSeries\",\"dataSource\":\"summary-events\",\"intervals\":\"LastDay\",\"aggregations\":[{\"name\":\"total_count\",\"type\":\"count\",\"fieldName\":\"count\"}],\"descending\":\"false\"}}}]},\"output\":[{\"to\":\"file\",\"params\":{\"file\":\"/src/test/resources\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}"
    val config = JSONUtils.deserialize[JobConfig](auditConfig)
    val reportConfig = JobConfig(Fetcher("none", None, None), null, null, "org.ekstep.analytics.model.MetricsAuditModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("MetricsAuditModel"))
    val pipelineMetrics = MetricsAuditModel.execute(sc.emptyRDD, config.modelParams)
  }

  it should "execute MetricsAudit job and won't throw any Exception" in {
    val configString ="{\"search\":{\"type\":\"none\"},\"model\":\"org.ekstep.analytics.model.MetricsAuditJob\",\"modelParams\":{\"auditConfig\":[{\"name\":\"denorm\",\"search\":{\"type\":\"azure\",\"queries\":[{\"bucket\":\"dev-data-store\",\"prefix\":\"telemetry-denormalized/raw/\",\"startDate\":\"2019-11-06\",\"endDate\":\"2019-11-06\"}]},\"filters\":[{\"name\":\"flags.user_data_retrieved\",\"operator\":\"EQ\",\"value\":true}]},{\"name\":\"failed\",\"search\":{\"type\":\"azure\",\"queries\":[{\"bucket\":\"dev-data-store\",\"prefix\":\"failed/\",\"startDate\":\"2019-11-06\",\"endDate\":\"2019-11-06\"}]}},{\"name\":\"telemetry-count\",\"search\":{\"type\":\"druid\",\"druidQuery\":{\"queryType\":\"timeSeries\",\"dataSource\":\"telemetry-events\",\"intervals\":\"LastDay\",\"aggregations\":[{\"name\":\"total_count\",\"type\":\"count\",\"fieldName\":\"\"}],\"descending\":\"false\"}}}]},\"output\":[{\"to\":\"kafka\",\"params\":{\"brokerList\":\"localhost:9092\",\"topic\":\"metrics.topic\"}}],\"parallelization\":8,\"appName\":\"Metrics Audit\"}"

    val config_1= JSONUtils.deserialize[JobConfig](configString);
    val config = JobConfig(Fetcher("none", None, None), null, null, "org.ekstep.analytics.model.ExperimentDefinitionModel", None, Option(Array(Dispatcher("console", Map("printEvent" -> false.asInstanceOf[AnyRef])))), Option(10), Option("TestExperimentDefinitionJob"))
    MetricsAuditJob.main(configString)(Option(sc));
  }
}
