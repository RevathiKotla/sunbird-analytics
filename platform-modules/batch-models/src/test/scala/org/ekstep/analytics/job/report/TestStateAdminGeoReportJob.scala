package org.ekstep.analytics.job

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.model.SparkSpec
import org.scalamock.scalatest.MockFactory
import org.ekstep.analytics.job.report.{BaseReportsJob, StateAdminGeoReportJob}
import org.ekstep.analytics.util.EmbeddedCassandra

class TestStateAdminGeoReportJob extends SparkSpec(null) with MockFactory {

  implicit var spark: SparkSession = _
  var map: Map[String, String] = _
  var shadowUserDF: DataFrame = _
  var orgDF: DataFrame = _
  var reporterMock: BaseReportsJob = mock[BaseReportsJob]
  val sunbirdKeyspace = "sunbird"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession()
    EmbeddedCassandra.loadData("src/test/resources/reports/reports_test_data.cql") // Load test data in embedded cassandra server
  }

  "StateAdminGeoReportJob" should "generate reports" in {
    val reportDF = StateAdminGeoReportJob.generateGeoReport()(spark)
    assert(reportDF.count() === 6)
    //for geo report we expect these columns
    assert(reportDF.columns.contains("School id") === true)
    assert(reportDF.columns.contains("School name") === true)
    assert(reportDF.columns.contains("Channel") === true)
    assert(reportDF.columns.contains("District id") === true)
    assert(reportDF.columns.contains("District name") === true)
    assert(reportDF.columns.contains("Block id") === true)
    assert(reportDF.columns.contains("Block name") === true)
    assert(reportDF.columns.contains("slug") === true)
    assert(reportDF.columns.contains("externalid") === true)
    val apslug = reportDF.where(col("slug") === "ApSlug")
    val school_name = apslug.select("School name").collect().map(_ (0)).toList
    assert(school_name(0) === "MPPS SIMHACHALNAGAR")
    assert(school_name(1) === "Another school")
    assert(reportDF.select("District id").distinct().count == 4)
  }
}