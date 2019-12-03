package org.ekstep.analytics.job

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.ekstep.analytics.util.ESUtil
import org.scalamock.scalatest.MockFactory
import org.sunbird.cloud.storage.conf.AppConf

import scala.collection.Map
import org.joda.time.DateTime
import org.joda.time.format.DateTimeFormat
import org.ekstep.analytics.job.report.ReportGenerator
import org.ekstep.analytics.job.report.AssessmentMetricsJob
import org.ekstep.analytics.job.report.BaseReportSpec
import org.ekstep.analytics.util.EmbeddedES
import scala.collection.mutable.Buffer
import org.ekstep.analytics.framework.FrameworkContext
import org.sunbird.cloud.storage.BaseStorageService

class TestAssessmentMetricsJob extends BaseReportSpec with MockFactory {
  
  implicit var spark: SparkSession = _
  
  var courseBatchDF: DataFrame = _
  var userCoursesDF: DataFrame = _
  var userDF: DataFrame = _
  var locationDF: DataFrame = _
  var orgDF: DataFrame = _
  var userOrgDF: DataFrame = _
  var externalIdentityDF: DataFrame = _
  var assessmentProfileDF: DataFrame = _
  var reporterMock: ReportGenerator = mock[ReportGenerator]
  val sunbirdCoursesKeyspace = "sunbird_courses"
  val sunbirdKeyspace = "sunbird"

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark = getSparkSession();

    /*
     * Data created with 31 active batch from batchid = 1000 - 1031
     * */
    courseBatchDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/courseBatchTable.csv")
      .cache()

    externalIdentityDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/usr_external_identity.csv")
      .cache()

    assessmentProfileDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/assessment.csv")
      .cache()


    /*
     * Data created with 35 participants mapped to only batch from 1001 - 1010 (10), so report
     * should be created for these 10 batch (1001 - 1010) and 34 participants (1 user is not active in the course)
     * and along with 5 existing users from 31-35 has been subscribed to another batch 1003-1007 also
     * */
    userCoursesDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/userCoursesTable.csv")
      .cache()

    /*
     * This has users 30 from user001 - user030
     * */
    userDF = spark
      .read
      .json("src/test/resources/assessment-metrics-updater/userTable.json")
      .cache()

    /*
     * This has 30 unique location
     * */
    locationDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/locationTable.csv")
      .cache()

    /*
     * There are 8 organisation added to the data, which can be mapped to `rootOrgId` in user table
     * and `organisationId` in userOrg table
     * */
    orgDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/orgTable.csv")
      .cache()

    /*
     * Each user is mapped to organisation table from any of 8 organisation
     * */
    userOrgDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/userOrgTable.csv")
      .cache()
      
     EmbeddedES.loadData("compositesearch", "cs", Buffer(
         """{"contentType":"SelfAssess","name":"My content 1","identifier":"do_112835335135993856149"}""",
         """{"contentType":"SelfAssess","name":"My content 2","identifier":"do_112835336280596480151"}""",
         """{"contentType":"SelfAssess","name":"My content 3","identifier":"do_112832394979106816112"}"""
     ))
  }
  
  "AssessmentMetricsJob" should "define all the configurations" in {
    
    assert(AppConf.getConfig("assessment.metrics.content.index").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.cassandra.input.consistency").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.cloud.objectKey").isEmpty === false)
    assert(AppConf.getConfig("cloud.container.reports").isEmpty === false)
    assert(AppConf.getConfig("assessment.metrics.temp.dir").isEmpty === false)
    assert(AppConf.getConfig("course.upload.reports.enabled").isEmpty === false)
    assert(AppConf.getConfig("course.es.index.enabled").isEmpty === false)
  }

  ignore should "Ensure CSV Report Should have all proper columns names and required columns values" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(assessmentProfileDF)

    val reportDF = AssessmentMetricsJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()
    val denormedDF = AssessmentMetricsJob.denormAssessment(reportDF)
    val finalReport = AssessmentMetricsJob.transposeDF(denormedDF)
    val column_names = finalReport.columns
    // Validate the column names are proper or not.
    assert(column_names.contains("External ID") === true)
    assert(column_names.contains("User ID") === true)
    assert(column_names.contains("User Name") === true)
    assert(column_names.contains("Email ID") === true)
    assert(column_names.contains("Mobile Number") === true)
    assert(column_names.contains("Organisation Name") === true)
    assert(column_names.contains("District Name") === true)
    assert(column_names.contains("School Name") === true)
    //assert(column_names.contains("Playingwithnumbers") === true)
    assert(column_names.contains("Whole Numbers") === true)
    assert(column_names.contains("Total Score") === true)
    assert(column_names.contains("Total Score") === true)

    // Check the proper total score is persent or not.
    //    val total_scoreList = finalReport.select("Total Score").collect().map(_(0)).toList
    //    assert(total_scoreList(0) === "“14.0/15.0”")

    // check the proper score is present or not for the each worksheet.
    val worksheet_score = finalReport.select("Playingwithnumbers").collect().map(_ (0)).toList
    //assert(worksheet_score(0) === "10")
    // check the proper school name or not

    val school_name = finalReport.select("School Name").collect().map(_ (0)).toList
    assert(school_name(0) === "SACRED HEART(B)PS,TIRUVARANGAM")

    // check the proper district name ro not
    val district_name = finalReport.select("District Name").collect().map(_ (0)).toList
    assert(district_name(0) === "GULBARGA")

    // check the proper User Name or not.
    val user_name = finalReport.select("User Name").collect().map(_ (0)).toList
    assert(user_name(0) === "Rayulu Verma")

    // check the proper userid or not.
    val user_id = finalReport.select("User ID").collect().map(_ (0)).toList
    assert(user_id(0) === "user015")

  }

  it should "generate reports" in {
    implicit val mockFc = mock[FrameworkContext];
    val mockStorageService = mock[BaseStorageService]
    (mockFc.getStorageService(_:String, _:String, _:String)).expects(*, *, *).returns(mockStorageService).anyNumberOfTimes();
    (mockStorageService.upload _).expects(*, *, *, *, *, *, *).returns("").anyNumberOfTimes();
    (mockStorageService.closeContext _).expects().returns().anyNumberOfTimes()
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(assessmentProfileDF)

    val reportDF = AssessmentMetricsJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()

    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val denormedDF = AssessmentMetricsJob.denormAssessment(reportDF)
    AssessmentMetricsJob.saveReport(denormedDF, tempDir)
    assert(denormedDF.count == 7)
  }

  it should "present 1001 batchid and do_2123101488779837441168 courseid " in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(assessmentProfileDF)

    val reportDF = AssessmentMetricsJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()

    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val denormedDF = AssessmentMetricsJob.denormAssessment(reportDF)
    val denormedDFCount = denormedDF.groupBy("courseid", "batchid")
    denormedDF.createOrReplaceTempView("course_batch")
    val df = spark.sql("select * from course_batch where batchid ='1001' and courseid='do_2123101488779837441168' and userid='user021' ")
    assert(df.count() === 2)
  }

  it should "fetch the content names from the elastic search" in {
    val contentESIndex = AppConf.getConfig("assessment.metrics.content.index")
    assert(contentESIndex.isEmpty === false)
    val contentList = List("do_112835335135993856149", "do_112835336280596480151")
    val contentDF = ESUtil.getAssessmentNames(spark, contentList, AppConf.getConfig("assessment.metrics.content.index"), AppConf.getConfig("assessment.metrics.supported.contenttype"))
    assert(contentDF.count() === 2)
  }


  it should "have computed total score for the specific userid, batch, course" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(assessmentProfileDF)

    val reportDF = AssessmentMetricsJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()

    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val denormedDF = AssessmentMetricsJob.denormAssessment(reportDF)
    val denormedDFCount = denormedDF.groupBy("courseid", "batchid")
    denormedDF.createOrReplaceTempView("course_batch")
    val df = spark.sql("select * from course_batch where batchid =1006 and courseid='do_1126458775024025601296' and  userid='user026' ")
    assert(df.count() === 1)
    val total_sum_scoreList = df.select("total_sum_score").collect().map(_ (0)).toList
    assert(total_sum_scoreList(0) === "10.0/30.0")

  }

  it should "valid score in the assessment content for the specific course" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(assessmentProfileDF)

    val reportDF = AssessmentMetricsJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()

    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val denormedDF = AssessmentMetricsJob.denormAssessment(reportDF)
    val denormedDFCount = denormedDF.groupBy("courseid", "batchid")
    denormedDF.createOrReplaceTempView("course_batch")
    val df = spark.sql("select * from course_batch where batchid ='1005' and courseid='do_112695422838472704115'")
    assert(df.count() === 1)
    val total_scoreList = df.select("grand_total").collect().map(_ (0)).toList
    assert(total_scoreList(0) === "4/4")
  }

  it should "confirm all required column names are present or not" in {
    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(assessmentProfileDF)

    val reportDF = AssessmentMetricsJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()

    val tempDir = AppConf.getConfig("assessment.metrics.temp.dir")
    val renamedDir = s"$tempDir/renamed"
    val denormedDF = AssessmentMetricsJob.denormAssessment(reportDF)
    assert(denormedDF.columns.contains("content_name") === true)
    assert(denormedDF.columns.contains("grand_total") === true)
    assert(denormedDF.columns.contains("courseid") === true)
    assert(denormedDF.columns.contains("userid") === true)
    assert(denormedDF.columns.contains("batchid") === true)
    assert(denormedDF.columns.contains("maskedemail") === true)
    assert(denormedDF.columns.contains("district_name") === true)
    assert(denormedDF.columns.contains("maskedphone") === true)
    assert(denormedDF.columns.contains("orgname_resolved") === true)
    assert(denormedDF.columns.contains("district_name") === true)
    assert(denormedDF.columns.contains("externalid") === true)
    assert(denormedDF.columns.contains("schoolname_resolved") === true)
    assert(denormedDF.columns.contains("username") === true)
  }

  it should "generate reports for the best score" in {

    assessmentProfileDF = spark
      .read
      .format("com.databricks.spark.csv")
      .option("header", "true")
      .load("src/test/resources/assessment-metrics-updater/assessment_best_score.csv")
      .cache()

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "course_batch", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(courseBatchDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_courses", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(userCoursesDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user", "keyspace" -> sunbirdKeyspace))
      .returning(userDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "user_org", "keyspace" -> sunbirdKeyspace))
      .returning(userOrgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "organisation", "keyspace" -> sunbirdKeyspace))
      .returning(orgDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "location", "keyspace" -> sunbirdKeyspace))
      .returning(locationDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "usr_external_identity", "keyspace" -> sunbirdKeyspace))
      .returning(externalIdentityDF)

    (reporterMock.loadData _)
      .expects(spark, Map("table" -> "assessment_aggregator", "keyspace" -> sunbirdCoursesKeyspace))
      .returning(assessmentProfileDF)

    val reportDF = AssessmentMetricsJob
      .prepareReport(spark, reporterMock.loadData)
      .cache()
    reportDF.createOrReplaceTempView("best_attempt")
    val df = spark.sql("select * from best_attempt where batchid ='1010' and courseid='do_1125559882615357441175' and content_id='do_112835335135993856149' and userid ='user030'")
    val best_attempt_score = df.select("total_score").collect().map(_ (0)).toList
    assert(best_attempt_score.head === "50")
  }

}