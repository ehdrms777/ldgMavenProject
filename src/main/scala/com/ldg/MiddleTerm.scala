package com.ldg

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.Row

object MiddleTerm {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("hkProject").

      config("spark.master", "local").

      getOrCreate()


    // 2. 접속정보
    var staticUrl = "jdbc:postgresql://192.168.110.112:1521/orcl"

    var staticUser = "kopo"

    var staticPw = "kopo"

    var selloutDb = "kopo_channel_seasonality_new"

    // jdbc (java database connectivity) 연결

    val selloutDf = spark.read.format("jdbc").

      options(Map("url" -> staticUrl,"dbtable" -> selloutDb,"user" -> staticUser, "password" -> staticPw)).load

    // 메모리 테이블 생성

    selloutDf.createOrReplaceTempView("selloutTable")

    selloutDf.show(2)

    // 3. kopo_channel_seasonality_new의 qty값을 spark sql을 활용하여 1.2배 증가시킨 후 qty_new컬러을 생성하는 코드작성

    var middleResult=spark.sql("selectregionid,product,yearweek,qty*1.2asqty_new from selloutTable");


    // 4. 아래와 같이 getWeekInfo("201612") 호출 시 12라는 값이 INT가 리턴되도록 함수생성하시오

    //이클립스에서 UtilityFunction1 > src > com.dg > mathFunction.java에서
//    public static int getWeekInfo(String inputValue){
//      int result = Integer.parseInt(inputValue.substring(4));
//      return result;
//    }

    //넣어주고 export해서 f드라이브 jar, c드라이브 spark>jars에서 붙여넣고 스파크 껐다가 켜서 실행

//    import com.dg
//    com.dg.MathFunction
//    com.dg.MathFunction.getWeekInfo("201612")

    // 5. 2016년도 이상, 53주차 미포함, 프로덕트정보가(product1, product2)인 데이터만 남기는 프로그램 작성

    var rawData = middleResult
    var rawDataColumns = rawData.columns
    // 인덱스번호 주기
    var accountidNo = rawDataColumns.indexOf("regionid")

    var productNo = rawDataColumns.indexOf("product")

    var yearweekNo = rawDataColumns.indexOf("yearweek")

    var qtyNo = rawDataColumns.indexOf("qty")

    var productnameNo = rawDataColumns.indexOf("productname")

    // Rdd로 변환

    var rawRdd = rawData.rdd

    // 데이터 확인

    //    var {RDD변수명}.collect.foreach(println)

    //    rawRdd.collect.foreach(println)

    // var {RDD변수명} = {RDD변수명}.filter(x=>{ 필터조건식})
//
//    var rawExRdd = rawRdd.filter(x=>{
//
//      var checkValid = true;    //모든 데이터 true로 먼저 설정
//
//      // 설정 부적합 로직
//
//      if (x.getString(3).length !=6){     // yaerweek가 6자리가 아닌 경우 삭제!
//
//        checkValid = false;
//
//      }
//
//      checkValid
//
//    })

    //    PRODUCT1, PRODUCT2, 그리고 2016년이상인걸 찾겠다!

    var filteredRdd1 = rawRdd.filter(x=>{

      var checkValid = false;
      var yearValue = x.getString(yearweekNo).substring(4).toInt

      if ( (x.getString(productNo) == "PRODUCT1") &&

        (x.getString(productNo) == "PRODUCT2") &&

        (yearValue >= 2016)) {

        checkValid = true;

      }

      checkValid

    })

    //53주차 제거 로직

    var filteredRdd2 = rawRdd.filter(x=>{

      var checkValid = true;

      var weekValue = x.getString(yearweekNo).substring(4).toInt

      if (weekValue >=53){

        checkValid = false;

      }

      checkValid

    })



    // product1,2 인 정보만 필터링 1

    var testRdd = rawRdd.filter(x=>{

      var checkValid = false;

      if ( (x.getString(productNo)  == "PRODUCT1") ||

        (x.getString(productNo)  == "PRODUCT2") ){

        checkValid = true;

      }

      checkValid

    })



    // product1,2 인 정보만 필터링 2

    var productArray = Array("PRODUCT1","PRODUCT2")

    var productSet = productArray.toSet

    var resultRdd2 = testRdd.filter(x=>{

      var checkValid = false;

      var productInfo = x.getString(productNo);

      if (productSet.contains(productInfo)) {

        checkValid = true;

      }

      checkValid

    })



    // 데이터형 변환 [RDD → Dataframe]

    val finalResultDf = spark.createDataFrame(resultRdd2,

      StructType(

        Seq(

          StructField("KEY", StringType),

          StructField("REGIONID", StringType),

          StructField("PRODUCT", StringType),

          StructField("YEARWEEK", StringType),

          StructField("QTY", DoubleType),

          StructField("PRODUCTNAME", StringType))))

    // 6. 실습한 코드를 오라클 poly_server1(192.168.110.111)서버에 kopo_st_middle_ldg로 저장한 후 저장한 코드를 작성

    // data save

    // 데이터베이스 주소 및 접속정보 설정

    var myUrl = "jdbc:oracle:thin:@127.0.0.1:1521/XE"



    // 데이터 저장

    val prop = new java.util.Properties

    prop.setProperty("driver", "oracle.jdbc.OracleDriver")

    prop.setProperty("user", "kopo")

    prop.setProperty("password", "kopo")

    val table = "test1"

    //append

    finalResultDf.write.mode("overwrite").jdbc(myUrl, table, prop)



    // 파일저장

    finalResultDf.

      coalesce(1). // 파일개수

      write.format("csv").  // 저장포맷

      mode("overwrite"). // 저장모드 append/overwrite

      option("header", "true"). // 헤더 유/무

      save("d:/savedData2/kopo_st_middle_ldg.csv") // 저장파일명


    // 7. 저장한데이터로 시각화화기 x축은 yearweek정보, y축은 qty정보를 시각화해서 캡쳐


    // 8. 보너스문제
    //innerjoin, leftjoin

  }

}
