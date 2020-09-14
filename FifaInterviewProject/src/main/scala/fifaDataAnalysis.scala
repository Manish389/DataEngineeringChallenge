


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.storage.StorageLevel

import scala.collection.Seq

case class fifaData(SL_NO: Int,
                    ID: Int,
                    NAME: String,
                    AGE: Int,
                    PHOTO: String,
                    NATIONALITY: String,
                    FLAG: String,
                    OVERALL: Int,
                    POTENTIAL: Int,
                    CLUB: String,
                    CLUB_LOGO: String,
                    VALUE: String,
                    WAGE: String,
                    SPECIAL: Int,
                    PREFERRED_FOOT: String,
                    INTERNATIONAL_REPUTATION: Int,
                    WEAK_FOOT: Int,
                    SKILL_MOVES: Int,
                    WORK_RATE: String,
                    BODY_TYPE: String,
                    REAL_FACE: String,
                    POSITION: String,
                    JERSEY_NUMBER: Int,
                    JOINED: String,
                    LOANED_FROM: String,
                    CONTRACT_VALID_UNTIL: String,
                    HEIGHT: String,
                    WEIGHT: String,
                    LS: String,
                    ST: String,
                    RS: String,
                    LW: String,
                    LF: String,
                    CF: String,
                    RF: String,
                    RW: String,
                    LAM: String,
                    CAM: String,
                    RAM: String,
                    LM: String,
                    LCM: String,
                    CM: String,
                    RCM: String,
                    RM: String,
                    LWB: String,
                    LDM: String,
                    CDM: String,
                    RDM: String,
                    RWB: String,
                    LB: String,
                    LCB: String,
                    CB: String,
                    RCB: String,
                    RB: String,
                    CROSSING: Int,
                    FINISHING: Int,
                    HEADINGACCURACY: Int,
                    SHORTPASSING: Int,
                    VOLLEYS: Int,
                    DRIBBLING: Int,
                    CURVE: Int,
                    FKACCURACY: Int,
                    LONGPASSING: Int,
                    BALLCONTROL: Int,
                    ACCELERATION: Int,
                    SPRINTSPEED: Int,
                    AGILITY: Int,
                    REACTIONS: Int,
                    BALANCE: Int,
                    SHOTPOWER: Int,
                    JUMPING: Int,
                    STAMINA: Int,
                    STRENGTH: Int,
                    LONGSHOTS: Int,
                    AGGRESSION: Int,
                    INTERCEPTIONS: Int,
                    POSITIONING: Int,
                    VISION: Int,
                    PENALTIES: Int,
                    COMPOSURE: Int,
                    MARKING: Int,
                    STANDINGTACKLE: Int,
                    SLIDINGTACKLE: Int,
                    GKDIVING: Int,
                    GKHANDLING: Int,
                    GKKICKING: Int,
                    GKPOSITIONING: Int,
                    GKREFLEXES: Int,
                    RELEASE_CLAUSE: String
                   )

object fifaDataAnalysis extends App {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
  val spark = SparkSession.builder().appName("Proj").master(args(0)).getOrCreate()


  import spark.implicits._

  println("--------------- DATA LOAD STARTED --------------------------  ")
  val cleaningFifaDailyData = spark.read.option("endian", "little").option("encoding", "UTF-8")
    .option("header", true)
    .option("inferSchema", true)
    .csv(args(1))
    .na.fill(0, Seq("ID", "Age", "Overall", "Potential", "Special", "International Reputation", "Weak Foot", "Skill Moves", "Jersey Number", "Crossing", "Finishing", "HeadingAccuracy", "ShortPassing", "Volleys", "Dribbling", "Curve", "FKAccuracy", "LongPassing", "BallControl", "Acceleration", "SprintSpeed", "Agility", "Reactions", "Balance", "ShotPower", "Jumping", "Stamina", "Strength", "LongShots", "Aggression", "Interceptions", "Positioning", "Vision", "Penalties", "Composure", "Marking", "StandingTackle", "SlidingTackle", "GKDiving", "GKHandling", "GKKicking", "GKPositioning", "GKReflexes"))
    .na.fill("NA", Seq("Name", "Photo", "Nationality", "Flag", "Club", "Club Logo", "Value", "Wage", "Preferred Foot", "Work Rate", "Body Type", "Real Face", "Position", "Joined", "Loaned From", "Contract Valid Until", "Height", "Weight", "LS", "ST", "RS", "LW", "LF", "CF", "RF", "RW", "LAM", "CAM", "RAM", "LM", "LCM", "CM", "RCM", "RM", "LWB", "LDM", "CDM", "RDM", "RWB", "LB", "LCB", "CB", "RCB", "RB", "Release Clause"))
    .withColumnRenamed("_c0", "sl_no")
    .filter(col("sl_no").isNotNull)
    .toDF("SL_NO", "ID", "NAME", "AGE", "PHOTO", "NATIONALITY", "FLAG", "OVERALL", "POTENTIAL", "CLUB", "CLUB_LOGO", "VALUE", "WAGE", "SPECIAL", "PREFERRED_FOOT", "INTERNATIONAL_REPUTATION", "WEAK_FOOT", "SKILL_MOVES", "WORK_RATE", "BODY_TYPE", "REAL_FACE", "POSITION", "JERSEY_NUMBER", "JOINED", "LOANED_FROM", "CONTRACT_VALID_UNTIL", "HEIGHT", "WEIGHT", "LS", "ST", "RS", "LW", "LF", "CF", "RF", "RW", "LAM", "CAM", "RAM", "LM", "LCM", "CM", "RCM", "RM", "LWB", "LDM", "CDM", "RDM", "RWB", "LB", "LCB", "CB", "RCB", "RB", "CROSSING", "FINISHING", "HEADINGACCURACY", "SHORTPASSING", "VOLLEYS", "DRIBBLING", "CURVE", "FKACCURACY", "LONGPASSING", "BALLCONTROL", "ACCELERATION", "SPRINTSPEED", "AGILITY", "REACTIONS", "BALANCE", "SHOTPOWER", "JUMPING", "STAMINA", "STRENGTH", "LONGSHOTS", "AGGRESSION", "INTERCEPTIONS", "POSITIONING", "VISION", "PENALTIES", "COMPOSURE", "MARKING", "STANDINGTACKLE", "SLIDINGTACKLE", "GKDIVING", "GKHANDLING", "GKKICKING", "GKPOSITIONING", "GKREFLEXES", "RELEASE_CLAUSE")
    .as[fifaData]
  val dateFormat = "yyyyMMdd_HHmm"
  val dateValue = spark.range(1).select(date_format(current_timestamp, dateFormat)).as[(String)].first
  val fileName = "FifaDelta_" + dateValue


  if (cleaningFifaDailyData.count() > 0) {

    cleaningFifaDailyData.write.option("Header", "true").csv(args(2) + "/" + s"$fileName")
  }
  else {
    print("Delta Data not present")

  }
  println("--------------- DATA LOAD COMPLETE --------------------------  ")
  cleaningFifaDailyData.persist(StorageLevel.MEMORY_AND_DISK)

  println("-------------------------------------------------------------------------------------  ")
  println("2a. Which club has the most number of left footed midfielders under 30 years of age?")
  println("-------------------------------------------------------------------------------------  ")

  def LeftFootedMidfildersUnder30() = {
    val midfilders = Seq("RWM", "RM", "RCM", "CM", "CAM", "CDM", "LCM", "LM", "LWM")

    val leftFootedMidfielders = cleaningFifaDailyData.filter(col("Preferred Foot") === "Left"
      && col("Age") < 30 && col("Club") =!= "NA" && $"Position".isin(midfilders: _*))
      .withColumn("leftmidfielder_count", count("Name").over(Window.partitionBy("Club")))
      .select("Club", "leftmidfielder_count")
      .dropDuplicates()
      .withColumn("Rank", dense_rank().over(Window.orderBy(desc("leftmidfielder_count"))))
      .filter(col("Rank") === 1)
    leftFootedMidfielders
  }

  LeftFootedMidfildersUnder30().show()
  println("-----------------------------------------------------------------------  ")
  println("2b. The strongest National team by overall rating for a 4-4-2 formation")
  println("-----------------------------------------------------------------------  ")

  def strongestNationByRating() = {
    val strongCountrybyRating = cleaningFifaDailyData.select("Nationality", "Overall", "Position").dropDuplicates().
      withColumn("Avg_overall_rating", avg("Overall").over(Window.partitionBy("Nationality"))).
      withColumn("All_positions", collect_set("Position").over(Window.partitionBy("Nationality")))
      .drop("Overall", "Position")
      .dropDuplicates()
      .filter(array_contains(col("All_positions"), "GK") &&
        array_contains(col("All_positions"), "RB") &&
        (array_contains(col("All_positions"), "CB") || array_contains(col("All_positions"), "RCB")) &&
        (array_contains(col("All_positions"), "CB") || array_contains(col("All_positions"), "LCB ")) &&
        array_contains(col("All_positions"), "LB") &&
        (array_contains(col("All_positions"), "RM") || array_contains(col("All_positions"), "RWM ")) &&
        (array_contains(col("All_positions"), "LCM") || array_contains(col("All_positions"), "CM ")) &&
        (array_contains(col("All_positions"), "RCM") || array_contains(col("All_positions"), "CM ")) &&
        (array_contains(col("All_positions"), "LM") || array_contains(col("All_positions"), "LWM ")) &&
        (array_contains(col("All_positions"), "RF") || array_contains(col("All_positions"), "CF ") || array_contains(col("All_positions"), "LF ")) &&
        array_contains(col("All_positions"), "ST")
      )
      .withColumn("Rank", dense_rank().over(Window.orderBy(desc("Avg_overall_rating"))))
      .filter(col("Rank") === 1)
    strongCountrybyRating
  }

  strongestNationByRating.show()
  println("---------------------------------------------------------------  ")
  println(" 2b. The strongest Club by overall rating for a 4-4-2 formation  ")
  println("---------------------------------------------------------------  ")

  def strongestClubByRating() = {
    val strongClubbyRating = cleaningFifaDailyData.select("Club", "Overall", "Position").dropDuplicates().
      withColumn("Avg_overall_rating", avg("Overall").over(Window.partitionBy("Club"))).
      withColumn("All_positions", collect_set("Position").over(Window.partitionBy("Club")))
      .drop("Overall", "Position")
      .dropDuplicates()
      .filter(array_contains(col("All_positions"), "GK") &&
        array_contains(col("All_positions"), "RB") &&
        (array_contains(col("All_positions"), "CB") || array_contains(col("All_positions"), "RCB")) &&
        (array_contains(col("All_positions"), "CB") || array_contains(col("All_positions"), "LCB ")) &&
        array_contains(col("All_positions"), "LB") &&
        (array_contains(col("All_positions"), "RM") || array_contains(col("All_positions"), "RWM ")) &&
        (array_contains(col("All_positions"), "LCM") || array_contains(col("All_positions"), "CM ")) &&
        (array_contains(col("All_positions"), "RCM") || array_contains(col("All_positions"), "CM ")) &&
        (array_contains(col("All_positions"), "LM") || array_contains(col("All_positions"), "LWM ")) &&
        (array_contains(col("All_positions"), "RF") || array_contains(col("All_positions"), "CF ") || array_contains(col("All_positions"), "LF ")) &&
        array_contains(col("All_positions"), "ST")
      )
      .withColumn("Rank", dense_rank().over(Window.orderBy(desc("Avg_overall_rating"))))
      .filter(col("Rank") === 1)
    strongClubbyRating
  }

  strongestClubByRating.show()

  println("---------------------------------------------------------------------------------------------------------------  ")
  println(" 2c. Which team has the most expensive squad value in the world? Does that team also have the largest wage bill ?  ")
  println("---------------------------------------------------------------------------------------------------------------  ")

  def MostExpensiveNationValue() = {
    val mostExpensiveNationSquadvalue = cleaningFifaDailyData.select("Nationality", "Value").
      withColumn("Converted_value",
        when(col("Value").contains("M"),
          regexp_replace(regexp_replace(col("Value"), "€", ""), "M", "").cast("Decimal") * 1000000
        ).
          when(col("Value").contains("K"),
            regexp_replace(regexp_replace(col("Value"), "€", ""), "K", "").cast("Decimal") * 1000
          )
      )
      .groupBy("Nationality").agg(sum("Converted_value").as("value"))
      .withColumn("Rank", dense_rank().over(Window.orderBy(desc("value"))))
      .filter(col("Rank") === 1)
    mostExpensiveNationSquadvalue
  }

  def MostExpensiveNationWages() = {
    val mostExpensiveNationSquadWages = cleaningFifaDailyData.select("Nationality", "Wage").
      withColumn("Converted_Wage",
        when(col("Wage").contains("M"),
          regexp_replace(regexp_replace(col("Wage"), "€", ""), "M", "").cast("Decimal") * 1000000
        ).
          when(col("Wage").contains("K"),
            regexp_replace(regexp_replace(col("Wage"), "€", ""), "K", "").cast("Decimal") * 1000
          )
      )
      .groupBy("Nationality").agg(sum("Converted_Wage").as("Wage"))
      .withColumn("Rank", dense_rank().over(Window.orderBy(desc("Wage"))))
      .filter(col("Rank") === 1)
    mostExpensiveNationSquadWages
  }

  MostExpensiveNationValue.show()
  MostExpensiveNationWages.show()
  val ExpensiveNationByValue = MostExpensiveNationValue.select("Nationality").first().getString(0)
  val ExpensiveNationBywages = MostExpensiveNationWages.select("Nationality").first().getString(0)

  if (ExpensiveNationByValue == ExpensiveNationBywages) {
    println("Yes," + ExpensiveNationByValue + " has the Most expensive squad value in World and also with Highest Wage!")
  }
  else {
    println("The Most expensive squad value in World and also with Highest Wage are not Same !")
  }
  println("-----------------------------------------------------  ")
  println(" 2d. Which position pays the highest wage in average?   ")
  println("-----------------------------------------------------  ")

  def positionWithHighestAvgWage() = {
    val positionWithHighestWageAvg = cleaningFifaDailyData.select("Position", "Wage").
      withColumn("Wage_value_Converted",
        when(col("Wage").contains("M"),
          regexp_replace(regexp_replace(col("Wage"), "€", ""), "M", "").cast("Decimal") * 1000000
        ).
          when(col("Wage").contains("K"),
            regexp_replace(regexp_replace(col("Wage"), "€", ""), "K", "").cast("Decimal") * 1000
          )
      ).withColumn("Avg_wage", avg("Wage_value_Converted").over(Window.partitionBy("Position")))
      .select("Position", "Avg_wage")
      .dropDuplicates()
      .withColumn("Rank", row_number().over(Window.orderBy(desc("Avg_wage"))))
      .filter(col("Rank") === 1)
    positionWithHighestWageAvg
  }

  positionWithHighestAvgWage.show(false)

  println("-------------------------------------------------------------------------------------------------------------  ")
  println(" 2e. What makes a goalkeeper great? Share 4 attributes which are most relevant to becoming a good goalkeeper?    ")
  println("-------------------------------------------------------------------------------------------------------------  ")

  def fourGoalkeeperAttributes() = {
    val greatGoalkeeperdf = cleaningFifaDailyData
      .where(col("Position") === "GK")
      .agg(avg("Crossing").alias("Crossing"),
        avg("Finishing").alias("Finishing"),
        avg("HeadingAccuracy").alias("HeadingAccuracy"),
        avg("ShortPassing").alias("ShortPassing"),
        avg("Volleys").alias("Volleys"),
        avg("Dribbling").alias("Dribbling"),
        avg("Curve").alias("Curve"),
        avg("FKAccuracy").alias("FKAccuracy"),
        avg("LongPassing").alias("LongPassing"),
        avg("BallControl").alias("BallControl"),
        avg("Acceleration").alias("Acceleration"),
        avg("SprintSpeed").alias("SprintSpeed"),
        avg("Agility").alias("Agility"),
        avg("Reactions").alias("Reactions"),
        avg("Balance").alias("Balance"),
        avg("ShotPower").alias("ShotPower"),
        avg("Jumping").alias("Jumping"),
        avg("Stamina").alias("Stamina"),
        avg("Strength").alias("Strength"),
        avg("LongShots").alias("LongShots"),
        avg("Aggression").alias("Aggression"),
        avg("Interceptions").alias("Interceptions"),
        avg("Positioning").alias("Positioning"),
        avg("Vision").alias("Vision"),
        avg("Penalties").alias("Penalties"),
        avg("Composure").alias("Composure"),
        avg("Marking").alias("Marking"),
        avg("StandingTackle").alias("StandingTackle"),
        avg("SlidingTackle").alias("SlidingTackle"),
        avg("GKDiving").alias("GKDiving"),
        avg("GKHandling").alias("GKHandling"),
        avg("GKKicking").alias("GKKicking"),
        avg("GKPositioning").alias("GKPositioning"),
        avg("GKReflexes").alias("GKReflexes"))
      .withColumn("Position", lit("GK")).
      select(col("Position"), expr("stack(34, 'Crossing',Crossing,'Finishing',Finishing,'HeadingAccuracy',HeadingAccuracy,'ShortPassing',ShortPassing,'Volleys',Volleys,'Dribbling',Dribbling,'Curve',Curve,'FKAccuracy',FKAccuracy,'LongPassing',LongPassing,'BallControl',BallControl,'Acceleration',Acceleration,'SprintSpeed',SprintSpeed,'Agility',Agility,'Reactions',Reactions,'Balance',Balance,'ShotPower',ShotPower,'Jumping',Jumping,'Stamina',Stamina,'Strength',Strength,'LongShots',LongShots,'Aggression',Aggression,'Interceptions',Interceptions,'Positioning',Positioning,'Vision',Vision,'Penalties',Penalties,'Composure',Composure,'Marking',Marking,'StandingTackle',StandingTackle,'SlidingTackle',SlidingTackle,'GKDiving',GKDiving,'GKHandling',GKHandling,'GKKicking',GKKicking,'GKPositioning',GKPositioning,'GKReflexes',GKReflexes) as (great_Goalkeeper_Attributes, Average)"))
      .drop("Position")
      .withColumn("great_Goalkeeper_AttributeWise_Rank", dense_rank().over(Window.orderBy(desc("Average"))))
      .where($"great_Goalkeeper_AttributeWise_Rank" < 5)

    greatGoalkeeperdf
  }

  fourGoalkeeperAttributes.show()

  println("-------------------------------------------------------------------------------------------------------------  ")
  println(" 2e. What makes a good Striker (ST)? Share 5 attributes which are most relevant to becoming a top striker ?     ")
  println("-------------------------------------------------------------------------------------------------------------  ")

  def fiveStrikerAttributes() = {
    val greatStrikerdf = cleaningFifaDailyData
      .where(col("Position") === "ST")
      .agg(avg("Crossing").alias("Crossing"),
        avg("Finishing").alias("Finishing"),
        avg("HeadingAccuracy").alias("HeadingAccuracy"),
        avg("ShortPassing").alias("ShortPassing"),
        avg("Volleys").alias("Volleys"),
        avg("Dribbling").alias("Dribbling"),
        avg("Curve").alias("Curve"),
        avg("FKAccuracy").alias("FKAccuracy"),
        avg("LongPassing").alias("LongPassing"),
        avg("BallControl").alias("BallControl"),
        avg("Acceleration").alias("Acceleration"),
        avg("SprintSpeed").alias("SprintSpeed"),
        avg("Agility").alias("Agility"),
        avg("Reactions").alias("Reactions"),
        avg("Balance").alias("Balance"),
        avg("ShotPower").alias("ShotPower"),
        avg("Jumping").alias("Jumping"),
        avg("Stamina").alias("Stamina"),
        avg("Strength").alias("Strength"),
        avg("LongShots").alias("LongShots"),
        avg("Aggression").alias("Aggression"),
        avg("Interceptions").alias("Interceptions"),
        avg("Positioning").alias("Positioning"),
        avg("Vision").alias("Vision"),
        avg("Penalties").alias("Penalties"),
        avg("Composure").alias("Composure"),
        avg("Marking").alias("Marking"),
        avg("StandingTackle").alias("StandingTackle"),
        avg("SlidingTackle").alias("SlidingTackle"),
        avg("GKDiving").alias("GKDiving"),
        avg("GKHandling").alias("GKHandling"),
        avg("GKKicking").alias("GKKicking"),
        avg("GKPositioning").alias("GKPositioning"),
        avg("GKReflexes").alias("GKReflexes"))
      .withColumn("Position", lit("ST")).
      select(col("Position"), expr("stack(34, 'Crossing',Crossing,'Finishing',Finishing,'HeadingAccuracy',HeadingAccuracy,'ShortPassing',ShortPassing,'Volleys',Volleys,'Dribbling',Dribbling,'Curve',Curve,'FKAccuracy',FKAccuracy,'LongPassing',LongPassing,'BallControl',BallControl,'Acceleration',Acceleration,'SprintSpeed',SprintSpeed,'Agility',Agility,'Reactions',Reactions,'Balance',Balance,'ShotPower',ShotPower,'Jumping',Jumping,'Stamina',Stamina,'Strength',Strength,'LongShots',LongShots,'Aggression',Aggression,'Interceptions',Interceptions,'Positioning',Positioning,'Vision',Vision,'Penalties',Penalties,'Composure',Composure,'Marking',Marking,'StandingTackle',StandingTackle,'SlidingTackle',SlidingTackle,'GKDiving',GKDiving,'GKHandling',GKHandling,'GKKicking',GKKicking,'GKPositioning',GKPositioning,'GKReflexes',GKReflexes) as (great_Striker_Attributes, Average)"))
      .drop("Position")
      .withColumn("great_Striker_Attributes_Rank", dense_rank().over(Window.orderBy(desc("Average"))))
      .where($"great_Striker_Attributes_Rank" < 6)

    greatStrikerdf
  }

  fiveStrikerAttributes.show()


  println("-------------------------------------------------------------------------------------------------------------  ")
  println(" 3. In addition to your solution in (1), also store the dataset in Postgres Database using a " +
    "docker (postgres:11.5) where one can query for" +
    "Overall Rating (int), Position, Country, Name, Club Name, " +
    "Wage(as decimal), Value (as decimal), Joined (date), Age (int)")
  println("-------------------------------------------------------------------------------------------------------------  ")

  val greenplumData = cleaningFifaDailyData.
    withColumn("Wage_value_Converted",
      when(col("Wage").contains("M"),
        regexp_replace(regexp_replace(col("Wage"), "€", ""), "M", "").cast("Decimal") * 1000000
      ).
        when(col("Wage").contains("K"),
          regexp_replace(regexp_replace(col("Wage"), "€", ""), "K", "").cast("Decimal") * 1000
        )
    ).withColumn("Converted_value",
    when(col("Value").contains("M"),
      regexp_replace(regexp_replace(col("Value"), "€", ""), "M", "").cast("Decimal") * 1000000
    ).
      when(col("Value").contains("K"),
        regexp_replace(regexp_replace(col("Value"), "€", ""), "K", "").cast("Decimal") * 1000
      )
  )
    .select(col("Overall").alias("Overall_Rating"),
      col("Position"), col("Nationality").alias("Country"), col("Name"), col("Club").alias("Club_Name")
      , col("Wage_value_Converted").alias("Wage")
      , col("Converted_value").alias("Value")
      , to_date(col("joined"), "MMM dd, yyyy").alias("Joined"),
      col("Age")
    )

  greenplumData.show()

  greenplumData.write.format(PropertiesLoader.greenplumsource)
    .options(Map("dbtable" -> PropertiesLoader.greenplumTable, "url" -> PropertiesLoader.greenplumurl))
    .option("driver", "org.postgresql.Driver")
    .option("loadDataCompression", "LZ4")
    .option("insertBatchSize", 100000).mode("append").save()

  cleaningFifaDailyData.unpersist()
  spark.close()
}