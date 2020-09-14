import java.util.Properties

import org.apache.log4j.PropertyConfigurator

object PropertiesLoader {

  val connectionParam = new Properties
  connectionParam.load(getClass().getResourceAsStream("/greenplum.properties"))
  PropertyConfigurator.configure(connectionParam)

  //Greenplum specific properties

  val greenplumurl = connectionParam.getProperty("greenplumurl")
  val greenplumsource = connectionParam.getProperty("greenplumsource")
  val greenplumTable = connectionParam.getProperty("greenplumTable")
}