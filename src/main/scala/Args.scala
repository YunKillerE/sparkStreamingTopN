import com.beust.jcommander.Parameter

class Args extends Serializable {

  @Parameter(names = Array("-appName"), required = true) var appName: String = null

  @Parameter(names = Array("-kuduMaster"), required = true) var kuduMaster: String = null

  @Parameter(names = Array("-brokers"), required = true) var brokers: String = null

  @Parameter(names = Array("-groupid"), required = true) var groupid: String = null

  @Parameter(names = Array("-topic"), required = true) var topic: String = null

  @Parameter(names = Array("-kuduTableName"), required = true) var kuduTableName: String = null

  @Parameter(names = Array("-numStreams"), required = true) var numStreams: Int = 5

}
