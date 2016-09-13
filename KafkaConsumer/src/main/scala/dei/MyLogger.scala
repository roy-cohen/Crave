package dei

import java.io.Serializable

import org.apache.log4j.Level

  class MyLogger() extends Serializable{
    @transient lazy val log = org.apache.log4j.Logger.getRootLogger()
    log.setLevel(Level.WARN)
    def debug (str : String): Unit = {
      log.debug(str)
    }

    def warn(str : String): Unit = {
      log.warn(str)
    }
}
