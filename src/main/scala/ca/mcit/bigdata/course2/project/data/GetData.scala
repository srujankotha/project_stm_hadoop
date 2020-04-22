package ca.mcit.bigdata.course2.project.data
import java.io.BufferedInputStream
import ca.mcit.bigdata.course2.project.inputdata.{Calender, Route, Trip}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.io.Source

class GetData {
  val conf = new Configuration()
  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
    //val filePathRoutes = "/home/bd-user/Downloads/routes.txt"
  conf.addResource(new Path ("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))
  val hadoop:FileSystem = FileSystem.get(conf)


  val filePathRoutes = new BufferedInputStream( hadoop.open (new Path("/user/fall2019/srujan/stm/routes.txt")))
  //noinspection SourceNotClosed
  def gRouteList: List[Route] = Source.fromInputStream(filePathRoutes).getLines().drop(1)
    .map(line => line.split(","))
    .map(a => Route(a(0).toInt, a(1), a(2), a(3), a(4).toInt, a(5), a(6))).toList

  val filePathCalender = new BufferedInputStream( hadoop.open(new Path("/user/fall2019/srujan/stm/calendar.txt")))
  //noinspection SourceNotClosed
  def getCalenderList: List[Calender] = {
    Source.fromInputStream(filePathCalender).getLines().drop(1).map(line => line.split(","))
      .map(a => Calender(a(0), a(1), a(2), a(3), a(4), a(5), a(6), a(7), a(8), a(9))).toList
  }
  val filePathTrips = new BufferedInputStream( hadoop.open(new Path("/user/fall2019/srujan/stm/trips.txt")))
  //noinspection SourceNotClosed
  def getTripList: List[Trip] = Source.fromInputStream(filePathTrips)
    .getLines()
    .drop(1)
    .map(line => line.split(","))
    .map(a => Trip(a(0).toInt, a(1), a(2), a(3), a(4), a(5), a(6), a(7),a(8))).toList


}
