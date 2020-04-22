package ca.mcit.bigdata.course2.project.data

import java.io.OutputStreamWriter
import ca.mcit.bigdata.course2.project.proccesed.EnrichTrip
import com.opencsv._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}

class DataWriter(enrichedList: List[EnrichTrip]) {
  println("filtered data")//+element.tripRoute.trips.service_id)
  val conf = new Configuration()

  val fs:FileSystem= FileSystem.get(conf)
  val outputPath = new Path ("/user/fall2019/srujan/stm/SrujanfinalOutput.csv")
  val out = new OutputStreamWriter( fs.create(outputPath))
  val writer: CSVWriter = new CSVWriter(out)
  val outputfile: Array[String] = Array("Route Id", "Service Id", "Trip Id", "Trip Head Sign", "Direction Id",
    "Shape Id", "Wheelchair accessible", "Note_FR", "Note En", "Agency Id",
    "Route Short Name", "Route Long Name", "Route Type", "Route Url", "Route Colour",
    "Monday", "Start Date", "End Date")
  conf.addResource(new Path("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/hdfs-site.xml"))
  conf.addResource(new Path ("/home/bd-user/opt/hadoop-2.7.3/etc/cloudera/core-site.xml"))

  def writeData():Unit = {
    println("Filtered data")//+element.tripRoute.trips.service_id)

    writer.writeNext(outputfile)
    enrichedList.foreach { element => {
      val data = Array(element.tripRoute.routes.route_id.toString, element.calender.service_id,
        element.tripRoute.trips.trip_id, element.tripRoute.trips.trip_headsign,
        element.tripRoute.trips.direction_id, element.tripRoute.trips.shape_id,
        element.tripRoute.trips.wheelchair_accessible, element.tripRoute.trips.note_fr,
        element.tripRoute.trips.note_en, element.tripRoute.routes.agency_id,
        element.tripRoute.routes.route_short_name, element.tripRoute.routes.route_long_name,
        element.tripRoute.routes.route_type.toString, element.tripRoute.routes.route_url,
        element.tripRoute.routes.route_color, element.calender.monday,
        element.calender.start_date, element.calender.end_date
      )
      writer.writeNext(data)
    }
println("=="+element.tripRoute.trips.service_id)

    }

    writer.close()

  }

}
