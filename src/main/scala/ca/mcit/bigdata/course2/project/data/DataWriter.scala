package ca.mcit.bigdata.course2.project.data

import java.io.{File, FileWriter}

import ca.mcit.bigdata.course2.project.proccesed.EnrichTrip
import com.opencsv._

class DataWriter(enrichedList: List[EnrichTrip]) {
  println("filtered data")//+element.tripRoute.trips.service_id)

  val outputPath = "/home/bd-user/project_result_csv/SrujanfinalOutput.csv"
  var file: File = new File(outputPath)
  val output: FileWriter = new FileWriter(file)
  val writer: CSVWriter = new CSVWriter(output)
  val outputfile: Array[String] = Array("Route Id", "Service Id", "Trip Id", "Trip Head Sign", "Direction Id",
    "Shape Id", "Wheelchair accessible", "Note_FR", "Note En", "Agency Id",
    "Route Short Name", "Route Long Name", "Route Type", "Route Url", "Route Colour",
    "Monday", "Start Date", "End Date")


  def writeData():Unit = {
    println("Filtered data")//+element.tripRoute.trips.service_id)

    writer.writeNext(outputfile)
    enrichedList.foreach { element => {
      val data = Array(element.tripRoute.routes.route_id.toString, element.calender.service_id.toString,
        element.tripRoute.trips.trip_id.toString, element.tripRoute.trips.trip_headsign.toString,
        element.tripRoute.trips.direction_id.toString, element.tripRoute.trips.shape_id.toString,
        element.tripRoute.trips.wheelchair_accessible.toString, element.tripRoute.trips.note_fr.getOrElse().toString,
        element.tripRoute.trips.note_en.getOrElse().toString, element.tripRoute.routes.agency_id.toString,
        element.tripRoute.routes.route_short_name.toString, element.tripRoute.routes.route_long_name.toString,
        element.tripRoute.routes.route_type.toString, element.tripRoute.routes.route_url.toString,
        element.tripRoute.routes.route_color.toString, element.calender.monday.toString,
        element.calender.start_date.toString, element.calender.end_date.toString
      )
      writer.writeNext(data)
    }
println("=="+element.tripRoute.trips.service_id)

    }

    writer.close()

  }

}
