package ca.mcit.bigdata.course2.project.inputdata

case class Trip(route_id:Int,
                service_id:String,
                trip_id:String,
                trip_headsign:String,
                direction_id:String,
                shape_id:String,
                wheelchair_accessible:String,
                note_fr: Option[String]=None,
                note_en:Option[String]= None)


