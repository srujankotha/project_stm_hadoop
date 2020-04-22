package ca.mcit.bigdata.course2.project

import ca.mcit.bigdata.course2.project.data.{DataWriter, GetData}
import ca.mcit.bigdata.course2.project.inputdata.{Calender, Route, Trip}
import ca.mcit.bigdata.course2.project.lookup.{CalendarLookup, RouteLookup}
import ca.mcit.bigdata.course2.project.proccesed.{EnrichTrip, TripRoute}

import scala.collection.mutable.ListBuffer

object Enricher extends App{

  val readData : GetData = new GetData
  val tripList: List[Trip] = readData.getTripList
  val routeList: List[Route] = readData.gRouteList
  val calanderList: List[Calender] = readData.getCalenderList
  val routeLookup = new RouteLookup(routeList)
  val calenderLookUp = new CalendarLookup(calanderList)
  val enrichedTripRoute: List[TripRoute] = tripList.map(trip => TripRoute(trip,
    routeLookup.lookup(trip.route_id)))
  val enrichedTrip: List[EnrichTrip] = enrichedTripRoute.map(tripRoute => EnrichTrip(tripRoute,
    calenderLookUp.lookup(tripRoute.trips.service_id)))

  var enrichedTripRes = new ListBuffer[EnrichTrip]()

  for{
    tripRoute <- enrichedTripRoute

  } yield {
        enrichedTripRes += EnrichTrip(tripRoute,calenderLookUp.lookup(tripRoute.trips.service_id))
  }

  val writer = new DataWriter(enrichedTrip.filter(p => p.calender.monday=="1" && p.tripRoute.routes.route_url.contains("metro")))
writer.writeData()
}
