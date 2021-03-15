package project6

case class Enriched_Station( systemid:String, timezone:String, stationid:Int, name:String, shortname:String,
                             lat:Double, lon:Double, capacity:Int)
object Enriched_Station{def fromCsv(route:String):Enriched_Station={
  val p=route.split(",").toList
  Enriched_Station(p(0),p(1),p(2).toInt,p(3),p(4),p(5).toDouble,p(6).toDouble,p(7).toInt)}
}

case class TripStream(startdate:String,startstationcode:String,enddate:String,endstationcode:Int
                      ,durationsec:Int,
                      ismember:Int)
object TripStream {
  def fromCsv(trip: String): TripStream = {
    val p = trip.split(",").toList
    TripStream(p(0), p(1), p(2), p(3).toInt, p(4).toInt, p(5).toInt)
  }
  def toCsv(calendar: TripStream, trip: Enriched_Station): String = {
    s"${calendar.startdate},${calendar.startstationcode},${calendar.enddate}," +
      s"${calendar.endstationcode},${calendar.durationsec}," +
      s"${calendar.ismember},${trip.systemid},${trip.timezone}" +
      s",${trip.stationid},${trip.name},${trip.shortname},${trip.lat},${trip.lon}," +
      s"${trip.capacity}"
  }
}