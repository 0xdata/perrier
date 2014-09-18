package org.apache.spark.examples.h2o

/** Prostate schema definition. */
case class Prostate(ID      :Option[Int]  ,
                    CAPSULE :Option[Int]  ,
                    AGE     :Option[Int]  ,
                    RACE    :Option[Int]  ,
                    DPROS   :Option[Int]  ,
                    DCAPS   :Option[Int]  ,
                    PSA     :Option[Float],
                    VOL     :Option[Float],
                    GLEASON :Option[Int]  ) {
}

/** A dummy csv parser for prostate dataset. */
object ProstateParse extends Serializable {
  def apply(row: Array[String]): Prostate = {
    import org.apache.spark.examples.h2o.SchemaUtils._
    Prostate(int(row(0)), int(row(1)), int(row(2)), int(row(3)), int(row(4)), int(row(5)), float(row(6)), float(row(7)), int(row(8)) )
  }
}

/** Airlines schema definition. */
class Airlines (Year              :Option[Int],
                Month             :Option[Int],
                DayofMonth        :Option[Int],
                DayOfWeek         :Option[Int],
                DepTime           :Option[Int],
                CRSDepTime        :Option[Int],
                ArrTime           :Option[Int],
                CRSArrTime        :Option[Int],
                UniqueCarrier     :Option[String],
                FlightNum         :Option[Int],
                TailNum           :Option[Int],
                ActualElapsedTime :Option[Int],
                CRSElapsedTime    :Option[Int],
                AirTime           :Option[Int],
                ArrDelay          :Option[Int],
                DepDelay          :Option[Int],
                Origin            :Option[String],
                Dest              :Option[String],
                Distance          :Option[Int],
                TaxiIn            :Option[Int],
                TaxiOut           :Option[Int],
                Cancelled         :Option[Int],
                CancellationCode  :Option[Int],
                Diverted          :Option[Int],
                CarrierDelay      :Option[Int],
                WeatherDelay      :Option[Int],
                NASDelay          :Option[Int],
                SecurityDelay     :Option[Int],
                LateAircraftDelay :Option[Int],
                IsArrDelayed      :Option[Boolean],
                IsDepDelayed      :Option[Boolean]) extends Product {

  /*
  def this() = this(None,None,None,None,None,None,None,None,None,
    None,None,None,None,None,None,None,None,None,
    None,None,None,None,None,None,None,None,None,
    None,None,None,None) */
  override def canEqual(that: Any):Boolean = that.isInstanceOf[Airlines]
  override def productArity: Int = 31
  override def productElement(n: Int) = n match {
    case  0 => Year
    case  1 => Month
    case  2 => DayofMonth
    case  3 => DayOfWeek
    case  4 => DepTime
    case  5 => CRSDepTime
    case  6 => ArrTime
    case  7 => CRSArrTime
    case  8 => UniqueCarrier
    case  9 => FlightNum
    case 10 => TailNum
    case 11 => ActualElapsedTime
    case 12 => CRSElapsedTime
    case 13 => AirTime
    case 14 => ArrDelay
    case 15 => DepDelay
    case 16 => Origin
    case 17 => Dest
    case 18 => Distance
    case 19 => TaxiIn
    case 20 => TaxiOut
    case 21 => Cancelled
    case 22 => CancellationCode
    case 23 => Diverted
    case 24 => CarrierDelay
    case 25 => WeatherDelay
    case 26 => NASDelay
    case 27 => SecurityDelay
    case 28 => LateAircraftDelay
    case 29 => IsArrDelayed
    case 30 => IsDepDelayed
    case  _ => throw new IndexOutOfBoundsException(n.toString)
  }
  override def toString:String = {
    val sb = new StringBuffer
    for( i <- 0 until productArity )
      sb.append(productElement(i)).append(',')
    sb.toString
  }
}

/** A dummy csv parser for airlines dataset. */
object AirlinesParse extends Serializable {
  def apply(row: Array[String]): Airlines = {
    import SchemaUtils._
    new Airlines(int (row( 0)), // Year
      int (row( 1)), // Month
      int (row( 2)), // DayofMonth
      int (row( 3)), // DayOfWeek
      int (row( 4)), // DepTime
      int (row( 5)), // CRSDepTime
      int (row( 6)), // ArrTime
      int (row( 7)), // CRSArrTime
      str (row( 8)), // UniqueCarrier
      int (row( 9)), // FlightNum
      int (row(10)), // TailNum
      int (row(11)), // ActualElapsedTime
      int (row(12)), // CRSElapsedTime
      int (row(13)), // AirTime
      int (row(14)), // ArrDelay
      int (row(15)), // DepDelay
      str (row(16)), // Origin
      str (row(17)), // Dest
      int (row(18)), // Distance
      int (row(19)), // TaxiIn
      int (row(20)), // TaxiOut
      int (row(21)), // Cancelled
      int (row(22)), // CancellationCode
      int (row(23)), // Diverted
      int (row(24)), // CarrierDelay
      int (row(25)), // WeatherDelay
      int (row(26)), // NASDelay
      int (row(27)), // SecurityDelay
      int (row(28)), // LateAircraftDelay
      bool(row(29)), // IsArrDelayed
      bool(row(30))) // IsDepDelayed
  }
}
