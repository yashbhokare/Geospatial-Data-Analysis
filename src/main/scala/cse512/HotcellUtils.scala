package cse512

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Calendar

object HotcellUtils {
  val coordinateStep = 0.01

  def CalculateCoordinate(inputString: String, coordinateOffset: Int): Int =
  {
    // Configuration variable:
    // Coordinate step is the size of each cell on x and y
    var resultult = 0
    coordinateOffset match
    {
      case 0 => resultult = Math.floor((inputString.split(",")(0).replace("(","").toDouble/coordinateStep)).toInt
      case 1 => resultult = Math.floor(inputString.split(",")(1).replace(")","").toDouble/coordinateStep).toInt
      // We only consider the data from 2009 to 2012 icellCountlusively, 4 years in total. Week 0 Day 0 is 2009-01-01
      case 2 => {
        val timestamp = HotcellUtils.timestampParser(inputString)
        resultult = HotcellUtils.dayOfMonth(timestamp) // Assume every month has 31 days
      }
    }
    return resultult
  }

  def timestampParser (timestampString: String): Timestamp =
  {
    val dateFormat = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
    val parsedDate = dateFormat.parse(timestampString)
    val timeStamp = new Timestamp(parsedDate.getTime)
    return timeStamp
  }

  def dayOfYear (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_YEAR)
  }

  def dayOfMonth (timestamp: Timestamp): Int =
  {
    val calendar = Calendar.getInstance
    calendar.setTimeInMillis(timestamp.getTime)
    return calendar.get(Calendar.DAY_OF_MONTH)
  }



  def getNeighbours(inputX: Int, inputY: Int, inputZ: Int, minX: Int, minY: Int, minZ: Int, maxX: Int, maxY: Int,  maxZ: Int): Int =
  {
    var cellCount = 0
    var result = 26

    if (inputX == minX || inputX == maxX)
      cellCount += 1

    if (inputY == minY || inputY == maxY)
      cellCount += 1

    if (inputZ == minZ || inputZ == maxZ)
      cellCount += 1

    if (cellCount == 1)
      result =  17;
    else if (cellCount == 2)
      result =  11;
    else if (cellCount == 3)
      result =  7;

    return result;
  }

  def getZValue(neighbourCount: Int, total: Int, cellCount: Int, x: Int, y: Int, z: Int, avg: Double, std: Double): Double =
  {
    // val dividend = (total.toDouble - (avg * neighbourCount.toDouble))
    // val divisor = std * math.sqrt((((cellCount.toDouble * neighbourCount.toDouble) - (neighbourCount.toDouble * neighbourCount.toDouble)) / (cellCount.toDouble - 1.0).toDouble).toDouble).toDouble
    // var result = (dividend / divisor).toDouble
    return    ({(total.toDouble - (avg * neighbourCount.toDouble))} /  std * math.sqrt((((cellCount.toDouble * neighbourCount.toDouble) - (neighbourCount.toDouble * neighbourCount.toDouble)) / (cellCount.toDouble - 1.0).toDouble).toDouble).toDouble ).toDouble;
  }
}
