package cse512

object HotzoneUtils {

  def ST_Contains(queryRectangle: String, pointString: String ): Boolean = {
    var point = pointString.split(",")
    var rectangleList = queryRectangle.split(",")
    var point1 = point(0).toDouble
    var point2 = point(1).toDouble

    var rectangleX1 = rectangleList(0).toDouble
    var rectangleY1 = rectangleList(1).toDouble
    var rectangleX2 = rectangleList(2).toDouble
    var rectangleY2 = rectangleList(3).toDouble

    if (rectangleX1>rectangleX2){
      var temp = rectangleX2
      rectangleX2 = rectangleX1
      rectangleX1 = temp
    }

    if (rectangleY1>rectangleY2){
      var temp = rectangleY2
      rectangleY2 = rectangleY1
      rectangleY1 = temp
    }

    if(point1>=rectangleX1 && point1<=rectangleX2 && point2>=rectangleY1 && point2<=rectangleY2){
      return true
    }

    return false;
  }

}
