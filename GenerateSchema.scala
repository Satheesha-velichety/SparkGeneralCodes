
  def getDataType(dataTypeStr: String): DataType = {
    dataTypeStr match {
      case "Int" => {
        return IntegerType
      }
      case "Double" => {
        return DoubleType
      }
      case "Boolean" => {
        return BooleanType
      }
      case "Float" => {
        return FloatType
      }
      case "Long" => {
        return LongType
      }
      case "Integer" => {
        return IntegerType
      }
      case "Date" => {
        return DateType
      }
      case "Timestamp" => {
        return TimestampType
      }
      case "String" => {
        return StringType
      }
      case "map" => {
        return MapType(StringType,StringType,true)
      }
      case _ => {
        return StringType
      }
    }
  }

  def buildSchema(schemaMap:scala.collection.mutable.LinkedHashMap[String,String]) : StructType = {

    import scala.collection.mutable.ArrayBuffer
    val strtyp:ArrayBuffer[StructField]= ArrayBuffer[StructField]()
    for ((key: String, value: String) <- schemaMap) {
      strtyp.+=(StructField(key, getDataType(value),true))
    }
    return StructType(strtyp.toArray)
  }
