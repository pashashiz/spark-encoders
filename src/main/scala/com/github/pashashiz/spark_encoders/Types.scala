package com.github.pashashiz.spark_encoders

object Types

//   def javaType(dt: DataType): String = dt match {
//    case udt: UserDefinedType[_] => javaType(udt.sqlType)
//    case ObjectType(cls) if cls.isArray => s"${javaType(ObjectType(cls.getComponentType))}[]"
//    case ObjectType(cls) => cls.getName
//    case _ => PhysicalDataType(dt) match {
//      case _: PhysicalArrayType => "ArrayData"
//      case PhysicalBinaryType => "byte[]"
//      case PhysicalBooleanType => JAVA_BOOLEAN
//      case PhysicalByteType => JAVA_BYTE
//      case PhysicalCalendarIntervalType => "CalendarInterval"
//      case PhysicalIntegerType => JAVA_INT
//      case _: PhysicalDecimalType => "Decimal"
//      case PhysicalDoubleType => JAVA_DOUBLE
//      case PhysicalFloatType => JAVA_FLOAT
//      case PhysicalLongType => JAVA_LONG
//      case _: PhysicalMapType => "MapData"
//      case PhysicalShortType => JAVA_SHORT
//      case PhysicalStringType => "UTF8String"
//      case _: PhysicalStructType => "InternalRow"
//      case _ => "Object"
//    }
//  }
