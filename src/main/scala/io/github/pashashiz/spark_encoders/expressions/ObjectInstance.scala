package io.github.pashashiz.spark_encoders.expressions

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.types.ObjectType

object ObjectInstance {
  def apply(objectClass: Class[_]): Expression =
    StaticFieldAccess(objectClass, ObjectType(objectClass), "MODULE$")
}
