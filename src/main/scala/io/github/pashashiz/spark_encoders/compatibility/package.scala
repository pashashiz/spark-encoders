package io.github.pashashiz.spark_encoders

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.DataType

package object compatibility {
  /**
   * Shim for StaticInvoke that has an extra constructor arg added in Spark 4.x.
   * Databricks often backports changes like this Databricks Runtimes running with older Spark versions.
   * This change was also backported to DBR 14, 15 and 16.
   */
  def staticInvoke(
    staticObject: Class[_],
    dataType: DataType,
    functionName: String,
    arguments: Seq[Expression],
    propagateNull: Boolean = true,
    returnNullable: Boolean = true,
    isDeterministic: Boolean = true
  ): Expression = {
    val constructor = classOf[StaticInvoke].getConstructors.head
    val isSpark4Compatible = constructor.getParameterCount > 8
    if (isSpark4Compatible) {
      constructor.newInstance(
        staticObject,
        dataType,
        functionName,
        arguments,
        Nil, // inputTypes
        Boolean.box(propagateNull),
        Boolean.box(returnNullable),
        Boolean.box(isDeterministic),
        Option.empty, // scalarFunction - added in Spark 4.0
      ).asInstanceOf[StaticInvoke]
    } else {
      StaticInvoke(
        staticObject,
        dataType,
        functionName,
        arguments,
        inputTypes = Nil,
        propagateNull,
        returnNullable,
        isDeterministic,
      )
    }
  }
}
