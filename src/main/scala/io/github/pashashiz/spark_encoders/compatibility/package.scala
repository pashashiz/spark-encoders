package io.github.pashashiz.spark_encoders

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.objects.StaticInvoke
import org.apache.spark.sql.types.DataType
import scala.util.Try

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

  /**
   * Shim for ClosureCleaner.clean that handles different implementations across Spark versions.
   * 
   * - Spark 3.x: Uses ClosureCleaner.clean(closure: AnyRef, checkSerializable: Boolean, cleanTransitively: Boolean): Unit
   * - Spark 4.x: Introduces SparkClosureCleaner.clean(closure: AnyRef, checkSerializable: Boolean, cleanTransitively: Boolean): Unit
   *              which internally uses a new version of ClosureCleaner.clean.
   */
  def cleanClosure(
    closure: AnyRef,
    checkSerializable: Boolean = true,
    cleanTransitively: Boolean = true
  ): Unit = {
    val cleanerClass = Try {
      // Try Spark 4.x first - check if SparkClosureCleaner exists
      Class.forName("org.apache.spark.util.SparkClosureCleaner$")
    }.recover { case _: ClassNotFoundException =>
      // Fall back to Spark 3.5
      Class.forName("org.apache.spark.util.ClosureCleaner$")
    }.get

     val companionObject = cleanerClass.getField("MODULE$").get(null)
     val cleanMethod = cleanerClass.getDeclaredMethod(
       "clean",
        classOf[AnyRef], classOf[Boolean], classOf[Boolean]
     )

     cleanMethod.invoke(companionObject, closure, Boolean.box(checkSerializable), Boolean.box(cleanTransitively))
  }
}
