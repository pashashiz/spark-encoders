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

  /**
   * Shim for ClosureCleaner.clean that handles different implementations across Spark versions.
   * 
   * - Spark 3.x: Uses ClosureCleaner.clean(closure: AnyRef, checkSerializable: Boolean, cleanTransitively: Boolean): Unit
   * - Spark 4.x: Introduces SparkClosureCleaner.clean(closure: AnyRef, checkSerializable: Boolean, cleanTransitively: Boolean): Unit
   *              which internally uses the new ClosureCleaner.clean signature and handles return values/serialization
   */
  def closureCleanerClean(
    closure: AnyRef,
    checkSerializable: Boolean = true,
    cleanTransitively: Boolean = true
  ): Unit = {
    try {
      // Try Spark 4.x first - check if SparkClosureCleaner exists
      val sparkClosureCleanerClass = Class.forName("org.apache.spark.util.SparkClosureCleaner$")
      val companionObject = sparkClosureCleanerClass.getField("MODULE$").get(null)
      val cleanMethod = sparkClosureCleanerClass.getDeclaredMethod("clean", 
        classOf[AnyRef], classOf[Boolean], classOf[Boolean])
      
      cleanMethod.setAccessible(true)
      cleanMethod.invoke(companionObject, closure, Boolean.box(checkSerializable), Boolean.box(cleanTransitively))
      
    } catch {
      case _: ClassNotFoundException =>
        // Fall back to Spark 3.x ClosureCleaner
        try {
          val closureCleanerClass = Class.forName("org.apache.spark.util.ClosureCleaner$")
          val companionObject = closureCleanerClass.getField("MODULE$").get(null)
          val cleanMethod = closureCleanerClass.getDeclaredMethod("clean", 
            classOf[AnyRef], classOf[Boolean], classOf[Boolean])
          
          cleanMethod.setAccessible(true)
          cleanMethod.invoke(companionObject, closure, Boolean.box(checkSerializable), Boolean.box(cleanTransitively))
          
        } catch {
          case e: Exception =>
            throw new RuntimeException(s"Failed to access ClosureCleaner.clean via reflection: ${e.getMessage}", e)
        }
      case e: Exception =>
        throw new RuntimeException(s"Failed to access SparkClosureCleaner.clean via reflection: ${e.getMessage}", e)
    }
  }
}
