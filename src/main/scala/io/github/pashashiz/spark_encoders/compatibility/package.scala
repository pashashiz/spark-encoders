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
   * Shim for ClosureCleaner.clean that handles different method signatures across Spark versions.
   * Databricks often backports changes, so method signatures may vary between versions.
   */
  def closureCleanerClean(
    closure: AnyRef,
    checkSerializable: Boolean = true,
    cleanTransitively: Boolean = true
  ): Unit = {
    try {
      // Try to get the ClosureCleaner companion object and clean method via reflection
      val closureCleanerClass = Class.forName("org.apache.spark.util.ClosureCleaner$")
      val companionObject = closureCleanerClass.getField("MODULE$").get(null)
      
      // Try different method signatures to handle version compatibility
      val methods = closureCleanerClass.getDeclaredMethods.filter(_.getName == "clean")
      
      val method = methods.find { m =>
        val paramTypes = m.getParameterTypes
        paramTypes.length == 3 &&
        paramTypes(0) == classOf[AnyRef] &&
        paramTypes(1) == classOf[Boolean] &&
        paramTypes(2) == classOf[Boolean]
      }.getOrElse {
        // Fallback to first clean method if exact signature not found
        methods.head
      }
      
      method.setAccessible(true)
      method.invoke(companionObject, closure, Boolean.box(checkSerializable), Boolean.box(cleanTransitively))
    } catch {
      case e: Exception =>
        // If reflection fails, throw the original exception since ClosureCleaner is package-private
        throw new RuntimeException(s"Failed to access ClosureCleaner.clean via reflection: ${e.getMessage}", e)
    }
  }
}
