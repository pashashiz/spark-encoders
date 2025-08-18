package org.apache.spark.util

object PrivateClosureCleaner {

  /** Clean the given closure in place.
    *
    * More specifically, this renders the given closure serializable as long as it does not
    * explicitly reference unserializable objects.
    *
    * @param closure           the closure to clean
    * @param checkSerializable whether to verify that the closure is serializable after cleaning
    * @param cleanTransitively whether to clean enclosing closures transitively
    */
  def clean(
      closure: AnyRef,
      checkSerializable: Boolean = true,
      cleanTransitively: Boolean = true): Unit =
    _root_.io.github.pashashiz.spark_encoders.compatibility.closureCleanerClean(closure, checkSerializable, cleanTransitively)
}
