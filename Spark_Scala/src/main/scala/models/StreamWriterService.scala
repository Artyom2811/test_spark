package models

import org.apache.spark.sql.ForeachWriter

trait StreamWriterService[T] extends ForeachWriter[(Seq[T], Int, Long)] {

  override def open(partitionId: Long, version: Long): Boolean = true

  def process(records: (Seq[T], Int, Long)): Unit

  override def close(errorOrNull: Throwable): Unit = ()
}
