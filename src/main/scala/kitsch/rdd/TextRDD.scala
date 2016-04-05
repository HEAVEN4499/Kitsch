package kitsch.rdd

import com.typesafe.scalalogging.slf4j.LazyLogging
import kitsch._

import scala.io.Source

private[kitsch] class TextRDD(kitsch: Kitsch, filePath: String)
  extends RDD[String](kitsch) with LazyLogging {
  override def compute(): Iterator[String] = new NextIterator[String] {
    var reader = read

    override protected def close() {
      if (reader != null) {
        try {
          reader.close()
        } catch {
          case e: Exception =>
        } finally {
          reader = null
        }
      }
    }

    override protected def getNext(): String = {
      try {
        val value = reader.readLine()
        if (value == null) {
          finished = true
          null
        } else {
          reader.readLine()
        }
      } catch {
        case eof: Exception =>
          finished = true
          null
      }
    }
  }

  override def cache = {
    kitsch.makeRDD(iterator().toArray.toSeq)
  }

  def read = Source.fromFile(filePath).bufferedReader
}

private[kitsch] abstract class NextIterator[U] extends Iterator[U] {

  private var gotNext = false
  private var nextValue: U = _
  private var closed = false
  protected var finished = false

  protected def getNext(): U

  protected def close()

  def closeIfNeeded() {
    if (!closed) {
      // Note: it's important that we set closed = true before calling close(), since setting it
      // afterwards would permit us to call close() multiple times if close() threw an exception.
      closed = true
      close()
    }
  }

  override def hasNext: Boolean = {
    if (!finished) {
      if (!gotNext) {
        nextValue = getNext()
        if (finished) {
          closeIfNeeded()
        }
        gotNext = true
      }
    }
    !finished
  }

  override def next(): U = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    gotNext = false
    nextValue
  }
}