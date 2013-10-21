package io.druid.aggregation

import java.nio.ByteBuffer
import java.util.Comparator
import io.druid.segment.{ObjectColumnSelector, ColumnSelectorFactory}
import io.druid.query.aggregation.{BufferAggregator, Aggregator, AggregatorFactory}

class MonoidAggregatorFactory[T](outputFieldName: String,
                                inputFieldName: String,
                                cacheTypeId: Byte,
                                m: Monoid[T])(implicit val ordering: Ordering[T], val codec: BufferCodec[T]) extends AggregatorFactory {

  final val Comparator = new Comparator[T] {
    def compare(o1: T, o2: T) = ordering.compare(o1, o2)
  }

  def getComparator = Comparator

  def factorize(metricFactory: ColumnSelectorFactory): Aggregator = {
    val selector = metricFactory.makeObjectColumnSelector(inputFieldName).asInstanceOf[ObjectColumnSelector[T]]
    assert(selector != null, "No complex selector available for " + inputFieldName)
    new MonoidAggeator[T](
      outputFieldName,
      selector,
      m
    )
  }

  def factorizeBuffered(metricFactory: ColumnSelectorFactory): BufferAggregator = {
    val selector = metricFactory.makeObjectColumnSelector(inputFieldName).asInstanceOf[ObjectColumnSelector[T]]
    assert(selector != null, "No complex selector available for " + inputFieldName)
    new MonoidBufferAggregator(
      selector,
      m
    )
  }

  def combine(lhs: AnyRef, rhs: AnyRef): AnyRef = {
    m(lhs.asInstanceOf[T], rhs.asInstanceOf[T]).asInstanceOf[AnyRef]
  }

  def getCombiningFactory: AggregatorFactory = new MonoidAggregatorFactory(outputFieldName, inputFieldName, cacheTypeId, m)

  def deserialize(o: AnyRef): AnyRef = o

  def finalizeComputation(o: AnyRef): AnyRef = codec.asQueryResult(o.asInstanceOf[T])

  def getName: String = outputFieldName

  def requiredFields: java.util.List[String] = java.util.Arrays.asList(inputFieldName)

  def getCacheKey: Array[Byte] = {
    val fieldNameBytes = inputFieldName.getBytes
    ByteBuffer.allocate(1 + fieldNameBytes.length).put(cacheTypeId).put(fieldNameBytes).array()
  }

  def getTypeName: String = codec.typeName

  def getMaxIntermediateSize = codec.maxIntermediateByteSize

  def getAggregatorStartValue: AnyRef = m.identity.asInstanceOf[AnyRef]

}

class MonoidAggeator[T](final val name: String, selector: ObjectColumnSelector[T], m: Monoid[T])(implicit val codec: BufferCodec[T]) extends Aggregator {
  private[this] var value = m.identity

  def aggregate() { value = m.apply(value, selector.get()) }

  def reset() { value = m.identity }

  def get(): AnyRef = value.asInstanceOf[AnyRef]

  def getName: String = name

  override def clone = new MonoidAggeator[T](name, selector, m)

  def getFloat: Float = throw new UnsupportedOperationException("This aggregator only supports complex metrics")

  def close() {}
}

class MonoidBufferAggregator[T](selector: ObjectColumnSelector[T], m: Monoid[T])(implicit val codec: BufferCodec[T]) extends BufferAggregator {
  def init(buf: ByteBuffer, position: Int) {
    codec.write(buf, position, m.identity)
  }

  def aggregate(buf: ByteBuffer, position: Int) {
    val a = codec.read(buf, position)
    val selected = selector.get()
    val value = m(a, selected)
    codec.write(buf, position, value)
  }

  def get(buf: ByteBuffer, position: Int): AnyRef = codec.read(buf, position).asInstanceOf[AnyRef]

  def getFloat(buf: ByteBuffer, position: Int): Float = throw new UnsupportedOperationException("This aggregator only supports complex metrics")

  def close() { }
}

