package io.druid.aggregation

import java.nio.ByteBuffer
import com.fasterxml.jackson.annotation.JsonProperty
import com.twitter.algebird.{HyperLogLog, HLL, HyperLogLogMonoid}

case class CardinalityAggregator[T](typeName: String, fromString: String => T, toBytes: T => Array[Byte]) extends Monoid[HLL] {
  final val m = new HyperLogLogMonoid(bits = 12)
  def identity: HLL = m.zero

  def apply(a: HLL, b: HLL): HLL = m.plus(a, b)

  implicit object Ordering extends Ordering[HLL] {
    // Doesn't really make sense, but keep it consistent
    def compare(x: HLL, y: HLL) = x.estimatedSize.compareTo(y.estimatedSize)
  }

  implicit object Codec extends BufferCodec[HLL] {

    def asQueryResult(value: HLL): AnyRef = value.estimatedSize.asInstanceOf[AnyRef]

    def write(buf: ByteBuffer, position: Int, value: HLL) {

      val oldPos = buf.position()
      val bytes = HyperLogLog.toBytes(value)
      buf.position(position)

      buf.putInt(bytes.length)
      buf.put(bytes)

      buf.position(oldPos)
    }

    def read(buf: ByteBuffer, position: Int) = {
      val oldPos = buf.position()
      buf.position(position)

      val length = buf.getInt
      val bytes = Array.ofDim[Byte](length)
      buf.get(bytes, 0, length)
      val hll = HyperLogLog.fromBytes(bytes)

      buf.position(oldPos)
      hll
    }

    final val maxIntermediateByteSize = math.pow(2, m.bits).toInt + 4 + 4

    override def typeName = CardinalityAggregator.this.typeName
  }

  implicit val SerDe = MetricSerde(typeName, m.zero, stringValue => { if (stringValue == null) m.zero else m.apply(fromString(stringValue))(toBytes) }, new ObjectCodec[HLL](Codec))

}

object CardinalityAggregator {
  val String = CardinalityAggregator[String]("stringCardinality", s => s, _.getBytes("UTF-8"))
}


class StringCardinalityAggregator(@JsonProperty("name") name: String,
                 @JsonProperty("fieldName") fieldName: String)
  extends MonoidAggregatorFactory[HLL](name, fieldName, CacheKeys.StringCardinality, CardinalityAggregator.String)(
    ordering = CardinalityAggregator.String.Ordering, codec = CardinalityAggregator.String.Codec
  )
