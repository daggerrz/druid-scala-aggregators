package io.druid.aggregation

import java.nio.ByteBuffer
import com.fasterxml.jackson.annotation.JsonProperty


object Average extends Monoid[Average] {

  final val identity = new Average(0, 0)

  def apply(a: Average, b: Average) = Average(a.count + b.count, a.sum + b.sum)

  implicit object AverageOrdering extends Ordering[Average] {
    def compare(x: Average, y: Average) = x.avg.compareTo(y.avg)
  }

  implicit object AverageCodec extends BufferCodec[Average] with FloatRepresentation[Average] {

    def asQueryResult(value: Average): AnyRef = value.avg.asInstanceOf[AnyRef]

    def read(buf: ByteBuffer, position: Int) = {
      new Average(
        count = buf.getInt(position),
        sum = buf.getInt(position + 4)
      )
    }

    def write(buf: ByteBuffer, position: Int, value: Average) {
      buf.putInt(position, value.count)
      buf.putInt(position + 4, value.sum)
    }

    def fromFloat(f: Float) = new Average(count = 1, sum = f.toInt)

    def toFloat(value: Average) = value.avg

    def maxIntermediateByteSize: Int = 8

    override def typeName = "average"
  }

  implicit val SerDe = MetricSerde(AverageCodec.typeName, identity, string => Average(count = 1, sum = string.toInt), new ObjectCodec[Average](AverageCodec))

}

class IntAverage(@JsonProperty("name") name: String,
                 @JsonProperty("fieldName") fieldName: String)
  extends MonoidAggregatorFactory[Average](name, fieldName, CacheKeys.IntAverage, Average)

case class Average(count: Int, sum: Int) {
  def avg = if (count == 0) 0 else sum / count
}


