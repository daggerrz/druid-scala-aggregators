package io.druid.aggregation

import com.fasterxml.jackson.annotation.JsonProperty

class IntMin(@JsonProperty("name") name: String,
             @JsonProperty("fieldName") fieldName: String)
  extends MonoidAggregatorFactory[Int](name, fieldName, CacheKeys.IntMin, Min)

object Min extends Monoid[Int] {
  final val identity = Int.MaxValue

  def apply(a: Int, b: Int) = math.min(a, b)
}
