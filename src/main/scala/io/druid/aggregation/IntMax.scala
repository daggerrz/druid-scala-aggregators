package io.druid.aggregation

import com.fasterxml.jackson.annotation.JsonProperty

class IntMax(@JsonProperty("name") name: String,
             @JsonProperty("fieldName") fieldName: String)
  extends MonoidAggregatorFactory[Int](name, fieldName, CacheKeys.IntMax, Max)

object Max extends Monoid[Int] {
  final val identity = Int.MinValue

  def apply(a: Int, b: Int) = math.max(a, b)
}
