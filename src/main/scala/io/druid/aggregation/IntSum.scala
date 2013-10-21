package io.druid.aggregation

import com.fasterxml.jackson.annotation.JsonProperty

class IntSum(@JsonProperty("name") name: String,
             @JsonProperty("fieldName") fieldName: String)
  extends MonoidAggregatorFactory[Int](name, fieldName, CacheKeys.IntSum, Sum)

object Sum extends Monoid[Int] {
  final val identity = 0

  def apply(a: Int, b: Int) = a + b
}

