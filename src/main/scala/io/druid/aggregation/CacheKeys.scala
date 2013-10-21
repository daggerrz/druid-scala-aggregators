package io.druid.aggregation

object CacheKeys {
  final val IntMax : Byte = 0x07
  final val IntMin : Byte = 0x08
  final val IntSum : Byte = 0x09
  final val IntAverage : Byte = 0x0a
  final val StringCardinality : Byte = 0x0b
}
