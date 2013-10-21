package io.druid.aggregation

trait Monoid[T] {
  def identity: T
  def apply(a: T, b: T): T
}


