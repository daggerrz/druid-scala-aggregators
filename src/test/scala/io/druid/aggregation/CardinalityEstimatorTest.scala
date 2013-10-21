package io.druid.aggregation

import com.metamx.common.guava.Accumulator
import org.scalatest.FlatSpec
import org.scalatest.matchers.ShouldMatchers
import io.druid.segment.serde.ComplexMetrics
import io.druid.query.aggregation.AggregatorFactory
import io.druid.data.input.{MapBasedRow, Row}
import io.druid.segment.{QueryableIndexSegment, IncrementalIndexSegment}
import com.twitter.algebird.HLL

class CardinalityEstimatorTest extends FlatSpec with ShouldMatchers {
  import TestIndexHelper._

  ComplexMetrics.registerSerde(CardinalityAggregator.String.Codec.typeName, CardinalityAggregator.String.SerDe)

  val indexAggregations : Seq[AggregatorFactory] = Array(
    new StringCardinalityAggregator(fieldName = "name", name = "unique_names")
  )

  val computeAggregations : Seq[AggregatorFactory] = Array(
    new StringCardinalityAggregator(fieldName = "unique_names", name = "unique_names")
  )

  val nameAccumulator = new Accumulator[HLL, Row] {
    def accumulate(accumulated: HLL, in: Row): HLL = {
      val mapRow = in.asInstanceOf[MapBasedRow]
      val uniques = mapRow.getEvent.get("unique_names").asInstanceOf[HLL]
      CardinalityAggregator.String.m.plus(accumulated, uniques)
    }
  }


  "Cardinality estimator" should "work as a regular aggregator" in {
    val segment = new IncrementalIndexSegment(buildIncrementalIndex(indexAggregations))
    val rows = querySegment(
      segment,
      dimensions = Nil,
      aggregators = computeAggregations
    )
    val results = rows.accumulate[HLL](CardinalityAggregator.String.identity, nameAccumulator)
    // John, Lisa, Peter,
    results.estimatedSize should be (3.0 plusOrMinus 0.1)
  }

  it should "work as a  buffered aggregator" in {
    val rows = querySegment(
      segment = new QueryableIndexSegment("segmentId", persistAndLoadIndex(buildIncrementalIndex(indexAggregations))),
      dimensions = Nil,
      aggregators = computeAggregations
    )
    val results = rows.accumulate[HLL](CardinalityAggregator.String.identity, nameAccumulator)
    // John, Lisa, Peter,
    results.estimatedSize should be(3.0 plusOrMinus 0.1)
  }


}
