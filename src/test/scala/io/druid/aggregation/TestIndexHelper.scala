package io.druid.aggregation

import scala.collection.JavaConverters._
import com.google.common.io.Files
import com.google.common.base.Supplier
import java.nio.{ByteOrder, ByteBuffer}
import org.joda.time.Interval
import io.druid.query.dimension.{DimensionSpec, DefaultDimensionSpec}
import io.druid.segment.incremental.{IncrementalIndexSchema, IncrementalIndex}
import io.druid.segment.{Segment, IndexIO, IndexMerger, QueryableIndex}
import io.druid.query.aggregation.{PostAggregator, AggregatorFactory}
import io.druid.query.groupby.{GroupByQueryConfig, GroupByQuery, GroupByQueryEngine}
import io.druid.data.input.Row
import io.druid.collections.StupidPool
import io.druid.query.spec.QuerySegmentSpecs
import io.druid.granularity.{DurationGranularity, AllGranularity}
import io.druid.data.input.impl.{StringInputRowParser, TimestampSpec, SpatialDimensionSchema, JSONDataSpec}

object TestIndexHelper {
  val startTime = 0L
  val granularity = new DurationGranularity(60 * 1000, startTime)
  val dimensions = List("gender", "age").asJava
  val dimensionExcludes = List.empty[String].asJava
  val data = new JSONDataSpec(dimensions, List.empty[SpatialDimensionSchema].asJava)

  val time = new TimestampSpec("utcdt", "iso")
  val rowParser = new StringInputRowParser(time, data, dimensionExcludes)

  /**
   * Build an incremental index and add example data from users.json to it.
   */
  def buildIncrementalIndex(aggregations: Seq[AggregatorFactory]) : IncrementalIndex = {
    val index = new IncrementalIndex(
      new IncrementalIndexSchema.Builder()
        .withMinTimestamp(startTime)
        .withSpatialDimensions(data.getSpatialDimensions)
        .withQueryGranularity(granularity)
        .withMetrics(aggregations.toArray)
        .build()
    )

    val in = getClass.getResourceAsStream("/users.json")
    scala.io.Source.fromInputStream(in, "UTF-8").getLines().foreach { line => index.add(rowParser.parse(line)) }
    index
  }

  /**
   * Persist the index and load it back in. Ensures that the on-disk format works.
   */
  def persistAndLoadIndex(index: IncrementalIndex) : QueryableIndex = {
    val indexDir = Files.createTempDir()
    IndexMerger.persist(index, indexDir)
    IndexIO.loadIndex(indexDir)
  }

  /**
   * Executes a GroupBy query given segment and returns the raw rows.
   */
  def querySegment(segment: Segment, dimensions: Seq[String], aggregators: Seq[AggregatorFactory]): com.metamx.common.guava.Sequence[Row] = {

    val queryEngine: GroupByQueryEngine = {
      val config = new GroupByQueryConfig {
        override def getMaxIntermediateRows: Int = 5
      }
      val supplier = new Supplier[ByteBuffer]() {
        def get(): ByteBuffer = ByteBuffer.allocateDirect(0xFFFF).order(ByteOrder.LITTLE_ENDIAN)
      }
      val pool = new StupidPool[ByteBuffer](supplier)
      new GroupByQueryEngine(new Supplier[GroupByQueryConfig] {
        def get(): GroupByQueryConfig = config
      }, pool)
    }

    val query = new GroupByQuery(
      "example-source",
      QuerySegmentSpecs.create(new Interval(0, System.currentTimeMillis())),
      null,
      new AllGranularity,
      dimensions.map(n => new DefaultDimensionSpec(n, n)).toList.asInstanceOf[List[DimensionSpec]].asJava,
      aggregators.toList.asJava,
      List.empty[PostAggregator].asJava,
      null, // havingSpec,
      null, // limitSpec,
      null, // orderbySpec
      null // context
    )
    val rows: com.metamx.common.guava.Sequence[Row] = queryEngine.process(query, segment.asStorageAdapter())
    rows
  }

}
