package com.lansalo.columns

import java.io.File

import org.scalameter.api.{Bench, Gen}
import org.scalameter.persistence.SerializationPersistor

class AddColumnsBenchmark extends Bench.OfflineReport {

  import AddColumnsSpecFixture._

  override lazy val persistor = SerializationPersistor(new File("target/add-columns/results"))

  val ranges: Gen[Int] = Gen.range("size")(10, 1000, 20)

  val columnsNameGen: Gen[List[String]] = for {
    size <- ranges
  } yield (0 until size).toList.map(n => s"word_$n")

  performance of "columnsNameGen" in {
    measure method "addColumnsViaMap" in {
      using(columnsNameGen) in {
        r => evaluate(addColumnsViaMap(incipitDF, r))
      }
    }
  }

  performance of "columnsNameGen" in {
    measure method "addColumnsFold" in {
      using(columnsNameGen) in {
        r => evaluate(addColumnsFold(incipitDF, r))
      }
    }
  }

}
