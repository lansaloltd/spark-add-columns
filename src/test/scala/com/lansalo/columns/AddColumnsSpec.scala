package com.lansalo.columns

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.scalatest.{BeforeAndAfterAll, Matchers, WordSpec}

class AddColumnsSpec extends WordSpec with Matchers with BeforeAndAfterAll {

  import AddColumnsSpecFixture._

  override def afterAll(): Unit = {
    super.afterAll()
    spark.stop()
  }

  "warmup add columns" should {
      "do a bit of initialisation" in {
        val start = System.currentTimeMillis()
        val result = addColumnsFold(incipitDF, add4Columns)
        println("Done warmup in: " + (System.currentTimeMillis() - start))
    }
  }

  "addColumns via map" should {
    "add columns" when {
      "pass 4 new columns to be added map" in {
        val start = System.currentTimeMillis()
        val result = addColumnsViaMap(incipitDF, add4Columns)
        result.take(1)
        println("Done 4 map in: " + (System.currentTimeMillis() - start))
      }
      "pass 20 new columns to be added map" in {
        val start = System.currentTimeMillis()
        val result = addColumnsViaMap(incipitDF, add20Columns)
        result.take(1)
        println("Done 20 map in: " + (System.currentTimeMillis() - start))
      }
      "pass 40 new columns to be added map" in {
        val start = System.currentTimeMillis()
        val result = addColumnsViaMap(incipitDF, add40Columns)
        result.take(1)
        println("Done 40 map in: " + (System.currentTimeMillis() - start))
      }
      "pass 100 new columns to be added map" in {
        val start = System.currentTimeMillis()
        val result = addColumnsViaMap(incipitDF, add100Columns)
        result.take(1)
        println("Done 100 map in: " + (System.currentTimeMillis() - start))
      }
      "pass 200 new columns to be added map" in {
        val start = System.currentTimeMillis()
        val result = addColumnsViaMap(incipitDF, add200Columns)
        result.take(1)
        println("Done 200 map in: " + (System.currentTimeMillis() - start))
      }
      "pass 400 new columns to be added map" in {
        val start = System.currentTimeMillis()
        val result = addColumnsViaMap(incipitDF, add400Columns)
        result.take(1)
        println("Done 400 map in: " + (System.currentTimeMillis() - start))
      }
    }
  }

  "addColumnsFold" should {
    "add columns" when {
      "pass 4 new columns to be added" in {
        val start = System.currentTimeMillis()
        val result = addColumnsFold(incipitDF, add4Columns)
        result.take(1)
        println("Done 4 fold in: " + (System.currentTimeMillis() - start))
      }
      "pass 20 new columns to be added" in {
        val start = System.currentTimeMillis()
        val result = addColumnsFold(incipitDF, add20Columns)
        result.take(1)
        println("Done 20 fold in: " + (System.currentTimeMillis() - start))
      }
      "pass 40 new columns to be added" in {
        val start = System.currentTimeMillis()
        val result = addColumnsFold(incipitDF, add40Columns)
        result.take(1)
        println("Done 40 fold in: " + (System.currentTimeMillis() - start))
      }
      "pass 100 new columns to be added" in {
        val start = System.currentTimeMillis()
        val result = addColumnsFold(incipitDF, add100Columns)
        result.take(1)
        println("Done 100 fold in: " + (System.currentTimeMillis() - start))
      }
      "pass 200 new columns to be added" in {
        val start = System.currentTimeMillis()
        val result = addColumnsFold(incipitDF, add200Columns)
        result.take(1)
        println("Done 200 fold in: " + (System.currentTimeMillis() - start))
      }
      "pass 400 new columns to be added" in {
        val start = System.currentTimeMillis()
        val result = addColumnsFold(incipitDF, add400Columns)
        result.take(1)
        println("Done 400 fold in: " + (System.currentTimeMillis() - start))
      }
    }
  }

}

object AddColumnsSpecFixture {

  val spark = SparkSession.builder().appName("addCols").master("local").getOrCreate()
  import spark.implicits._

  val incipitDF = Seq(
    (1, "Mark Twain", "Adventures of Huckleberry Finn","""You don't know about me without you have read a book called "The Adventures of Tom Sawyer," but that ain't no matter."""),
    (2, "Edith Wharton", "The Age of Innocence", """On a January evening of the early seventies, Christine Nilsson was singing in Faust at the Academy of Music in New York."""),
    (3, "Robert Penn Warren", "All the King's Men", "To get there you follow Highway 58, going northeast out of the city, and it is a good highway and new."),
    (4, "Friedrich Nietzsche", "Thus Spoke Zarathustra", "When Zarathustra was thirty years old, he left his home and the lake of his home, and went into the mountains."),
    (5, "George Orwell", "Animal Farm", "Mr. Jones, of the Manor Farm, had locked the hen-houses for the night, but was too drunk to remember to shut the pop-holes."),
    (6, "Leo Tolstoy", "Anna Karenina", "Happy families are all alike; every unhappy family is unhappy in its own way.")
  ).toDF("id", "author", "title", "incipit").cache()

  val add4Columns: List[String] = List("to", "get", "alike", "with")

  val add8Columns: List[String] = add4Columns ++ List("man", "in", "anna", "was")

  val add20Columns: List[String] = add8Columns ++ List("happy", "pig", "way", "morning", "city", "when", "two", "drunk", "pop", "John", "four", "hence")

  val add40Columns: List[String] = add20Columns ++ add20Columns.map(_ + "ing")

  val add100Columns: List[String] = add20Columns ++ add40Columns.map(_ + "b") ++ add40Columns.map(_ + "bc")

  val add200Columns: List[String] = add100Columns ++ add100Columns.map(_ + "o")

  val add400Columns: List[String] = add200Columns ++ add200Columns.map(_ + "ed")
}


