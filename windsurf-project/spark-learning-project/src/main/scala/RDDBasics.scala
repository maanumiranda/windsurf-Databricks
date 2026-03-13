import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object RDDBasics {
  
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("RDD Basics")
      .setMaster("local[*]")
    
    val sc = new SparkContext(conf)
    
    try {
      // Run RDD examples
      rddCreationExamples(sc)
      rddTransformationExamples(sc)
      rddActionExamples(sc)
      wordCountExample(sc)
      
    } catch {
      case e: Exception =>
        println(s"Error: ${e.getMessage}")
        e.printStackTrace()
        System.exit(1)
    } finally {
      sc.stop()
    }
  }
  
  def rddCreationExamples(sc: SparkContext): Unit = {
    println("=== RDD Creation Examples ===")
    
    // Create RDD from a list
    val numbersRDD: RDD[Int] = sc.parallelize(List(1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
    println(s"Numbers RDD: ${numbersRDD.collect().mkString(", ")}")
    
    // Create RDD from range
    val rangeRDD: RDD[Int] = sc.parallelize(1 to 10)
    println(s"Range RDD: ${rangeRDD.collect().mkString(", ")}")
    
    // Create RDD with specific number of partitions
    val partitionedRDD: RDD[Int] = sc.parallelize(1 to 100, numSlices = 4)
    println(s"Partitioned RDD partitions: ${partitionedRDD.getNumPartitions}")
    
    // Create RDD from text (create sample data)
    val sampleText = List("Hello Spark", "RDD operations", "Learning Apache Spark")
    val textRDD: RDD[String] = sc.parallelize(sampleText)
    println(s"Text RDD: ${textRDD.collect().mkString(", ")}")
  }
  
  def rddTransformationExamples(sc: SparkContext): Unit = {
    println("\n=== RDD Transformation Examples ===")
    
    val numbersRDD = sc.parallelize(1 to 10)
    
    // Map transformation
    val squaredRDD: RDD[Int] = numbersRDD.map(x => x * x)
    println(s"Squared numbers: ${squaredRDD.collect().mkString(", ")}")
    
    // Filter transformation
    val evenRDD: RDD[Int] = numbersRDD.filter(x => x % 2 == 0)
    println(s"Even numbers: ${evenRDD.collect().mkString(", ")}")
    
    // FlatMap transformation
    val wordsRDD: RDD[String] = numbersRDD.flatMap(x => List(x, x * 2))
    println(s"FlatMap result: ${wordsRDD.collect().mkString(", ")}")
    
    // Map and filter chain
    val processedRDD: RDD[Int] = numbersRDD
      .filter(x => x > 5)
      .map(x => x * 10)
    println(s"Processed numbers: ${processedRDD.collect().mkString(", ")}")
    
    // Union transformation
    val moreNumbers = sc.parallelize(11 to 15)
    val unionRDD: RDD[Int] = numbersRDD.union(moreNumbers)
    println(s"Union result: ${unionRDD.collect().mkString(", ")}")
    
    // Intersection transformation
    val commonNumbers = sc.parallelize(5 to 15)
    val intersectionRDD: RDD[Int] = numbersRDD.intersection(commonNumbers)
    println(s"Intersection result: ${intersectionRDD.collect().mkString(", ")}")
    
    // Distinct transformation
    val duplicateNumbers = sc.parallelize(List(1, 2, 2, 3, 3, 3, 4, 5))
    val distinctRDD: RDD[Int] = duplicateNumbers.distinct()
    println(s"Distinct result: ${distinctRDD.collect().mkString(", ")}")
  }
  
  def rddActionExamples(sc: SparkContext): Unit = {
    println("\n=== RDD Action Examples ===")
    
    val numbersRDD = sc.parallelize(1 to 10)
    
    // Collect action
    val collected: Array[Int] = numbersRDD.collect()
    println(s"Collected: ${collected.mkString(", ")}")
    
    // Count action
    val count: Long = numbersRDD.count()
    println(s"Count: $count")
    
    // Reduce action
    val sum: Int = numbersRDD.reduce((a, b) => a + b)
    println(s"Sum: $sum")
    
    // First action
    val first: Int = numbersRDD.first()
    println(s"First element: $first")
    
    // Take action
    val firstThree: Array[Int] = numbersRDD.take(3)
    println(s"First three elements: ${firstThree.mkString(", ")}")
    
    // TakeOrdered action
    val topThree: Array[Int] = numbersRDD.takeOrdered(3)(Ordering[Int].reverse)
    println(s"Top three elements: ${topThree.mkString(", ")}")
    
    // Foreach action
    println("Foreach action (printing each element):")
    numbersRDD.foreach(x => print(s"$x "))
    println()
    
    // Count by value
    val duplicateNumbers = sc.parallelize(List(1, 2, 2, 3, 3, 3, 4, 5))
    val valueCounts: collection.Map[Int, Long] = duplicateNumbers.countByValue()
    println(s"Value counts: $valueCounts")
  }
  
  def wordCountExample(sc: SparkContext): Unit = {
    println("\n=== Word Count Example ===")
    
    // Sample text
    val text = List(
      "hello world",
      "hello spark", 
      "spark is great",
      "hello again",
      "spark programming"
    )
    
    val textRDD: RDD[String] = sc.parallelize(text)
    
    // Word count logic
    val counts: RDD[(String, Int)] = textRDD
      .flatMap(line => line.split(" "))
      .map(word => (word.toLowerCase, 1))
      .reduceByKey((a, b) => a + b)
      .sortBy(tuple => tuple._2, ascending = false)
    
    println("Word counts:")
    counts.collect().foreach { case (word, count) =>
      println(s"$word: $count")
    }
    
    // More advanced word count with filtering
    val filteredCounts: RDD[(String, Int)] = textRDD
      .flatMap(line => line.toLowerCase.split("\\W+"))
      .filter(word => word.nonEmpty && word.length > 3)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .sortBy(_._2, ascending = false)
    
    println("\nFiltered word counts (words longer than 3 characters):")
    filteredCounts.collect().foreach { case (word, count) =>
      println(s"$word: $count")
    }
  }
}
