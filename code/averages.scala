
trait Animal extends Product with Serializable
case class ProductCase(score:Double,review:String) extends Animal
//val product1 = new ProductCase(88.0, "review1") 
val product2 = new ProductCase(95.0, "review2")

val product1 : Product = (88.0, "review1") 
val product2 : Product = (95.0, "review2")
val product3 : Product = (91.0, "review3") 
val product4 : Product = (93.0, "review4")
val product5 : Product = (95.0, "review5") 
val product6 : Product = (98.0, "review6")


val initialScores = Array(("Fred", product1), ("Fred", product2), ("Fred", product3), ("Wilma", product4), ("Wilma", product5), ("Wilma", product6))
val wilmaAndFredScores = sc.parallelize(initialScores).cache()


type ScoreCollector = (Int, Product) 
type PersonScores = (String, (Int, Product))


//val initialScores = Array(("Fred", (88.0, "review")), ("Fred", (95.0,"review")), ("Fred", (91.0,"review")), ("Wilma", (93.0,"review")), ("Wilma", (95.0,"review")), ("Wilma", (98.0,"review")))


val createScoreCombiner = (score: Product) => (1, score)

val scoreCombiner = (collector: ScoreCollector, score: Product) => {
         val (numberScores, (product)) = collector
         val product1score = java.lang.Double.parseDouble(product.productElement(0).toString);
         val product2score = java.lang.Double.parseDouble(score.productElement(0).toString);
        val review = (if (product1score > product2score) product.productElement(1).toString else score.productElement(1).toString)
        val productResult : Product = (product1score + product2score, review)
        (numberScores + 1, productResult)
      }

val scoreMerger = (collector1: ScoreCollector, collector2: ScoreCollector) => {
      
      val (numScores1, (product1)) = collector1
      val (numScores2, (product2)) = collector2

      val product1score = java.lang.Double.parseDouble(product1.productElement(0).toString);
      val product2score = java.lang.Double.parseDouble(product2.productElement(0).toString);
      val product1review = product1.productElement(1).toString;
      val product2review = product2.productElement(1).toString;

      val review = (if (product1score > product2score) product1review else product2review)
      val productResult : Product = (product1score + product2score, review)
      (numScores1 + numScores2, productResult)
    }
val scores = wilmaAndFredScores.combineByKey(createScoreCombiner, scoreCombiner, scoreMerger)

val averagingFunction = (personScore: PersonScores) => {
       val (name, (numberScores, product)) = personScore
       val totalScore = java.lang.Double.parseDouble(product.productElement(0).toString);
       val review = product.productElement(1).toString;
       (name, totalScore / numberScores, review)
    }

val averageScores = scores.collectAsMap().map(averagingFunction)

println("Average Scores using CombingByKey")
    averageScores.foreach((ps) => {
      val(name,average) = ps
       println(name+ "'s average score : " + average)
    })