
case class ProductCase(score:Double,review:String) extends Product with Serializable

val product1 = new ProductCase(88.0, "review1") 
val product2 = new ProductCase(95.0, "review2")
val product3 = new ProductCase(91.0, "review3") 
val product4 = new ProductCase(93.0, "review4")
val product5 = new ProductCase(95.0, "review5") 
val product6 = new ProductCase(98.0, "review6")


val initialScores = Array(("Fred", product1), ("Fred", product2), ("Fred", product3), ("Wilma", product4), ("Wilma", product5), ("Wilma", product6))
val wilmaAndFredScores = sc.parallelize(initialScores).cache()


type ScoreCollector = (Int, ProductCase) 
type PersonScores = (String, (Int, ProductCase))


//val initialScores = Array(("Fred", (88.0, "review")), ("Fred", (95.0,"review")), ("Fred", (91.0,"review")), ("Wilma", (93.0,"review")), ("Wilma", (95.0,"review")), ("Wilma", (98.0,"review")))


val createScoreCombiner = (score: ProductCase) => (1, score)

val scoreCombiner = (collector: ScoreCollector, score: ProductCase) => {
         val (numberScores, (product)) = collector
         val product1score = product.score
         val product2score = score.score
        val review = (if (product1score > product2score) product.review else score.review)
        val productResult = new ProductCase(product1score + product2score, review)
        (numberScores + 1, productResult)
      }

val scoreMerger = (collector1: ScoreCollector, collector2: ScoreCollector) => {
      
      val (numScores1, (product1)) = collector1
      val (numScores2, (product2)) = collector2

      val product1score = product1.score
      val product2score = product2.score
      val product1review = product1.review
      val product2review = product2.review

      val review = (if (product1score > product2score) product1review else product2review)
      val productResult = new ProductCase(product1score + product2score, review)
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
      val(name,average,review) = ps
       println(name+ "'s average score : " + average + " top review: " + review)
    })