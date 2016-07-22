package dei

import org.apache.spark.SparkContext

class ItemsAndCategories (sc: SparkContext){

  val dishesTextFile = "s3://roy-data/dishes.txt"

  def readCategoriesAndItems(categoryItems: collection.mutable.Map[String, collection.mutable.Set[String]],
                             items: collection.mutable.Set[String]) = {

    val source = sc.textFile(dishesTextFile);

    source.collect.map(line => {

      val tokens = line.split(":");
      val categoryLine = tokens(0);
      val item = tokens(1);

      items.add(item)

      val categories = categoryLine.split('|');

      categories.map(category => {

        val itemSet = categoryItems.getOrElse(category, collection.mutable.Set[String]());
        itemSet.add(item);
        categoryItems.put(category, itemSet);
      })
    })
  }
}
