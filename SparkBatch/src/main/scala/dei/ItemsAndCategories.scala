package dei

/**
  * Created by roycohen on 6/25/16.
  */
class ItemsAndCategories {

  def readCategoriesAndItems(fileName: String,
                             categoryItems: collection.mutable.Map[String, collection.mutable.Set[String]],
                             items: collection.mutable.Set[String]) = {

    val source = scala.io.Source.fromFile(fileName);

    for (line <- source.getLines) {

      val tokens = line.split(":");
      val categoryLine = tokens(0);
      val item = tokens(1);

      items.add(item)

      val categories = categoryLine.split('|');

      categories.foreach(category => {

        val itemSet = categoryItems.getOrElse(category, collection.mutable.Set[String]());
        itemSet.add(item);
        categoryItems.put(category, itemSet);
      })
    }
  }
}
