import org.apache.spark.mllib.fpm.FPGrowth
import org.apache.spark.rdd.RDD

val data = sc.textFile("data/formattedTrain2.dat")

val transactions: RDD[Array[String]] = data.map(s => s.trim.split(' '))

val fpg = new FPGrowth().setMinSupport(0.05).setNumPartitions(150)
val model = fpg.run(transactions)
model.freqItemsets.collect().foreach { itemset => println(itemset.items.mkString("[", ",", "]") + ", " + itemset.freq) }

val theJoke = "150"

var isFirst = true;
model.freqItemsets.collect().foreach { itemset => {val seq = itemset.items.mkString("[", ",", "]");  if(seq.contains(theJoke) && seq.length > 12 && isFirst) {println(seq.replace(theJoke + ",", "").replace(theJoke, "").replace(",,", ",") + ", " + itemset.freq); isFirst=false;}} }
