import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.catalog.Function;

import scala.Tuple2;

public class Browse {
	
	static final int support_threshold = 8;
	public static void main(String[] args) throws IOException {
		JavaRDD<String> lines = Sp.settings().read().textFile("browsingSmall.txt").toJavaRDD();
		JavaRDD<String[]> baskets = Sp.lines2strArr(lines, " ");

	//• C1 = { {b} {c} {j} {m} {n} {p} }
		JavaRDD<String> items = lines.flatMap(l -> Arrays.asList(l.split(" ")).iterator());

	//• Count the support of itemsets in C1
		JavaPairRDD<String, Integer> items_1 = items.mapToPair(x->new Tuple2<>(x, 1));
		JavaPairRDD<String, Integer> items_counted = items_1.reduceByKey((n1, n2) -> n1 + n2);
		

	//• Prune non-frequent: L1 = { b, c, j, m }
		JavaPairRDD<String, Integer> freq_unsorted = items_counted.filter(x->x._2 >= support_threshold).mapToPair(x->new Tuple2<>(x._1, x._2));
		JavaPairRDD<String, Long>  freq = freq_unsorted.sortByKey().keys().zipWithIndex().mapToPair(x->new Tuple2<>(x._1, x._2() + 1));
		freq.saveAsTextFile("output");

	//• Generate C2 = { {b,c} {b,j} {b,m} {c,j} {c,m} {j,m} }
		
		
		
//		JavaPairRDD<Tuple2<String, String>, Integer>  c2 = baskets.flatMapToPair(new PairFlatMapFunction<String[], Tuple2<String, String>, Integer>() {
//			
//			public Iterator<Tuple2<Tuple2<String, String>, Integer>> call(String[] str) throws Exception {
//				return Arrays.stream(str).map(x->new Tuple2<>(new Tuple2<>(x, x), 1)).collect(Collectors.toList()).iterator();
//			}
//		});

	//• Count the support of itemsets in C2
	//• Prune non-frequent: L2 = { {b,m} {b,c} {c,m} {c,j} }
	//• Generate C3 = { {b,c,m} {b,c,j} {b,m,j} {c,m,j} }
	//• Count the support of itemsets in C3
	//• Prune non-frequent: L3 = { {b,c,m} }
		

		
	}
}
