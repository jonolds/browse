import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.rdd.RDD;

import scala.Tuple2;

public class Browse {

	static void aPriori(JavaRDD<String> lines, int support_threshold) throws IOException {
		JavaRDD<String[]> baskets = Sp.lines2strArr(lines, " ");

//FREQUENT ITEMS=======================================================
	//• C1 = { {b} {c} {j} {m} {n} {p} }
		JavaRDD<String> items = lines.flatMap(l -> Arrays.asList(l.split(" ")).iterator());

	//• Count the support of itemsets in C1
		JavaPairRDD<String, Integer> c1_count = items.mapToPair(x->new Tuple2<>(x, 1)).reduceByKey((n1, n2)->n1+n2);

	//• Prune non-frequent: L1 = { b, c, j, m }
		JavaPairRDD<String, Integer> l1_unsorted = c1_count.filter(x->x._2 >= support_threshold).mapToPair(x->new Tuple2<>(x._1, x._2));
		JavaPairRDD<String, Integer>  l1 = l1_unsorted.sortByKey().keys().zipWithIndex().mapToPair(x->new Tuple2<>(x._1, x._2() + 1)).mapToPair(x->new Tuple2<>(x._1, x._2.intValue())).cache();

		l1.saveAsTextFile("output/output1");
		
//PAIRS=======================================================
	//• Generate C2 = { {b,c} {b,j} {b,m} {c,j} {c,m} {j,m} }
		JavaPairRDD<String, String> c2_flat = baskets.flatMapToPair(new PairFlatMapFunction<String[], String, String>() {
			public Iterator<Tuple2<String, String>> call(String[] str) throws Exception {
				List<Tuple2<String, String>> pair_list = new ArrayList<>();
				
				for(int i = 0; i < str.length; i++)
					for(int j = 0; j < str.length; j++)
						if(str[i].compareTo(str[j]) < 0)
							pair_list.add(new Tuple2<>(str[i], str[j]));
				
				return pair_list.iterator();
			}
		});

		//STRING - PAIRS
		JavaPairRDD<String, String> c2_freq_only = prunePairsWithNonfreqItems(c2_flat, l1);
		//INTS - PAIRS
		JavaPairRDD<Integer, Integer> converted = convertToInts(c2_flat, l1);
		
		
	//• Count the support of itemsets in C2
		JavaPairRDD<Tuple2<String, String>, Integer> c2_uncounted = c2_freq_only.mapToPair(x->new Tuple2<>(x, 1));
		JavaPairRDD<Tuple2<String, String>, Integer> c2 = c2_uncounted.reduceByKey((n1, n2) -> n1 + n2);
		
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> conv_uncounted = converted.mapToPair(x->new Tuple2<>(x, 1));
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> conv = conv_uncounted.reduceByKey((n1, n2) -> n1 + n2);
		
		
	//• Prune non-frequent: L2 = { {b,c} {b,m} {c,m} {c,j} }
		//STRING - PAIRS
		JavaPairRDD<String, String> l2 = JavaPairRDD.fromJavaRDD(c2.filter(x-> x._2() >= support_threshold).keys());
		//INTS - PAIRS
		JavaPairRDD<Integer, Integer> l2_conv = JavaPairRDD.fromJavaRDD(conv.filter(x-> x._2() >= support_threshold).keys());
		
		System.out.println("support " + support_threshold + " pairs: " + l2.count());
//		l2.saveAsTextFile("output/output" + String.valueOf(support_threshold));

		
		
//TRIPLES=======================================================
	//• Generate C3 = { {b,c,m} {b,c,j} {b,m,j} {c,m,j} }
		JavaPairRDD<String, String> c3_flat = baskets.flatMapToPair(new PairFlatMapFunction<String[], String, String>() {
			public Iterator<Tuple2<String, String>> call(String[] str) throws Exception {
				List<Tuple2<String, String>> pair_list = new ArrayList<>();
				for(int i = 0; i < str.length; i++)
					for(int j = 0; j < str.length; j++)
						if(str[i].compareTo(str[j]) < 0)
							pair_list.add(new Tuple2<>(str[i], str[j]));
				
				return pair_list.iterator();
			}
		});
		
	//• Count the support of itemsets in C3	
	//• Prune non-frequent: L3 = { {b,c,m} }
	
	}
	
	static <K>JavaPairRDD<K, K> prunePairsWithNonfreqItems(JavaPairRDD<K, K> unpruned, JavaPairRDD<K, Integer> freq) {
		//PRUNE by key
		JavaPairRDD<K, K> key_pruned = unpruned.join(freq).mapToPair(x->new Tuple2<>(x._1, x._2._1));
		JavaPairRDD<K, K> key_pruned_swapped = key_pruned.mapToPair(x->new Tuple2<>(x._2, x._1));
		JavaPairRDD<K, K> key_val_pruned = key_pruned_swapped.join(freq).mapToPair(x->new Tuple2<>(x._1, x._2._1));
		JavaPairRDD<K, K> pruned = key_val_pruned.mapToPair(x->new Tuple2<>(x._2, x._1));
		return pruned; 
	}
	
	static JavaPairRDD<Integer, Integer> convertToInts(JavaPairRDD<String, String> orig, JavaPairRDD<String, Integer> table) {
		JavaPairRDD<String, Integer> convert_key = JavaPairRDD.fromJavaRDD(orig.join(table).values());
		JavaPairRDD<Integer, Integer> convert_val = JavaPairRDD.fromJavaRDD(convert_key.join(table).values());
		return convert_val;
	}
	
	public static void main(String[] args) throws IOException, InterruptedException {
		final int support_threshold = 100;
		JavaRDD<String> lines = Sp.settings().read().textFile("browsingSmall.txt").toJavaRDD();
		aPriori(lines, 3);
		
		Thread.sleep(30000);		
	}
}
