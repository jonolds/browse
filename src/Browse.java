import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.storage.StorageLevel;

import scala.Tuple2;

public class Browse {


	static void aPriori(int support) throws IOException {
		JavaRDD<String> lines = settings().read().textFile("browsing.txt").toJavaRDD();
		JavaRDD<String[]> baskets = lines.map(ln->ln.split(" ")).persist(StorageLevel.MEMORY_ONLY_SER());

/* FREQUENT ITEMS ======================================================= */
	//• C1 = { {b} {c} {j} {m} {n} {p} }
		JavaRDD<String> items = baskets.flatMap(l -> Arrays.asList(l).iterator());

	//• Count the support of itemsets in C1
		JavaPairRDD<String, Integer> c1_count = items.mapToPair(x->new Tuple2<>(x, 1)).persist(StorageLevel.MEMORY_ONLY_SER()).reduceByKey((n1, n2)->n1+n2);

	//• Prune non-frequent: L1 = { b, c, j, m }
		//**** 1) Sort by string 2) SWAP 3) sort by support 4) SWAP back
		JavaPairRDD<String, Integer> l1_support = c1_count.filter(x->x._2 >= support);
		JavaPairRDD<String, Integer> l1 = l1_support.sortByKey().mapToPair(x->new Tuple2<>(x._2, x._1)).sortByKey(false).mapToPair(x->new Tuple2<>(x._2, x._1));		
		
		/* HASHTABLE */
		final Hashtable<String, Tuple2<Integer, String>> ht1 = getHashtable(l1.collect());
		final Map<Integer, Integer> L1 = l1_support.mapToPair(x->new Tuple2<>(ht1.get(x._1)._1, x._2)).collectAsMap();

		
/* PAIRS ======================================================= */
		class C2PairFlatFunc implements PairFlatMapFunction<String[], Integer, Integer> {
			public Iterator<Tuple2<Integer, Integer>> call(String[] s) throws Exception {
				List<Tuple2<Integer, Integer>> pair_list = new ArrayList<>();
				for(int i = 0; i < s.length; i++) for(int j = 0; j < s.length; j++)
					if(s[i].compareTo(s[j]) < 0 && ht1.containsKey(s[i]) && ht1.containsKey(s[j]))
						pair_list.add(new Tuple2<>(ht1.get(s[i])._1, ht1.get(s[j])._1));
				return pair_list.iterator();
			}
		}
		
		//• Generate C2 = { {b,c} {b,j} {b,m} {c,j} {c,m} {j,m} }
		JavaPairRDD<Integer, Integer> c2 = baskets.flatMapToPair(new C2PairFlatFunc());
		
	//• Count the support of itemsets in C2
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> c2_counted = c2.mapToPair(x->new Tuple2<>(x, 1)).reduceByKey((n1, n2) -> n1 + n2);
		
	//• Prune non-frequent: L2 = { {b,c} {b,m} {c,m} {c,j} }
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> l2_support = c2_counted.filter(x->x._2 >= support);
		JavaPairRDD<Tuple2<Integer, Integer>, Integer> l2 = l2_support.sortByKey(new Tup2IntIntComp(), false).cache();
		final Map<Tuple2<Integer, Integer>, Integer> L2 = l2.collectAsMap();
		
		/* l2_confidence */
		JavaPairRDD<Tuple2<Integer, Integer>, Double> L2_conf = l2_support.flatMapToPair(new PairFlatMapFunction<Tuple2<Tuple2<Integer, Integer>, Integer>, Tuple2<Integer, Integer>, Double>() {
			public Iterator<Tuple2<Tuple2<Integer, Integer>, Double>> call(Tuple2<Tuple2<Integer, Integer>, Integer> t)	throws Exception {
				List<Tuple2<Tuple2<Integer, Integer>, Double>> list = new ArrayList<>();
				Integer i = t._1._1, j = t._1._2;
				Integer i_sup = L1.get(i), j_sup = L1.get(j);
				list.add(new Tuple2<>(new Tuple2<>(i, j), Double.valueOf(t._2)/i_sup));
				return list.iterator();
			}
		});
		
		L2_conf.saveAsTextFile("output/out1");


/* TRIPLES ======================================================= */
//		class C3PairFlatFunc implements PairFlatMapFunction<String[], Tuple2<Integer, Integer>, Integer> {
//			public Iterator<Tuple2<Tuple2<Integer, Integer>, Integer>> call(String[] s) throws Exception {
//				List<Tuple2<Tuple2<Integer, Integer>, Integer>> trip_list = new ArrayList<>();
//				for(int i = 0; i < s.length; i++) for(int j = 0; j < s.length; j++) for(int k = 0; k < s.length; k++)
//					if(s[i].compareTo(s[j]) < 0 && s[j].compareTo(s[k]) < 0 && ht.containsKey(s[i]) && ht.containsKey(s[j]) && ht.containsKey(s[k]))
//						if(L2.containsKey(new Tuple2<>(ht.get(s[i])._1, ht.get(s[j])._1)) && L2.containsKey(new Tuple2<>(ht.get(s[j])._1, ht.get(s[k])._1)) && L2.containsKey(new Tuple2<>(ht.get(s[i])._1, ht.get(s[k])._1)))
//							trip_list.add(	new Tuple2<>(	new Tuple2<>(ht.get(s[i])._1, ht.get(s[j])._1), ht.get(s[k])._1	)	);
//				return trip_list.iterator();
//			}
//		}
//			
//		//• Generate C3 = { {b,c,m} {b,c,j} {b,m,j} {c,m,j} }
//		JavaPairRDD<Tuple2<Integer, Integer>, Integer> c3 = baskets.flatMapToPair(new C3PairFlatFunc()).persist(StorageLevel.MEMORY_ONLY_SER());
//		//• Count the support of itemsets in C3
//		JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Integer>, Integer> c3_counted = c3.mapToPair(x->new Tuple2<>(x, 1)).reduceByKey((n1, n2) -> n1 + n2);
//	
//		//• Prune non-frequent: L3 = { {b,c,m} }
//		JavaPairRDD<Tuple2<Tuple2<Integer, Integer>, Integer>, Integer> l3 = c3_counted.filter(x->x._2 >= support);
//		final List<Tuple2<Tuple2<Integer, Integer>, Integer>> L3 = l3.mapToPair(x->new Tuple2<>(new Tuple2<>(x._1._1._1, x._1._1._2), x._1._2)).collect();
//		
//		
//		System.out.println(L3.size());
	}

	static class Tup2IntIntComp implements Comparator<Tuple2<Integer, Integer>>, Serializable {
		public int compare(Tuple2<Integer, Integer> a, Tuple2<Integer, Integer> b) {
			return (a._1 > b._1) ? 1 : (a._1 < b._1) ? -1 : (a._2 > b._2) ? 1 : (a._2 < b._2) ? -1 : 0;
		}
	}
	
	static class Tup2Tup2IntIntIntComp implements Comparator<Tuple2<Tuple2<Integer, Integer>, Integer>>, Serializable {
		public int compare(Tuple2<Tuple2<Integer, Integer>, Integer> a, Tuple2<Tuple2<Integer, Integer>, Integer> b) {
			if(a._1._1 > b._1._1)
				return 1;
			else if(a._1._1 < b._1._1)
				return -1;
			else if(a._1._2 > b._1._2)
				return 1;
			else if(a._1._2 < b._1._2)
				return -1;
			else if(a._2 > b._2)
				return 1;
			else if(a._2 < b._2)
				return -1;
			else
				return 0;		
		}
	}
	
	static Hashtable<String, Tuple2<Integer, String>> getHashtable(List<Tuple2<String, Integer>> list) {
		Hashtable<String, Tuple2<Integer, String>> ht = new Hashtable<>(list.size());
		for(int i = 0; i < list.size(); i++) { 
			ht.put(list.get(i)._1, new Tuple2<>(i+1, list.get(i)._1)); 
		};
		return ht;
	}

	public static void main(String[] args) throws IOException, InterruptedException {
		aPriori(100);
//		Thread.sleep(90000);
	}
	
	static SparkSession settings() throws IOException {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		SparkSession.clearActiveSession();
		SparkSession spark = SparkSession.builder().appName("Browse").config("spark.master", "local").config("spark.eventlog.enabled","true").config("spark.executor.cores", "2").getOrCreate();
		SparkContext sc = spark.sparkContext();
		sc.setLogLevel("WARN");
		FileUtils.deleteDirectory(new File("output"));
		return spark;
	}
}
