import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.SparkSession;

import scala.Serializable;
import scala.Tuple2;

public class Sp implements Serializable {

//==SPLIT======================================================================================

	//SPLIT: String into Integer[] using d1 (delimiter).
	static JavaRDD<Integer[]> lines2intArr(JavaRDD<String> lines, String d1) {
		JavaRDD<Integer[]> tokenized = lines.map(new Function<String, Integer[]>() { 
			public Integer[] call(String str) {
				String[] line_split = str.split(d1);
				Integer[] line_split_ints = new Integer[line_split.length];
				for(int i = 0; i < line_split.length; i++)
					line_split_ints[i] = Integer.valueOf(line_split[i]);
				return line_split_ints;
			} 
		});
		return tokenized;
	}
	
	//SPLIT: String into String[] using d1 (delimiter).
	static JavaRDD<String[]> lines2strArr(JavaRDD<String> lines, String d1) {
		JavaRDD<String[]> tokenized = lines.map(new Function<String, String[]>() { 
			public String[] call(String str) {
				return str.split(d1);
			} 
		});
		return tokenized;
	}
	
	//SPLIT TWICE: line into two parts d1 (delimiter). First becomes Int, second is split at d2 into Int[]
	static JavaPairRDD<Integer, Integer[]> linesToPairs_int_intArr(JavaRDD<String> lines, String d1, String d2) {
		JavaPairRDD<Integer, Integer[]> tokenized = lines.mapToPair(new PairFunction<String, Integer, Integer[]>() { 
			public Tuple2<Integer, Integer[]> call(String s) {
				String[] pers_frds_split = s.split(d1);
				if(pers_frds_split.length > 1)
					return new Tuple2<>(Integer.parseInt(pers_frds_split[0]), Arrays.stream(pers_frds_split[1].split(d2)).map(x->Integer.parseInt(x)).toArray(Integer[]::new));
				return new Tuple2<>(Integer.parseInt(pers_frds_split[0]), null);
			} 
		});
		return tokenized;
	}

	

//==SWAP======================================================================================
	
	//SWAP 1 AND 3 - Pair(Pair(1, 2), 3) becomes Pair(Pair(3, 2), 1)
	static <K1, K2, V>JavaPairRDD<Tuple2<V, K2>, K1> swap_1_3(JavaPairRDD<Tuple2<K1, K2>, V> unsorted) {
		JavaPairRDD<Tuple2<V, K2>, K1> swapped = unsorted.mapToPair(new PairFunction<Tuple2<Tuple2<K1, K2>, V>, Tuple2<V, K2>, K1>() {
			public Tuple2<Tuple2<V, K2>, K1> call(Tuple2<Tuple2<K1, K2>, V> t) {
				return new Tuple2<>(new Tuple2<>(t._2, t._1._2), t._1._1);
			}
		});
		return swapped; 
	}
	
	

//==SHIFT==========================================================================================
	
	//SHIFT - tuple left to right
	static <K, K1, V>JavaPairRDD<K, Tuple2<K1, V>> shiftTupLtoR(JavaPairRDD<Tuple2<K, K1>, V> tup_on_left) {
		JavaPairRDD<K, Tuple2<K1, V>> shifted = tup_on_left.mapToPair(new PairFunction<Tuple2<Tuple2<K, K1>, V>, K, Tuple2<K1, V>>() {
			public Tuple2<K, Tuple2<K1, V>> call(Tuple2<Tuple2<K, K1>, V> t) {
				return new Tuple2<>(t._1._1, new Tuple2<>(t._1._2, t._2));
			}
		});
		return shifted; 
	}
	
	//SHIFT - tuple right to left
	static <K, V1, V2>JavaPairRDD<Tuple2<K, V1>, V2> shiftTupRtoL(JavaPairRDD<K, Tuple2<V1, V2>> tup_on_left) {
		JavaPairRDD<Tuple2<K, V1>, V2> shifted = tup_on_left.mapToPair(new PairFunction<Tuple2<K, Tuple2<V1, V2>>, Tuple2<K, V1>, V2>() {
			public Tuple2<Tuple2<K, V1>, V2> call(Tuple2<K, Tuple2<V1, V2>> t) {
				return new Tuple2<>(new Tuple2<>(t._1, t._2._1), t._2._2);
			}
		});
		return shifted; 
	}
	
	
	
//==SWAP AND SHIFT==================================================================================
	
	//SWAP and SHIFT -!!GENERIC!!- from Pair<Pair<1, 2>, 3> to Pair<3, Pair<2, 1>
	static <K1, K2, V> JavaPairRDD<V, Tuple2<K2, K1>> swap_1_3_and_format(JavaPairRDD<Tuple2<K1, K2>, V> unsorted) {
		JavaPairRDD<V, Tuple2<K2, K1>> swapped = unsorted.mapToPair(new PairFunction<Tuple2<Tuple2<K1, K2>, V>, V, Tuple2<K2, K1>>() {
			public Tuple2<V, Tuple2<K2, K1>> call(Tuple2<Tuple2<K1, K2>, V> t) {
				return new Tuple2<>(t._2, new Tuple2<>(t._1._2, t._1._1));
			}
		});
		return swapped; 
	}
	
	
	
//==PAIRIFY===============================================================================
	
	//ADD LINE NUMBER AS KEY
	static <T> JavaPairRDD<Integer, T> addIdxKey(JavaRDD<T> rdd) {
		JavaPairRDD<Integer, T> rdd_with_idx_key = rdd.mapToPair(new PairFunction<T, Integer, T>() {
			int num = -1;
			public Tuple2<Integer, T> call(T t) {
				num++;
				return new Tuple2<>(num, t);
			} 
		});
		return rdd_with_idx_key;
	}
	
	//ADD LINE NUMBER AS KEY
	static <K, V> JavaPairRDD<Integer, K> makeKeyValAndAddIdxKey(JavaPairRDD<K, V> rdd) {
		JavaPairRDD<Integer, K> rdd_with_idx_key = rdd.mapToPair(new PairFunction<Tuple2<K,V>, Integer, K>() {
			int num = 0;
			public Tuple2<Integer, K> call(Tuple2<K, V> t) {
				num++;
				return new Tuple2<>(num, t._1);
			} 
		});
		return rdd_with_idx_key;
	}
	
	//ADD LINE NUMBER AS KEY
	static <K, V> JavaPairRDD<Integer, K> pairIdxWithKey(JavaPairRDD<K, V> rdd_pair) {
		JavaPairRDD<Integer, K> rdd_with_idx_key = rdd_pair.mapToPair(new PairFunction<Tuple2<K, V>, Integer, K>() {
			int num = 0;
			public Tuple2<Integer, K> call(Tuple2<K, V> t) {
				num++;
				return new Tuple2<>(num, t._1);
			} 
		});
		return rdd_with_idx_key;
	}
	
	
	
//==SETS===============================================================================
	//GET ALL POSSIBLE VALUE PAIR COMBINATIONS FOR EACH LINE
	static <K, V> JavaPairRDD<K, Tuple2<V, V>> getAllPossValPairs(JavaPairRDD<K, V[]> pair_val_arr) {
		JavaPairRDD<K, Tuple2<V, V>> all_poss_val_pairs = pair_val_arr.flatMapToPair(new PairFlatMapFunction<Tuple2<K, V[]>, K, Tuple2<V, V>>() {
			public Iterator<Tuple2<K, Tuple2<V, V>>> call(Tuple2<K, V[]> t) throws Exception {
				List<Tuple2<K, Tuple2<V, V>>> list = new ArrayList<>();
				if(t._2.length > 1)				//Make sure it's not a one element array
					for(int i = 0; i < t._2.length-1; i++)
						for(int k = i+1;k < t._2.length; k++)
							list.add(new Tuple2<>(t._1, new Tuple2<>(t._2[i], t._2[k])));
				return list.iterator();
			}
			
		});
		return all_poss_val_pairs;
	}
	
	
	
//==FLATTEN===============================================================================
	
	//Deg_1 relations with tuple2<person, friend> as key, 0 as value for later subtraction
	static JavaPairRDD<Tuple2<Integer, Integer>,Integer> flattenValueArr(JavaPairRDD<Integer, Integer[]> has_frds) {
		JavaPairRDD<Tuple2<Integer, Integer>,Integer> pers_fr_0 = has_frds.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Integer[]>, Tuple2<Integer, Integer>, Integer>() {
			public Iterator<Tuple2<Tuple2<Integer, Integer>,Integer>> call(Tuple2<Integer, Integer[]> t2) {
				List<Tuple2<Tuple2<Integer, Integer>,Integer>> pairs = new ArrayList<>();
				for(Integer s: t2._2)
					pairs.add(new Tuple2<>(new Tuple2<>(t2._1, s), 0));
				return pairs.iterator();
			} 
		});
		return pers_fr_0;
	}
	
	//Creates a Pair(Pair(V1, V2), 1) for each possible combo in the value array. V=1 set for later reduce 
	static <K, V>JavaPairRDD<Tuple2<V, V>, Integer> flattenValueArr_with_FILTER_COUNTER(JavaPairRDD<K, V[]> has_frds) {
		JavaPairRDD<Tuple2<V, V>, Integer> deg2_poss_1 = has_frds.filter(x->x._2 != null).flatMapToPair(new PairFlatMapFunction<Tuple2<K, V[]>, Tuple2<V, V>, Integer>() {
			public Iterator<Tuple2<Tuple2<V, V>,Integer>>  call(Tuple2<K, V[]> pers) {
				List<Tuple2<Tuple2<V, V>,Integer>> d2 = new ArrayList<>();
				for(V friend : pers._2) 
					for(int i = 0; i < pers._2.length; i++)
						if(pers._2[i] != friend)
							d2.add(new Tuple2<>(new Tuple2<>(friend, pers._2[i]), 1));
				return d2.iterator();
			} 
		});
		return deg2_poss_1;
	}
	
	//FLATTEN BY KEY
	static <K, V> JavaPairRDD<K, V> flattenByKey(JavaPairRDD<K, V[]> pair_with_arr_val) {
		JavaPairRDD<K, V> pair_flatval_count = pair_with_arr_val.flatMapToPair(new PairFlatMapFunction<Tuple2<K, V[]>, K, V>() {
			public Iterator<Tuple2<K, V>> call(Tuple2<K, V[]> t) {
				List<Tuple2<K, V>> list = new ArrayList<>();
				for(V v: t._2)
					list.add(new Tuple2<>(t._1, v));
				return list.iterator();
			}
		});
		return pair_flatval_count;
	}
	
	//FLATTEN BY KEY WITH COUNT
	static <K, V> JavaPairRDD<K, Tuple2<V, Integer>> flattenByKeyWithCount(JavaPairRDD<K, V[]> pair_with_arr_val) {
		JavaPairRDD<K, Tuple2<V, Integer>> pair_flatval_count = pair_with_arr_val.flatMapToPair(new PairFlatMapFunction<Tuple2<K, V[]>, K, Tuple2<V, Integer>>() {
			public Iterator<Tuple2<K, Tuple2<V, Integer>>> call(Tuple2<K, V[]> t) {
				List<Tuple2<K, Tuple2<V, Integer>>> list = new ArrayList<>();
				for(V v: t._2)
					list.add(new Tuple2<>(t._1, new Tuple2<>(v, 1)));
				return list.iterator();
			}
		});
		return pair_flatval_count;
	}
	
	
	
//==OTHER======================================================================================

	//
	static <K, V>JavaPairRDD<K, Integer[]> iter2Array(JavaPairRDD<K, Iterable<Tuple2<Integer, Integer>>> int_TupItt_rdd) {
		JavaPairRDD<K, Integer[]> int_intArr = int_TupItt_rdd.mapToPair(new PairFunction<Tuple2<K, Iterable<Tuple2<Integer, Integer>>>, K, Integer[]>() { 
			public Tuple2<K, Integer[]> call(Tuple2<K,Iterable<Tuple2<Integer,Integer>>> int_TupItt) {
				Iterator<Tuple2<Integer, Integer>> it = int_TupItt._2.iterator();
				List<Integer> suggests = new ArrayList<>();
				while(it.hasNext())
					suggests.add(it.next()._1);
				Integer[] intArr = suggests.toArray(new Integer[suggests.size()]);
				return new Tuple2<>(int_TupItt._1, intArr);
			}
		});
		return int_intArr; 
	}
	
	
	
//==PRINT======================================================================================
	
	static <T> void printArr(JavaRDD<T[]> rdd) {
       for(T[] line: rdd.collect())
            System.out.println(arr2CommaStr(line));
	}
	
	static <K, V> void print(JavaPairRDD<K, V[]> pair_with_arr_val) {
       for(Tuple2<K,V[]> t: pair_with_arr_val.collect())
           System.out.println(String.valueOf(t._1) + ": " + arr2CommaStr(t._2));
	}
	
	static <K, V> void printTup(JavaPairRDD<K, V> pair) {
		for(Tuple2<K, V> t: pair.collect())
			System.out.println(t._1.toString() + " : " + t._2.toString());
	}
	
	static <T> String arr2CommaStr(T[] arr) {
		return Stream.of(arr).map(x->(String)x).collect(Collectors.joining(","));
	}

	
	
//==SPARK SESSION======================================================================================
	static SparkSession settings() throws IOException {
		Logger.getLogger("org").setLevel(Level.WARN);
		Logger.getLogger("akka").setLevel(Level.WARN);
		SparkSession.clearActiveSession();
		SparkSession spark = SparkSession.builder().appName("Browse").config("spark.master", "local").config("spark.eventlog.enabled","true").getOrCreate();
		SparkContext sc = spark.sparkContext();
		sc.setLogLevel("WARN");
		FileUtils.deleteDirectory(new File("output"));
		FileUtils.deleteDirectory(new File("output2"));
		return spark;
	}
}
