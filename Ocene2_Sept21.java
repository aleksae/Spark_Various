package spark_iep;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.*;
import org.apache.spark.api.java.*;

import scala.Tuple2;
import scala.Tuple6;

public class Ocene2_Sept21 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("Sept21_2")
				.setMaster("local");
		Integer N = 3000;
		try(JavaSparkContext sc = new JavaSparkContext(conf);){
			 JavaRDD<String> fajlPodaci = sc.textFile("studenti0.txt");
			 JavaPairRDD<Double,String> rezultat =fajlPodaci.flatMapToPair(s->{
				String[] glavnoRazbijanje = s.split("\t");
				List<Tuple2<String, Tuple2<Integer, Integer>>> lista = new LinkedList<>();
				if(glavnoRazbijanje.length==1) return lista.iterator();
				String[] podaciOPolaganjima = glavnoRazbijanje[1].split(";");
				for(String podatak:podaciOPolaganjima) {
					String[] detaljiOPolaganju = podatak.split(",");
					Integer ocena = Integer.parseInt(detaljiOPolaganju[2]);
					Tuple2<String, Tuple2<Integer, Integer>> clanListe = new Tuple2<>(
							detaljiOPolaganju[0], new Tuple2<>(1, ocena));
					lista.add(clanListe);
				}
				return lista.iterator();
			})
			.reduceByKey((a,b)->{return new Tuple2<>(
					a._1()+b._1(),
					a._2()+b._2()
					);})
			.filter(a->a._2._1() >= N)
			.mapToPair(s->new Tuple2<>((s._2._2*1.0/s._2()._1()), s._1))
			.sortByKey(false)
			;
			Tuple2<Double,String> prvi = rezultat.first();
			List<Tuple2<Double, String>> rezultatFin = rezultat.filter(p->p._1.equals(prvi._1())).collect();
			
			for(Tuple2<Double,String> res:rezultatFin) {
				System.out.println("Predmet "+res._2()+" uz prosek "+res._1());
			}
			
		}
	}
}
