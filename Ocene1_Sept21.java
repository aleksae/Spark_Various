package spark_iep;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.*;
import org.apache.spark.api.java.*;

import scala.Tuple2;
import scala.Tuple6;

public class Ocene1_Sept21 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("Sept21_1")
				.setMaster("local");
		try(JavaSparkContext sc = new JavaSparkContext(conf);){
			JavaRDD<String> fajlPodaci = sc.textFile("studenti0.txt");
			List<Tuple2<String, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>> rezultat = fajlPodaci.flatMapToPair(s->{
				String[] glavnoRazbijanje = s.split("\t");
				List<Tuple2<String, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>>> lista = new LinkedList<>();
				if(glavnoRazbijanje.length==1) return lista.iterator();
				String[] podaciOPolaganjima = glavnoRazbijanje[1].split(";");
				for(String podatak:podaciOPolaganjima) {
					String[] detaljiOPolaganju = podatak.split(",");
					Integer ocena = Integer.parseInt(detaljiOPolaganju[2]);
					Tuple2<String, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> clanListe = new Tuple2<>(
							detaljiOPolaganju[0]+"&"+detaljiOPolaganju[1], new Tuple6<>(1, ocena==6?1:0,ocena==7?1:0,ocena==8?1:0,ocena==9?1:0,ocena==10?1:0));
					lista.add(clanListe);
				}
				return lista.iterator();
			})
			.reduceByKey((a,b)->{return new Tuple6<>(
					a._1()+b._1(),
					a._2()+b._2(),
					a._3()+b._3(),
					a._4()+b._4(),
					a._5()+b._5(),
					a._6()+b._6()
					);})
			.collect();
			;
			System.out.println("Predmet&Rok Total 6 7 8 9 10");
			for(Tuple2<String, Tuple6<Integer, Integer, Integer, Integer, Integer, Integer>> rez:rezultat) {
				System.out.println(rez._1()+" "+rez._2()._1()+" "+rez._2()._2()+" "+rez._2()._3()+" "+rez._2()._4()+" "+rez._2()._5()+" "+rez._2()._6());
			}
		}
	}
}
