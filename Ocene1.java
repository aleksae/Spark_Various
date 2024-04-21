package spark_iep;

import java.util.LinkedList;
import java.util.List;

import org.apache.spark.*;
import org.apache.spark.api.java.*;
import scala.Tuple2;
import scala.Tuple5;

public class Ocene1 {

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("Ocene1")
				.setMaster("local");
		try(JavaSparkContext sc = new JavaSparkContext(conf);){
			JavaRDD<String> ulazniPodaci = sc.textFile("studenti_test.txt");
			//obrada ulaznih podataka
			List<Tuple2<String,Integer[]>> rezultat = ulazniPodaci.flatMapToPair(
					s->{
						List<Tuple2<String, Integer[]>> lista = new LinkedList<>();
						String[] podaciSvi = s.split("\t");
						//za slucaj da student nema polozene ispite
						if(podaciSvi.length==1) return lista.iterator();
						//niz podataka o ispitima za studenta
						String[] podaciIspiti = podaciSvi[1].split(";");
						for(String p:podaciIspiti) {
							//pod[0] = predmet1,rok1,6
							String[] pod = p.split(",");
							//konvertujemo ocenu u string radi dalje obrade
							Integer ocena = Integer.parseInt(pod[2]);
							//torka spremna za obradu i dodavanje u listu
							Tuple2<String, Integer[]> podatakZaListu = new Tuple2<>(pod[0]+"&"+pod[1], new Integer[] {ocena, ocena, ocena, 1});
							lista.add(podatakZaListu);
						}
						return lista.iterator();
					}
					)
					//prvi clan se koristi za max, drugi za min, treci je suma vrednosti, cetvrti brojac vrednosti
					.reduceByKey((a,b)->new Integer[] {Math.max(a[0], b[0]), Math.min(a[1], b[1]), a[2]+b[2], a[3]+b[3]}).collect();
			
			for(Tuple2<String, Integer[]> r:rezultat) {
				System.out.println("Predmet&Rok: "+r._1()+", max:"+r._2()[0]+", min:"+r._2()[1]+", avg:"+(r._2()[2]*1.0/r._2()[3]));
				
			}
			
		}
	}
	
}
