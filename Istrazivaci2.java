package spark_iep;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.*;
import org.apache.spark.api.java.*;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

public class Istrazivaci2 {
	
	//predmet zadat rok R, polagalo najvise studenata, a da nema ocene N

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("Ocene1")
				.setMaster("local");
		try(JavaSparkContext sc = new JavaSparkContext(conf);){
			JavaRDD<String> ulazniPodaci = sc.textFile("istrazivaci.txt");
			//obrada ulaznih podataka
			String R = "8";
			//primer clana liste - prvi ja autor, drugo skup svih sa kojima je saradjivao
			//(Zika, (Srbljan, Milicev, Zika, Zaki))
			JavaPairRDD<String, HashSet<String>> formatirani = ulazniPodaci.mapToPair(
					s->{
						String[] podaci = s.split("\t");
						if(podaci.length==1) return new Tuple2<>(podaci[0], new HashSet<String>());
						String[] radovi = podaci[1].split(";");
						HashSet<String> kolaboratori = new HashSet<>();
						for(String rad:radovi) {
							String[] razbijeni = rad.split(":");
							if(razbijeni.length==1) continue;
							String[] koautori = razbijeni[1].split(",");
							HashSet<String> setKoAutora = new HashSet<>(Arrays.asList(koautori));
							kolaboratori.addAll(setKoAutora);
						}
						return new Tuple2<>(podaci[0],kolaboratori);
					}
					);
			/*for(Tuple2<String,HashSet<String>>  res:formatirani.collect()) {
				System.out.println(res._1);
				Iterator<String> iterator = res._2.iterator();
				while (iterator.hasNext()) {
		            System.out.print(iterator.next());
		            if (iterator.hasNext()) {
		                System.out.print(", ");
		            }
		        }
				System.out.println("--------");
			}*/
			Tuple2<String, HashSet<String>>  sviSaKojimaJeIstrazivaoR = formatirani.filter(s->s._1.equals(R)).first();
			HashSet<String> kolaboratoriR = sviSaKojimaJeIstrazivaoR._2();
			
			
			JavaPairRDD<Integer, String> finalni = formatirani
			//da isfiltriramo sve koji su saradjivali sa R
			.filter(s->!(s._2.contains(R)))
			.mapToPair(s->{
				HashSet<String> datiPodaci = s._2;
				//menja datiPodaci tako da sadrzi samo one koje deli sa R
				datiPodaci.retainAll(kolaboratoriR);
				//sada imamo torku ciji je prvi clan broj zajednickih a drugi oznaka tog istrazivaca
				return new Tuple2<Integer, String>(datiPodaci.size(), s._1);
				
			})
			//sortiramo da bismo dobili one sa najvise, false da bi se sortiralno opadajuce
			.sortByKey(false)
			;
			
			try {
				//ispisuje sve sa MAX, moglo potencijalno i samo 1
				Tuple2<Integer, String> res = finalni.first();
				List<Tuple2<Integer, String>> finn = finalni.filter(s->s._1().equals(res._1())).collect();
				System.out.println("Max broj je  "+res._1()+" a saradnici su:");
				for(Tuple2<Integer, String> r:finn) {
					System.out.println("Istrazivac "+r._2());
				}
			}catch(Exception e) {
				System.out.println("Nema takvog za istrazivaca:"+R);
			}
			
			
			
			
			
		}
	}
	
}
