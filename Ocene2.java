package spark_iep;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.*;
import org.apache.spark.api.java.*;
import scala.Tuple2;
import scala.Tuple5;

public class Ocene2 {
	
	//predmet zadat rok R, polagalo najvise studenata, a da nema ocene N

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("Ocene1")
				.setMaster("local");
		try(JavaSparkContext sc = new JavaSparkContext(conf);){
			String rok = "12";
			String zadataOcena = "8";
			JavaRDD<String> ulazniPodaci = sc.textFile("studenti0.txt");
			//obrada ulaznih podataka
			//Tuple2<Integer,String[]>
			List<Tuple2<Integer,String[]>>  rezultat = ulazniPodaci.flatMapToPair(
					s->{
						List<Tuple2<String[], String>> lista = new LinkedList<>();
						String[] podaciSvi = s.split("\t");
						//za slucaj da student nema polozene ispite
						if(podaciSvi.length==1) return lista.iterator();
						//niz podataka o ispitima za studenta
						String[] podaciIspiti = podaciSvi[1].split(";");
						boolean imaOcenuNURoku = false;
						for(String p:podaciIspiti) {
							//pod[0] = predmet1,rok1,6
							String[] pod = p.split(",");
							//konvertujemo ocenu u string radi dalje obrade
							//torka spremna za obradu i dodavanje u listu
							Integer ocena = Integer.parseInt(pod[2]);
							//([predmet,rok], ocena)
							if(pod[2].equals(zadataOcena) && pod[1].equals(rok)) {
								//student imao ocenu N u roku R - to ne
								imaOcenuNURoku = true;
								break;
							}
							Tuple2<String[], String> podatakZaListu = new Tuple2<>(new String[] {pod[0],pod[1]}, pod[2]);
							lista.add(podatakZaListu);
						}
						if(imaOcenuNURoku) lista.clear();
						return lista.iterator();
					}
					)
					//isfiltriraj rok
			 		.filter(s->s._1[1].equals(rok))
			 		//reformatiraj kljuc predmet, vrednost ocena
			 		.mapToPair(s->new Tuple2<String, String>(s._1[0], s._2))
			 		//ni jedna ocena u roku nije zadata ocena
			 		//.filter(s->!(s._2.equals(zadataOcena)))
			 		//pravimoStringOcena ocena1,ocena2...
			 		.reduceByKey((a,b)->(a+";"+b))
			 		//reformatiraj da bude ([predmet1,ocena1;ocena2...],brojOcena)
			 		.mapToPair(s->new Tuple2<Integer, String[]>(s._2.split(";").length, new String[] {s._1, s._2}))
			 		//sortiraj po broju ocena
			 		.sortByKey(false)
			 		//isflitriraj da nema ocenu
			 		//.filter(s->{return !(Arrays.asList(s._2[1].split(";")).contains(zadataOcena));})
			 		//vrati sve, ovako zbog lakse obrade ako nema
			 		.collect()
					;
			if(rezultat.size()==0) {
				System.out.println("Nema jbg");
			}else {
				System.out.println("Trazeni predmet je "+rezultat.get(0)._2[0]+" a broj je "+rezultat.get(0)._1);
			}
			
			
		}
	}
	
}
