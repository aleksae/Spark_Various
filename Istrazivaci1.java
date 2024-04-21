package spark_iep;

import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

import org.apache.spark.*;
import org.apache.spark.api.java.*;
import scala.Tuple2;
import scala.Tuple4;
import scala.Tuple5;

public class Istrazivaci1 {
	

	public static void main(String[] args) {
		SparkConf conf = new SparkConf()
				.setAppName("Ocene1")
				.setMaster("local");
		try(JavaSparkContext sc = new JavaSparkContext(conf);){
			Integer N = 8;
			JavaRDD<String> ulazniPodaci = sc.textFile("istrazivaci.txt");
			//obrada ulaznih podataka
			//torka (IDAutora, (min, max, sum, cnt))
			List<Tuple2<String,Tuple4<Integer,Integer,Integer,Integer>>> res = ulazniPodaci.flatMapToPair(
					s->{
						List<Tuple2<String, Tuple4<Integer, Integer, Integer, Integer>>> lista = new LinkedList<>();
						String[] podaci = s.split("\t");
						if(podaci.length==1) return lista.iterator();
						String IDAutora = podaci[0];
						String[] radovi = podaci[1].split(";");
						for(String rad:radovi) {
							String[] razbijeni = rad.split(":");
							if(razbijeni.length==1) return lista.iterator();
							String IDRada = razbijeni[0];
							String[] koautori = razbijeni[1].split(",");
							Integer brojKoautora = koautori.length - 1;
							Tuple2<String, Tuple4<Integer, Integer, Integer, Integer>> clan = new Tuple2<>(IDAutora, new Tuple4<>(brojKoautora, brojKoautora, brojKoautora, 1));
							lista.add(clan);
						}
						return lista.iterator();
					}
					)
			.reduceByKey((a,b)->
			
			{
				return new Tuple4<>(Math.min(a._1(), b._1()),
						Math.max(a._2(), b._2()),
						a._3()+b._3(),
						a._4()+b._4()
				);
			}
					
					)
			.filter(s->s._2._4()>=N)
			.collect();
			
			
			for(Tuple2<String,Tuple4<Integer,Integer,Integer,Integer>> r:res) {
				System.out.println("Istrazivac: "+r._1+" a statistika je min: "+r._2._1()+" max: "+r._2._2()+" a avg je: "+(r._2._3()*1.0/r._2._4()));
			}
			
			
			
		}
	}
	
}
