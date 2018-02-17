package binod.Demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class ReadFileInSpark {

	  public static void main(String[] args) {
		
		  System.out.println("Another Test");
			SparkConf sparkConf = new SparkConf();
			sparkConf.setMaster("local");
			sparkConf.setAppName("Test Spark");
			JavaSparkContext sc = new JavaSparkContext(sparkConf);
			
			//String filePath = "file:///d:/sample_libsvm_data.txt";
			String filePath = "file:///d://student.txt";
			
			JavaRDD<String> names = sc.textFile(filePath);
			System.out.println("*************************** :"+names.count());
			System.out.println(names);
			for(String line:names.collect()){
				System.out.println("********** INSIDE ****************");
				System.out.println(names.collect());
			}
			
	}
}
