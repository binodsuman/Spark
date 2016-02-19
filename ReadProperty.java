package binod.Demo;



import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.StringTokenizer;
import java.util.logging.Level;
import java.util.logging.Logger;



public class ReadProperty {
	
	private static final String ENABLE_SCALABLE_STORAGE_PROP = "isenabled";
	private static final String KAFKA_BROKERS_LIST_PROP		= "kafka.serverlist";
	private static final String HBASE_BROKERS_LIST_PROP		= "hbase.serverlist";
	private static final String SPARK_BROKERS_LIST_PROP		= "spark.serverlist";
	private static final String ELASTICSEARCH_BROKERS_LIST_PROP		= "elasticsearch.serverlist";
	private static final String ZOOKEEPER_BROKERS_LIST_PROP		= "zookeeper.serverlist";
	private static final String COMMA_SEPARATOR = ",";
	
	String EVENT_ROUTER_PROPERTY_FILE = "E:\\BigDataWork\\scalablestorageconfig";
	
	private Properties properties;
	private boolean isEnabled = false;
	private String kafka_serverlink = null;
	private String hbase_serverlink = null;
	private String spark_serverlink = null;
	private String elasticsearch_serverlink = null;
	private String zookeeper_serverlink = null;
	
	Map<String,List<String>> isEnabledMap = new HashMap<String,List<String>>();
	Map<String,List<String>> kafkaMap = new HashMap<String,List<String>>();
	Map<String,List<String>>  hbaseMap = new HashMap<String,List<String>>();
	Map<String,List<String>>  sparkMap = new HashMap<String,List<String>>();
	Map<String,List<String>>  elasticsearchMap = new HashMap<String,List<String>>();
	Map<String,List<String>>  zookeeperMap = new HashMap<String,List<String>>();
	
	Map<String,Map<String,List<String>>> finalMap = new HashMap<String,Map<String,List<String>>>();
	
	
	protected final static Logger logger = Logger.getLogger(ReadProperty.class.getName());
	
	
	public void ReadPropertyAndSave(){
		File file = new File(EVENT_ROUTER_PROPERTY_FILE);
		if(!file.exists()){
			 logger.log(Level.SEVERE, EVENT_ROUTER_PROPERTY_FILE+" is not exist for Scalable Store Configuration");
		}else{
		 properties = new Properties();
		 loadProperties();
		 Map<String,Map<String,List<String>>> finalMap = readFromPropertiesFile();
		 displayFinalMap(finalMap);
		 if(deleteConfigFile()){
			 logger.log(Level.INFO, EVENT_ROUTER_PROPERTY_FILE+" has been deleted");
		 }else{
			 logger.log(Level.SEVERE, EVENT_ROUTER_PROPERTY_FILE+" did not delete, please check");
		 }
	   }
	}
	
	protected void loadProperties() {
		File file = new File(EVENT_ROUTER_PROPERTY_FILE);
		loadProperties(file, properties);
	}
	
	private Map readFromPropertiesFile(){
		isEnabled = getBooleanProperty(ENABLE_SCALABLE_STORAGE_PROP, false);
		String kafka_serverlink = getProperty(KAFKA_BROKERS_LIST_PROP);
		String hbase_serverlink = getProperty(HBASE_BROKERS_LIST_PROP);
		String spark_serverlink = getProperty(SPARK_BROKERS_LIST_PROP);
		String elasticsearch_serverlink = getProperty(ELASTICSEARCH_BROKERS_LIST_PROP);
		String zookeeper_serverlink = getProperty(ZOOKEEPER_BROKERS_LIST_PROP);
		
		/*System.out.println("isEnabled :"+isEnabled);
		System.out.println("kafka_serverlink :"+kafka_serverlink);
		System.out.println("hbase_serverlink :"+hbase_serverlink);
		System.out.println("spark_serverlink :"+spark_serverlink);
		System.out.println("elasticsearch_serverlink :"+elasticsearch_serverlink);
		System.out.println("zookeeper_serverlink :"+zookeeper_serverlink);*/
		
		
		List<String> isEnabledList = new ArrayList<String>();
		isEnabledList.add(isEnabled+"");
		isEnabledMap.put("serverlink",isEnabledList);
		
		
		if(kafka_serverlink != null){
			StringTokenizer kafkaTokenizer  = new StringTokenizer(kafka_serverlink,COMMA_SEPARATOR);
			List<String> kafka_serverlinkList = new ArrayList<String>();
			while(kafkaTokenizer!=null && kafkaTokenizer.hasMoreElements()){
				kafka_serverlinkList.add(kafkaTokenizer.nextToken());
			}
			
			kafkaMap.put("serverlink", kafka_serverlinkList);
		}
		
		if(hbase_serverlink != null){
			StringTokenizer hbaseTokenizer  = new StringTokenizer(hbase_serverlink,COMMA_SEPARATOR);
			List<String> hbase_serverlinkList = new ArrayList<String>();
			while(hbaseTokenizer!=null && hbaseTokenizer.hasMoreElements()){
				hbase_serverlinkList.add(hbaseTokenizer.nextToken());
			}
			
			hbaseMap.put("serverlink", hbase_serverlinkList);
		}
		
		if(elasticsearch_serverlink != null){
			StringTokenizer elasticSearchTokenizer  = new StringTokenizer(elasticsearch_serverlink,COMMA_SEPARATOR);
			List<String> elasticSearch_serverlinkList = new ArrayList<String>();
			while(elasticSearchTokenizer!=null && elasticSearchTokenizer.hasMoreElements()){
				elasticSearch_serverlinkList.add(elasticSearchTokenizer.nextToken());
			}
			
			elasticsearchMap.put("serverlink", elasticSearch_serverlinkList);
		}
		
		if(spark_serverlink != null){
			StringTokenizer sparkTokenizer  = new StringTokenizer(spark_serverlink,COMMA_SEPARATOR);
			List<String> spark_serverlinkList = new ArrayList<String>();
			while(sparkTokenizer!=null && sparkTokenizer.hasMoreElements()){
				spark_serverlinkList.add(sparkTokenizer.nextToken());
			}
			
			sparkMap.put("serverlink", spark_serverlinkList);
		}
		
		if(zookeeper_serverlink != null){
			StringTokenizer zookeeperTokenizer  = new StringTokenizer(zookeeper_serverlink,COMMA_SEPARATOR);
			List<String> zookeeper_serverlinkList = new ArrayList<String>();
			while(zookeeperTokenizer!=null && zookeeperTokenizer.hasMoreElements()){
				zookeeper_serverlinkList.add(zookeeperTokenizer.nextToken());
			}
			
			zookeeperMap.put("serverlink", zookeeper_serverlinkList);
		}
		
		finalMap.put("isEnabled", isEnabledMap);
		finalMap.put("kafka", kafkaMap);
		finalMap.put("hbase", hbaseMap);
		finalMap.put("spark", sparkMap);
		finalMap.put("elasticsearch", elasticsearchMap);
		finalMap.put("zookeeper", zookeeperMap);
		
		return finalMap;
	}
	
	 /**
     * Load properties from the given file into the given properties object.
     *
     * @throws RuntimeException if unable to load the properties from the file.
     */
    private static Properties loadProperties(File file, Properties properties) {
        FileInputStream is = null;

        try {
            // Load the properties
            is = new FileInputStream(file);
            properties.load(new BufferedInputStream(is));
        } catch (IOException ioe) {
            logger.log(Level.SEVERE, ioe.getMessage(), ioe);
            throw new RuntimeException(ioe);
        } finally {
            try {
                if (is != null) {
                    is.close();
                }
            } catch (IOException e) {
                logger.log(Level.SEVERE, e.getMessage(), e);
            }
        }

        return properties;
    }

	
	/**
	 * @param name Name of property
	 * @return Value of the property or null if not set.
	 */
	private String getProperty(String name)
	{
		return properties.getProperty(name);
	}
	
	private String getProperty(String name, String def) {
		return getProperties().getProperty(name, def);
	}

	private boolean getBooleanProperty(String name, boolean def) {
		String prop = getProperty(name, Boolean.toString(def));
		return (prop.equals("true") || prop.equals("yes"));
	}
	
	private Properties getProperties()
	{
		return properties;
	}
	
	private boolean deleteConfigFile()
	{
		File file = new File(EVENT_ROUTER_PROPERTY_FILE);
		return file.delete();			
	}
	
	private void displayFinalMap(Map<String,Map<String,List<String>>> finalMap){
		for (Map.Entry<String,Map<String,List<String>>> entry : finalMap.entrySet()) {
		     String parentKey = entry.getKey();
		     Map<String,List<String>> mapList =  entry.getValue();
		     System.out.println("Parent Key :"+parentKey);
		     for (Map.Entry<String,List<String>> childentry : mapList.entrySet()) {
		    	 String childKey = childentry.getKey();
			     List<String> list =  childentry.getValue();
		       System.out.println("childKey :"+childKey);
		       System.out.println("Value :"+list);
		     }
		    System.out.println("***********************************************"); 
		}
	}
	
	public static void main(String[] args) {
		ReadProperty demo = new ReadProperty();
		demo.ReadPropertyAndSave();
	}

}
