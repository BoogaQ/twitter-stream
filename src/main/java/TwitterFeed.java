import java.text.DecimalFormat;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.twitter.TwitterUtils;

import scala.Tuple2;
import twitter4j.HashtagEntity;
import twitter4j.Status;

@SuppressWarnings("serial")
public class TwitterFeed {
	// Decimal formatter to have uniform formatting
	private static DecimalFormat df = new DecimalFormat("#.##");
	
	// Twitter keys used to access twitter feed
	final static String consumerKey = "Xy28foJ5YY9EZHXULqNcuQUn9";
    final static String consumerSecret = "p6tMIS8FPOxDr9E1kvQyZdr8jT5qrqPgTzcnEZwbUCByzvVqN3";
    final static String accessToken = "1635494317-drIojeoIYlNMF3eunkwZ9K0lnshUVGW1jCD0Awr";
    final static String accessTokenSecret = "7Lxu3cHF8gvsx9KtwPpkedGAV7jzEqN9EzkNk3Al7qwrV";
    
	public static void main(String[] args) {
		// Set system properties where the twitter stream can access the tokens and keys
        System.setProperty("twitter4j.oauth.consumerKey", consumerKey);
        System.setProperty("twitter4j.oauth.consumerSecret", consumerSecret);
        System.setProperty("twitter4j.oauth.accessToken", accessToken);
        System.setProperty("twitter4j.oauth.accessTokenSecret", accessTokenSecret);
        
        // Initialize spark context
        SparkConf conf = new SparkConf().setMaster("local[2]").setAppName("TwitterFeed");
        
        // Initiate Java streaming context using the spark context and set the duration of window to 1 second.
        JavaStreamingContext jssc = new JavaStreamingContext(conf, new Duration(1000));
        
        // Create an input stream that will be used for all further stream processing
    	JavaReceiverInputDStream<Status> tweets = TwitterUtils.createStream(jssc);
    	
    	// Filter out the non-english tweets. I know this wasn't required, but it made the output messy with all the question marks in it. 
        JavaDStream<Status> twitterStream = tweets.filter((status) -> "en".equalsIgnoreCase(status.getLang()));       
        
        // Stop Spark from spaming its log which interferes with the output of my program.
        jssc.sparkContext().setLogLevel("ERROR");   
        
        // Set checkpoint, needed for ReduceByKeyAndWindow
        String checkpointDir = "C:\\Spark\\checkpoint";
        
        
        // All the answers to the quesitons. These should probably be run one at at a time otherwise the output is a mess.
        TwitterFeed.Question_A3b(twitterStream);
        jssc.checkpoint(checkpointDir);
        
        // Start the stream
        jssc.start();
        
        // Await termination is needed for the stream to print.
        try {
          jssc.awaitTermination();
        } catch (InterruptedException e) {
          e.printStackTrace();
        }
	}

	// A method that outputs the text content of tweets
	public static void Question_A1(JavaDStream<Status> twitterStream) {
		// Iterate through each RDD of tweets that is received each second, and map their text content to a stream of text data
		JavaDStream<String> wordsAndTags = twitterStream.map(
				// The function in the map method for apache stream. The first generic type is the input, in this case status objects from the twitter stream
				// The second generic type is the output of the map, which in this case is string.
        		new Function<Status, String>() {
        			// A required method for the Function object. This method does the mapping from argument to return type.
        			public String call(Status status) {
        				return status.getText(); 
        				}
        			});
		// Print out the tweets on the screen
		wordsAndTags.print();
	}
	// A method to extract the hashtags from each tweet. 
	public static void Question_A2(JavaDStream<Status> twitterStream) {
		// Map through each twitter status object. 
		JavaDStream<String> wordsAndTags = twitterStream.map(
        		new Function<Status, String>() {
        			public String call(Status status) {
        				String hashTags = "";
        				// As each status can have more than one hashtag, this loops through the list of hashtags and appends them to the hashTag string.
        				for (HashtagEntity e : status.getHashtagEntities()) {
        					hashTags += "#" + e.getText() + "\n";
        				}
        				// Transforms each twitter status to this string format. 
        				return "Characters: " + status.getText().length() + 
        						", Words: " + status.getText().split(" ").length + 
        						", HashTags: " + hashTags;
        			}
        		});
		// Print the tweet calculations and hashtags
		wordsAndTags.print();
	}
	// Method to count the average number of characters and words per each collected RDD.
	public static void Question_A3a(JavaDStream<Status> twitterStream) {
		// The map to pair method goes through the stream and maps the input into pairs using the Tuple2 class
		JavaPairDStream<Integer, Integer> charsAndWords = twitterStream.mapToPair(
				// Pair function has to be used for this. THe first generic type is the input, and the 2nd and 3rd type represnet the types of the Tuple2
        		new PairFunction<Status, Integer, Integer>(){
        			// The method that maps each twitter status to a pair of integers, in this case, character and word lengths
        			public Tuple2<Integer,Integer> call (Status in) {
        				return new Tuple2<Integer,Integer>(in.getText().length(), in.getText().split(" ").length);
        			}
        		});
        
        // Use the for each method to go into each RDD of Tuples and perform the required calculations for the collection of tweets
		// foreachRDD requires the use of a void function, which means it's not returning anything.
        charsAndWords.foreachRDD(new VoidFunction<JavaPairRDD<Integer,Integer>>() {
        	public void call(JavaPairRDD<Integer, Integer> rdd) { 
        		// Set the count to 0. THis shows us how many tweets were the calculations performed on and will be used to calculate averages.
        		int count = 0;
        		double totalChars = 0;
        		double totalWords = 0;
        		// Iterate through the elements of this RDD, and calculate the total number of words and characters in the collection
        		for (Tuple2<Integer, Integer> t : rdd.collect()) {
        			totalChars += t._1;
        			totalWords += t._2;
        			count += 1;
        		}
        		// Print out the averages
        		System.out.println("Number of tweets: " + count + 
        						   ", Average Characters: " + df.format(totalChars/count) + ", " +
        						   "Average Words: " + df.format(totalWords/count));
        	}
        });
	}
       
	// Get top hashtags for the tweet stream
    public static void Question_A3b(JavaDStream<Status> twitterStream) {
    	// Use flat map as there might be more than 1 hashtag per tweet
    	JavaDStream<String> hashTags = twitterStream.flatMap(
    			// FlatMapFunction is required for this
        		new FlatMapFunction<Status, HashtagEntity>() {
        			// The function returns an iterator, which we then transform to an arraylist.
        			public Iterator<HashtagEntity> call(Status status) {
        				return Arrays.asList(status.getHashtagEntities()).iterator();
        			}
        		// Once we have collected the hasthags, we map them to their string form and collect them as strings
        		}).map(new Function<HashtagEntity, String>() {
        			public String call(HashtagEntity hte) {
        				return hte.getText();
        		}
        		});
        
    	// Map the stream of strings containing the hashtags to tuples, with hashtag as the key and occurance, in this case 1, as the value.
        JavaPairDStream<String, Integer> tagTuples = hashTags.mapToPair(
        		new PairFunction<String, String, Integer>() {
        			public Tuple2<String,Integer> call (String s) {
        				return new Tuple2<String, Integer>(s, 1);
        			}
        		});
        // Use the reduceByKey method to lump all the Pairs with the same keys together so that one pair for each tag remains with its name as key, and number of times it appears as value
        JavaPairDStream<String, Integer> counts = tagTuples.reduceByKey(
        		// Function2 is used for reduction. The first two generic types are the inputs, and the last generic type is the output. 
				new Function2<Integer, Integer, Integer>() {
					public Integer call(Integer i1, Integer i2) {return i1 + i2;}
				});
        // Swaps the key and value values in the pairs, so that the number of occurances of the hashtag is the key, and the hashtag is the value
        JavaPairDStream<Integer, String> countedTags = counts.mapToPair(
        		// PairFunction takes in a tuple, and returns a tuple. The generic types are reversed to reflect the reversal of the key and value.
        		new PairFunction<Tuple2<String,Integer>, Integer, String>() {
        			public Tuple2<Integer, String> call(Tuple2<String,Integer> in) {
        				return in.swap();
        			}
        		});
        // This transforms the current RDD into a sorted one. This allows for printing top hashtags
        JavaPairDStream<Integer, String> sortedCounts = countedTags.transformToPair(
        		// Takes in a PairRDD and returns a PairRDD
        		new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer,String>>() {
        			public JavaPairRDD<Integer, String> call (JavaPairRDD<Integer, String> in) throws Exception {
        				// Sorts in descending order
        				return in.sortByKey(false);
        			}
        		});
        // For each RDD, print the top elements. Since this is sorted in decreasing order, it shows the top occuring hashtags
        sortedCounts.foreachRDD(
       	     new VoidFunction<JavaPairRDD<Integer, String>> () {
       	       public void call(JavaPairRDD<Integer, String> rdd) {
       	         String out = "\nTop 10 hashtags:\n";
       	         for (Tuple2<Integer, String> t: rdd.take(10)) {
       	           out = out + t.toString() + "\n";
       	         }
       	         System.out.println(out);
       	       }
       	     }
       	   );
	}	
    // Performs the same calculations as in A3b, but with added window. The code is largely the same as in question A3a and A3b so I won't comment on every method as i've done that in previous methods.
    // I will comment on the code that is different.
	public static void Question_A3c(JavaDStream<Status> twitterStream) {
		
		JavaPairDStream<Integer, Integer> charsAndWords = twitterStream.mapToPair(
        		new PairFunction<Status, Integer, Integer>(){
        			public Tuple2<Integer,Integer> call (Status in) {
        				return new Tuple2<Integer,Integer>(in.getText().length(), in.getText().split(" ").length);
        			}
        		});
        // The only difference here from A3a, is that the stream is passed through a window that captures the last 5 minutes of tweets, and returns calculations every 30 seconds. 
        JavaPairDStream<Integer, Integer> charsAndWordsWindow = charsAndWords.window(new Duration(5 * 60 * 1000), new Duration(30 * 1000));
        
        charsAndWordsWindow.foreachRDD(new VoidFunction<JavaPairRDD<Integer,Integer>>() {
        	public void call(JavaPairRDD<Integer, Integer> rdd) {   
        		int count = 0;
        		double averageChars = 0;
        		double averageWords = 0;
        		
        		for (Tuple2<Integer, Integer> t : rdd.collect()) {
        			averageChars += t._1;
        			averageWords += t._2;
        			count += 1;
        		}
        		System.out.println("Number of tweets: " + count + ", Average Characters: " + averageChars/count + ", Average Words: " + averageWords/count);
        	}
        });
		
		JavaDStream<String> hashTags = twitterStream.flatMap(
        		new FlatMapFunction<Status, HashtagEntity>() {
        			public Iterator<HashtagEntity> call(Status status) {
        				return Arrays.asList(status.getHashtagEntities()).iterator();
        			}
        		}).map(new Function<HashtagEntity, String>() {
        			public String call(HashtagEntity hte) {
        				return hte.getText();
        		}
        		});
        JavaPairDStream<String, Integer> tuples = hashTags.mapToPair(
        		new PairFunction<String, String, Integer>() {
        			public Tuple2<String, Integer> call(String in) {
        				return new Tuple2<String,Integer>(in, 1);
        			}  	
        		});
        // The previous method was reduceByKey, while here we reduce by key and window. It takes two functions:
        JavaPairDStream<String, Integer> counts = tuples.reduceByKeyAndWindow(
        				// One function to reduce by key and count the number of hashtag occurances
        				new Function2<Integer, Integer, Integer>() {
        					public Integer call(Integer i1, Integer i2) {return i1 + i2;}
        				},
        				// A second function that is used for when tweets leave the allocated window. It reduces the hashtag count once it has been longer than 5 minutes.
        				new Function2<Integer, Integer, Integer>() {
        					public Integer call(Integer i1, Integer i2) {return i1 - i2;}
        				},
        				// The window of tweets
        				new Duration(60 * 5 * 1000),
        				// The sliding window when calculations are done.
        				new Duration(30 * 1000));
        
        JavaPairDStream<Integer, String> swappedCounts = counts.mapToPair(
        		new PairFunction<Tuple2<String,Integer>, Integer, String>() {
        			public Tuple2<Integer, String> call(Tuple2<String,Integer> in) {
        				return in.swap();
        			}
        		});
        JavaPairDStream<Integer, String> sortedCounts = swappedCounts.transformToPair(
        		new Function<JavaPairRDD<Integer, String>, JavaPairRDD<Integer,String>>() {
        			public JavaPairRDD<Integer, String> call (JavaPairRDD<Integer, String> in) throws Exception {
        				return in.sortByKey(false);
        			}
        		});
        sortedCounts.foreachRDD(
        	     new VoidFunction<JavaPairRDD<Integer, String>> () {
        	       public void call(JavaPairRDD<Integer, String> rdd) {
        	         String out = "\nTop 10 hashtags:\n";
        	         for (Tuple2<Integer, String> t: rdd.take(10)) {
        	           out = out + t.toString() + "\n";
        	         }
        	         System.out.println(out);
        	       }
        	     }
        	   );
		}
	
}