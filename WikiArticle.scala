import scala.util.matching.Regex
import scala.xml.XML
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._

/*
* Author: Joshua Kang
*
*	WikiArticle takes processed wiki dump and uses an XML parser to extract only the articles. Stores the results
*		    		as a text file. Then proceeds to count the number of words from all articles as well
*				as the number of unique words from the articles. Note: This program removes the citations,
*				xml syntax, nonalphanumeric characters, and other words that are not in the article. Dates 
*				are considered valid as words as well as years. Gets rid of hyphens (ex. considers a-time as atime, 
*			    	one word because grammatically it is considered one word.
*/

object WikiArticle
{
	/*
         *textClean: takes a String and returns a clean String without special chars, citations, periods, question marks, and ' chars
         */
        def textClean(line: String) : String =
        {
		//removes all xml tags
		var temp = line.replaceAll("<.*?>", " ")
		
		//removes all citations
		temp = temp.replaceAll("\\{\\{.*?\\}\\}", " ")

		//removes all http links
                temp = temp.replaceAll("\\[https?:[\\w#=?.\\/&\\-]*\\s'", " '")
		temp = temp.replaceAll("\\[https?:.*?\\]", " ")	

		//removes all categories, and miscellaneous links/notes
                temp = temp.replaceAll("\\[\\s*([C|c]ategory|[W|w]ik[\\w]*|[F|f]ile):.*?\\]", " ")	

		//removes the rest of non-word characters except for remaining ', whitespace, hyphens, / chars, and % chars
                temp = temp.replaceAll("[^\\.\\|\\?\\s\\w'\\-]", " ") 

		//removes all | chars
		temp = temp.replaceAll("[\\w\\s]*\\|[\\w\\s]*", " ")
	
		//removes all ' marks to the left and right of words (Trump's is still valid)
                temp = temp.replaceAll("\\s?'{1,}|'{1,}\\s?", " ")

         	//removes all periods and question marks, replacing them with a whitespace to avoid combined words
         	temp = temp.replaceAll("(\\.[\\s]*|[\\s]*\\.|[\\s]*\\?|\\?[\\s]*)", " ")

		//removes all extra whitespace
		temp = temp.replaceAll("\\s{2,}", " ")
		
		//removes all hyphens
		temp = temp.replaceAll("\\-", "")

         	//returns contents of String as lowercase to account for uppercase and lowercase words as the same word
         	temp = temp.toLowerCase

              	return temp
       	}

	/*
	 *isArticle: takes a string (the contents within the text tags) and returns true if it is an article, false if
	 *	     it is a stub, disambiguation, or a redirect.
	 */
	def isArticle(text: String) : Boolean =  
	{	
	
		var status: Boolean = true		
					
		//captures category text
		if(text.startsWith("[[category") && text.endsWith("]]")) 
		{
			status = false
		}
		//captures redirects
		else if(text.containsSlice("#redirect"))
		{
			status = false
		}
		//captures lists 
		else if(text.containsSlice("toc}}"))
		{
			status = false
		}
		//captures stubs
		else if(text.containsSlice("stub}}"))
		{    
		        status = false
		}
		//captures disambiguations
		else if(text.containsSlice("disambiguation}}") 
		     	|| text.containsSlice("{{disambiguation") 
			|| text.containsSlice("{{geodis}}") 
			|| text.containsSlice("{{hndis") 
			|| text.containsSlice("{{numberdis}}") 
			|| text.containsSlice("{{letter-numbercombdisambig}}"))
		{
			status = false		
		}

		return status		
	}

        def main(args: Array[String])
        {
		val sparkConf = new SparkConf().setMaster("local").setAppName("Wiki WordCount")
		val sc = new SparkContext(sparkConf)
		val txt = sc.textFile(“output”)

		val xml = txt.map{ page =>
		    var line = XML.loadString(page)
		    val title = (line \ "title").text
		    val text = (line \\ "text").text
		    (title, text)
		}
		
		//checks content of each text and returns only articles
		val articles = xml.filter(r => isArticle(r._2.toLowerCase))
		val count = articles.count()
		println("The number of articles is: " + count)

		articles.saveAsTextFile("./articles")  


		//extra credit attempt - takes a long time

		//create revised version of article content using textClean function
		val cleanTitle = articles.map(line => textClean(line._1))
        	val cleanText = articles.map(line => textClean(line._2))

		//count all words from both title and text
		val words = cleanTitle.union(cleanText)
		
        	//create flatMap of article content to split and store individual words
        	//splits each line by whitespace, then filters for whitespace/empty strings
        	val wordFlatMap = words.flatMap(line => line.split(" ")).filter(line => line != null && !line.isEmpty())
        
		//getting the total word count from flatMap
        	val wordCount = wordFlatMap.count()

		//create map out of flatMap
        	//first maps each word with a number count, then accumulates number of times each word appears in the text,
        	//finally sorts the map alphabetically by its key (the words)
        	val wordMap = wordFlatMap.map(word => (word, 1)).reduceByKey(_+_).sortByKey()
        
		//create new RDD containing only distinct keys from map and then storing the number of distinct words
        	val uniqueRDD = wordMap.distinct()
        	val unique = uniqueRDD.count()

        	//printing the collective words and word counts for each word 
        	var counts = ("The total word count was: " + wordCount + "\n"
                   + "The total number of unique words was: " + unique + "\n\n")

        	println(counts)
	
        	wordMap.saveAsTextFile("WikiOutput")
	}	
}