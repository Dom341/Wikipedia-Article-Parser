import scala.io.Source
import java.io.PrintWriter
import java.io.File
import scala.collection.mutable.StringBuilder

/*
* Author: Joshua Kang
*
* PreProc program takes the latest wiki dump (October version) and outputs a text file containing only the page content
* 	  from the input file, where each page is contained as a line. The number of pages is also printed to the screen
*
*/

object PreProc 
{
	def main(args: Array[String])
	{
		val inputFile = "enwiki-latest-pages-articles-multistream.xml"
		val outputFile = new PrintWriter(new File("output"))
		var outputLine = new StringBuilder
		var outputStr = new String
		
		//stores the number of pages
		var count = 0

		//Flag is used to determine if current line is within a page 
		var pageFlag = false
	
	
		for (inputLine <- Source.fromFile(inputFile).getLines)
		{
			//if inputLine encounters <page>, set pageFlag to true and signify start of new page
                        if(inputLine.containsSlice("<page>"))
                        {
                          pageFlag = true
                        }

			//if pageFlag is true, inputLine is between <page> and </page> (including <page> and </page>) and is 
			//added to outputLine with leading and trailing whitespaces trimmed. All instances of multiple whitespace
			//is also reduced to 1 whitespace symbol
			if(pageFlag)
                        {
			  var temp = inputLine.trim()

			  //searching for all instances of sequential whitespace (2 or more) and replacing them with only one 
			  temp = temp.replaceAll("\\s{2,}", " ")
			  outputLine.append(temp)
			}

			//if inputLine encounters </page>, set pageFlag to false and signify end of current page
			if(inputLine.containsSlice("</page>"))
			{
			  pageFlag = false
			  outputStr = outputLine.mkString
			  outputFile.write(outputStr)
			  outputFile.write("\n")
			  outputLine.clear()
			  count = count + 1
			}
			
		}

		println("The total number of pages is: " + count)
		
		outputFile.close
	}
}