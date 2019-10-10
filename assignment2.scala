// Use the named values (val) below whenever your need to
// read/write inputs and outputs in your program. 
import java.io._
val inputFilePath  = "input.txt"
val outputDirPath = "output.csv"


// Write your solution here
//get the file
val file = sc.textFile(inputFilePath,1)
//get the split lines and put them in array
val lines = file.map(x => x.split(","))
//to drop the lines not in right format
val url_array = lines.filter(x => x.length == 4)
//get url and payload and convert to pair
val pairs = url_array.map(x => (x(0),x(3)))
//convert payload from KB MB to B
val new_pair = pairs.map(x => if (x._2.charAt(x._2.length-2) == 'M') (x._1,x._2.substring(0,x._2.length-2).toInt*1024*1024) else if (x._2.charAt(x._2.length-2) == 'K') (x._1,x._2.substring(0,x._2.length-2).toInt*1024) else
(x._1,x._2.substring(0,x._2.length-1).toInt))
//a case class to cal min max mean and variance
case class cal(var x: Int, var min: Int, var max: Int, var plist: List[Int]){

	def stats(b: cal): cal = cal(
		x,
		math.min(min, b.min),
		math.max(max, b.max),
		plist :+ b.x
	)

	def print_res(): String = {
		val mean = plist.sum.toDouble/plist.size
		val variance = plist.map(x => math.pow(x.toDouble - mean,2)).sum/plist.size
    	return min+"B,"+max+"B,"+"%.0fB,%.0fB".format(math.floor(mean),math.floor(variance))
    	
	}
}
//map to deliver variances to cla class and reduceByKey to get the stats and map to get the output String
val res = new_pair.mapValues(f=> cal(f,f,f,List(f))).reduceByKey(_ stats _).map(x => x._1+","+x._2.print_res).collect()
//to write to file
val filewriter = new BufferedWriter(new FileWriter(new File(outputDirPath)))

res.foreach(line => {
	filewriter.write(line)
	filewriter.write("\n")
})

filewriter.close()
