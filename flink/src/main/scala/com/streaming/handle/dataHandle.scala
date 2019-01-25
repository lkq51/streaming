package com.streaming.handle

import java.text.SimpleDateFormat
import java.util.Date

import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time

object dataHandle {
	def main(args: Array[String]): Unit = {
		val simple = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS")

		val port: Int = try{
			ParameterTool.fromArgs(args).getInt("port")
		}catch {
			case e: Exception => {
				System.err.println("no port specified. Please run 'dataHandle --port <port>'")
				return
			}
		}

		val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
		val text: DataStream[String] = env.socketTextStream("kason-pc", port, '\n')
		val words = text.flatMap{w => w.split("\\s")}
  		.map{ w => data(w, 1, simple.format(new Date(System.currentTimeMillis())))}
  		.keyBy("word")
  		.timeWindow(Time.seconds(10), Time.seconds(1))
  		.sum("count")

		text.print().setParallelism(1)
		env.execute("Socket dataHandle")
	}

	case class data(word: String, count: Long, time: String)
}
