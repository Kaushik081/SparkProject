package com.myProjects

import java.io.{BufferedReader, FileInputStream, InputStreamReader}

import org.slf4j.LoggerFactory



case class Schemas(fromSchema:String,toSchema:String,destinationTopic:String) extends Serializable

object AvroSchemaRegistor extends Serializable {

  private val LOGGER = LoggerFactory.getLogger(getClass)


  def loadSchema(resource : String , sep :Char =','): Map[Int,Schemas] = {
    val inputStream = new FileInputStream(resource)
    try {
      loadSchema(inputStream,sep)
    } finally {
      inputStream.close()
    }
  }

  def loadSchema(inputStream: FileInputStream ,sep:Char):  Map[Int,Schemas] ={
    val SchemaRegistory = collection.mutable.Map[Int ,Schemas]()
    val reader = new BufferedReader(new InputStreamReader(inputStream))
    if (reader.ready){
      reader.readLine() //comment this line if it doesnot include header
      while(reader.ready){
        val line = reader.readLine()
        val splitValue = line.split(sep)
        val schemaID = splitValue(0).toInt
        val srcSchema = splitValue(1)
        val destSchema = splitValue(2)
        val destTopic = splitValue(3)
        try {
          val fromSchema  = scala.io.Source.fromFile(srcSchema).mkString
          val toSchema = scala.io.Source.fromFile(destSchema).mkString
          val value = Schemas(fromSchema,toSchema,destTopic)
          SchemaRegistory += (schemaID -> value)
        }catch {
          case t: Throwable => LOGGER.error(s"Error reading schema for topic $schemaID : ${t.getMessage}")
        }
      }
    }
    SchemaRegistory.toMap
  }

}

