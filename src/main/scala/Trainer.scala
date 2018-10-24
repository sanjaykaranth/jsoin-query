package com.jsonquery.nlp

import edu.stanford.nlp.ie.AbstractSequenceClassifier
import edu.stanford.nlp.ie.crf._
import edu.stanford.nlp.io.IOUtils
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.ling.CoreAnnotations
import edu.stanford.nlp.sequences.DocumentReaderAndWriter
import edu.stanford.nlp.util.Triple
import edu.stanford.nlp.ie.crf.CRFClassifier
import edu.stanford.nlp.ling.CoreLabel
import edu.stanford.nlp.sequences.SeqClassifierFlags
import edu.stanford.nlp.util.StringUtils
import java.util.Properties
import java.util.concurrent.TimeUnit

import org.mongodb.scala.{Document, MongoClient, MongoDatabase}
import ujson.Js.Value

object Trainer {

  def run(): Unit = {
    System.out.println("Main")

    val keyWL = List( "inhouseCapabilities",  "machinery.name", "addresses.city" )
    Utils.generateTrainingFile(keyWL, "./generatedTokens.tsv", 80)
    Utils.trainAndWrite( "./out.ser.gz", "./ner.properties", "./generatedTokens.tsv")
  }
}

object Utils {

  object MongoHelpers {
    import org.mongodb.scala._
    import scala.concurrent.Await
    import scala.concurrent.duration.Duration

    implicit class DocumentObservable[C](val observable: Observable[Document]) extends ImplicitObservable[Document] {
      override val converter: (Document) => String = (doc) => doc.toJson
    }

    implicit class GenericObservable[C](val observable: Observable[C]) extends ImplicitObservable[C] {
      override val converter: (C) => String = (doc) => doc.toString
    }

    trait ImplicitObservable[C] {
      val observable: Observable[C]
      val converter: (C) => String

      def results(): Seq[C] = Await.result(observable.toFuture(), Duration(10, TimeUnit.SECONDS))
      def headResult() = Await.result(observable.head(), Duration(10, TimeUnit.SECONDS))
      def printResults(initial: String = ""): Unit = {
        if (initial.length > 0) print(initial)
        results().foreach(res => println(converter(res)))
      }
      def printHeadResult(initial: String = ""): Unit = println(s"${initial}${converter(headResult())}")
    }
  }

  import ujson.Js


  def clean( s: String ) = s.trim().replaceAll("\"", "")

  def trainAndWrite(modelOutPath: String, prop: String, trainingFilepath: String): Unit = {
    val props = StringUtils.propFileToProperties(prop)
    props.setProperty("serializeTo", modelOutPath)
    //if input use that, else use from properties file.
    if (trainingFilepath != null) props.setProperty("trainFile", trainingFilepath)
    val flags = new SeqClassifierFlags(props)
    val crf = new CRFClassifier[CoreLabel](flags)
    crf.train()
    crf.serializeClassifier(modelOutPath)
  }

  def getLeaves( json: Value, path: List[String] ): List[String] = {
    if(json == Js.Null )
      return List()

    if(path.isEmpty) { //leaf node
      json match {
        case o: Js.Obj => o.value.values.map(_.toString).toList //what to do with map?
        case a: Js.Arr => a.value.map(_.toString).toList
        case _ => List(json.toString)
      }
    }
    else {
      json match {
        case obj: Js.Obj => getLeaves(obj.value.getOrElse(path.head, Js.Null),path.tail)
        case arr: Js.Arr => arr.value.flatMap(o => getLeaves(o,path)).toList
        case _ => List("ERROR") //exception, shouldnt be here
      }
    }
  }

  def generateTrainingFile(keyWhiteList: List[String], filePath: String, numDocs: Int) = {
    //mongoimport --db zw --collection sme --file ./sme-profile.json

    import java.io.File
    import java.io.PrintWriter

    val writer = new PrintWriter(new File(filePath))

    val c: MongoClient = MongoClient()
    val db: MongoDatabase = c.getDatabase("zw")

    import MongoHelpers._
    val jsons: Seq[Value] = db.getCollection("sme").find().results().take(numDocs).map(d => ujson.read(d.toJson))

    for( json <- jsons ) {
      for (k <- keyWhiteList) {
        val leaves = getLeaves(json,k.split('.').toList).distinct
        leaves.foreach(l => writer.write(clean(l) + "\t" + k + "\n"))
      }

      writer.write("\n")
    }

    writer.close
  }
}


