package com.jsonquery.main

import java.util.Properties
import java.util.concurrent.TimeUnit

import akka.http.scaladsl.marshallers.sprayjson.SprayJsonSupport
import akka.http.scaladsl.server.Directives
import com.jsonquery.main.Test.print
import com.jsonquery.main.Utils.MongoHelpers
import org.mongodb.scala.{Document, MongoClient, MongoDatabase}
import spray.json.DefaultJsonProtocol
import ujson.Js
import ujson.Js.Value

import scala.collection.mutable

object Test {
  def print(s:String) = System.out.println(s)

  def run(): Unit = {
    print("Main")

    val keyWL = List("inhouseCapabilities", "machinery.name", "addresses.city", "addresses.state")
    Utils.generateTrainingFile(keyWL, "./newTokens.json", 80)
    print("Training file generated")

    val query = "Welding Trichy"
    val labels = Utils.labelQuery(query, "./newTokens.json")
    print("Labeling done")
    print(labels.toString)

    print("Searching..")
    val docs = Search.searchDocs(labels,80)
    print("Done. Doc list:" + docs.toString)
  }
}

import akka.actor.ActorSystem
import akka.http.scaladsl.Http
import akka.http.scaladsl.model._
import akka.stream.ActorMaterializer
import scala.io.StdIn

import java.io.InputStream
import java.security.{ SecureRandom, KeyStore }
import javax.net.ssl.{ SSLContext, TrustManagerFactory, KeyManagerFactory }

import akka.actor.ActorSystem
import akka.http.scaladsl.server.{ Route, Directives }
import akka.http.scaladsl.{ ConnectionContext, HttpsConnectionContext, Http }
import akka.stream.ActorMaterializer
import com.typesafe.sslconfig.akka.AkkaSSLConfig
final case class Labels( map: Map[String,Set[String]])
final case class DocSet(docs: Set[String])
final case class KeyWhiteList(numDocs:Int, keyWL: Set[String])
final case class LabelswFlag( labels: Labels, numDocs: Int, fullDoc: Boolean)

trait JsonSupport extends SprayJsonSupport with DefaultJsonProtocol {
  implicit val labelsFormat = jsonFormat1(Labels)
  implicit val docsFormat = jsonFormat1(DocSet)
  implicit val keyWLFormat = jsonFormat2(KeyWhiteList)
  implicit val labelswFlagFormat = jsonFormat3(LabelswFlag)

}

import ch.megard.akka.http.cors.scaladsl.CorsDirectives._

object Main extends Directives with JsonSupport{
  import spray.json._
  def main(args: Array[String]) {

    implicit val system = ActorSystem("my-system")
    implicit val materializer = ActorMaterializer()
    // needed for the future flatMap/onComplete in the end
    implicit val executionContext = system.dispatcher

    val port = args(0).toInt
    val port2 = args(1).toInt

    //https stuff
    val password: Array[Char] = "jsonquery".toCharArray // do not store passwords in code, read them from somewhere safe!

    val ks: KeyStore = KeyStore.getInstance("PKCS12")
    val keystore: InputStream = getClass.getClassLoader.getResourceAsStream("keystore.pkcs12")

    require(keystore != null, "Keystore required!")
    ks.load(keystore, password)

    val keyManagerFactory: KeyManagerFactory = KeyManagerFactory.getInstance("SunX509")
    keyManagerFactory.init(ks, password)

    val tmf: TrustManagerFactory = TrustManagerFactory.getInstance("SunX509")
    tmf.init(ks)

    val sslContext: SSLContext = SSLContext.getInstance("TLS")
    sslContext.init(keyManagerFactory.getKeyManagers, tmf.getTrustManagers, new SecureRandom)
    val https: HttpsConnectionContext = ConnectionContext.https(sslContext)

    val route =
      cors() {
      path("queryFromLabels") {
        post {
          entity(as[LabelswFlag]) { (labels) =>
            complete {

              print("Searching..")
              val docs = Search.searchDocs(mutable.HashMap(labels.labels.map.toSeq:_*), labels.numDocs,labels.fullDoc)
              print("Done. Doc list:" + docs.toString)

              DocSet(docs)
            }
          }
        }
      }} ~
      cors() {
      path("labelQuery") {
        get {
          parameters('q.as[String]) { (q) =>
            complete {
              val labels = Utils.labelQuery(q, "./newTokens.json")
              print("Labeling done")
              print(labels.toString)

              Labels(labels.toMap)
            }
          }
        }
      }} ~
      cors() {
      path("train") {
        post {
          entity(as[KeyWhiteList]) { (keyWL) =>
            complete {
//              val keyWL = List("inhouseCapabilities", "machinery.name", "addresses.city", "addresses.state")

              Utils.generateTrainingFile(keyWL.keyWL.toList, "./newTokens.json", keyWL.numDocs)
              HttpEntity(
                ContentTypes.`text/html(UTF-8)`,
                "Training done with whitelist = {" + keyWL.keyWL.mkString("\t") + "} over " + keyWL.numDocs.toString + " documents"
              )
            }
          }
        }
      }}

    val bindingFutures = Http().bindAndHandle(route, "0.0.0.0", port)
    val bindingFutures2 = Http().bindAndHandle(route, "0.0.0.0", port2, connectionContext=https)

    println(s"Server online at http://localhost:" + port.toString + "/\nPress RETURN to stop...")
    StdIn.readLine() // let it run until user presses return
    bindingFutures
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done

    bindingFutures2
      .flatMap(_.unbind()) // trigger unbinding from the port
      .onComplete(_ => system.terminate()) // and shutdown when done

  }
}

object Utils {

  import scala.collection.mutable.HashMap

  import ujson.Js

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

  def clean(s: String) = s.trim().replaceAll("\"", "").toLowerCase

  def getLeaves(json: Value, path: List[String]): Set[String] = {
    if (json == Js.Null)
      return Set()

    if (path.isEmpty) { //leaf node
      json match {
        case o: Js.Obj => o.value.values.map(_.toString).toSet //what to do with map?
        case a: Js.Arr => a.value.map(_.toString).toSet
        case _ => Set(json.toString)
      }
    }
    else {
      json match {
        case obj: Js.Obj => getLeaves(obj.value.getOrElse(path.head, Js.Null), path.tail)
        case arr: Js.Arr => arr.value.flatMap(o => getLeaves(o, path)).toSet
        case _ => Set("ERROR") //exception, shouldnt be here
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

    val hm = new HashMap[String, Set[String]]

    for (k <- keyWhiteList)
      for (json <- jsons)
        hm.update(k, hm.getOrElse(k, Set()) ++ getLeaves(json, k.split('.').toList).map(clean(_)))

    //for json
    val lhm = new mutable.LinkedHashMap[String, Js.Value]
    keyWhiteList.foreach(k => lhm += (k -> Js.Arr.from(hm.getOrElse(k, Set()))))
    val json = Js.Obj(lhm)

    writer.write(json.toString)

    writer.close
  }

  def strMatch( v: String, token: String ): Boolean ={
    v.contains(token)
  }

  def labelQuery(q: String, trainFilePath: String): HashMap[String, Set[String]] = {
    import scala.io.Source

    val tokens = q.split(" ").map(clean(_))
    val json = ujson.read(Source.fromFile(trainFilePath).getLines.mkString)

    val labels: HashMap[String, Set[String]] = HashMap() //cat -> Set of instances

    for ((k, a) <- json.obj) {
      a.arr.map(_.toString).foreach(v =>
        tokens.foreach(t =>
          if (strMatch(v, t))
            labels.update(k, labels.getOrElse(k, Set())+v)
        )
      )
    }
    labels
  }

}
object Search {
  import mutable.HashMap
  def searchDocs(labels: HashMap[String, Set[String]], numDocs: Int, fullDoc:Boolean =true): Set[String] = {

    if(labels.isEmpty)
      return Set()

    val c: MongoClient = MongoClient()
    val db: MongoDatabase = c.getDatabase("zw")

    import MongoHelpers._
    val jsons: Seq[Value] = db.getCollection("sme").find().results().take(numDocs).map(d => ujson.read(d.toJson))

    if(fullDoc)
      jsons.filter(searchDoc(_, labels)).map(_.toString) toSet
    else
      jsons.filter(searchDoc(_, labels)).map(json => json("smeId").str) toSet
  }

  def searchDoc( json: Js.Value, labels: HashMap[String, Set[String]]): Boolean = {
    for((k,v) <- labels ){
      val leaves: Set[String] = Utils.getLeaves( json, k.split('.').toList ).map(Utils.clean(_))
      if(v.map(Utils.clean(_)).intersect(leaves).isEmpty)
        return false
    }
    true
  }
}


