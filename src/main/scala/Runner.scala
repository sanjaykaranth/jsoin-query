package com.jsonquery.nlp
import edu.stanford.nlp.ie.crf.CRFClassifier

object Runner {

  def run(testStr: String): Unit = {

    val c = CRFClassifier.getClassifierNoExceptions("./out.ser.gz")

    System.out.println( testStr + " => " + c.classifyToString(testStr))

  }
}
