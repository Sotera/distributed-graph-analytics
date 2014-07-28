package com.soteradefense.dga.graphx.parser

import com.soteradefense.dga.graphx.config.Config
import junit.framework.TestCase
import org.junit.Assert.fail
import org.junit.Test

class CommandLineParserTest extends TestCase {

  @Test
  def testCommandLineWithOnlyIO() {
    val args = Array("-i", "/path/to/input/", "-o", "/path/to/output/")
    val cmdLine: Config = new CommandLineParser().parseCommandLine(args)
    assert(cmdLine.input.equals("/path/to/input/"))
    assert(cmdLine.output.equals("/path/to/output/"))
    assert(cmdLine.master.equals("local"))
    assert(cmdLine.appName.equals("GraphX Analytic"))
    assert(!cmdLine.kryo)
  }

  @Test
  def testCommandLineWithKryoEnabled() {
    val args = Array("-i", "/path/to/input/", "-o", "/path/to/output/", "-k", "true")
    val cmdLine: Config = new CommandLineParser().parseCommandLine(args)
    assert(cmdLine.input.equals("/path/to/input/"))
    assert(cmdLine.output.equals("/path/to/output/"))
    assert(cmdLine.master.equals("local"))
    assert(cmdLine.appName.equals("GraphX Analytic"))
    assert(cmdLine.kryo)
  }


  //TODO: Needs to be done a better way
  @Test
  def testCommandLineWithNoIO() {
    try {
      val args = new Array[String](0)
      val cmdLine: Config = new CommandLineParser().parseCommandLine(args)
      fail()
    }
    catch {
      case illegal: IllegalArgumentException => assert(assertion = true)
      case _: Throwable => fail()
    }
  }

}