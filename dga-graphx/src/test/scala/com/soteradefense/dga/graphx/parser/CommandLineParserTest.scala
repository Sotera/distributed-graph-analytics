/*
 *
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *       http://www.apache.org/licenses/LICENSE-2.0
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

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