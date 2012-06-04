/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.grumpy.scripted

import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configured
import org.codehaus.groovy.control.CompilerConfiguration
import org.apache.hadoop.conf.Configuration

/**
 * This is the script compiler. It must be set 
 */
@Commons
class ScriptCompiler extends Configured {

  //classloader to use -defaults to this class's but can be redefined
  ClassLoader classLoader;

  ScriptCompiler() {
    this(null)
  }

  ScriptCompiler(Configuration conf) {
    super(conf)
    classLoader = this.getClass().getClassLoader()
  }

  /**
   * Loads a script from a file
   * @param scriptFile
   * @param baseclass
   * @return
   */
  private Script loadScript(File scriptFile, Class baseclass) {
    if (!scriptFile.exists()) {
      throw new IOException("No script file \"$scriptFile\"")
    }
    String text = scriptFile.getText()
    parseScript(text, baseclass)
  }

  /**
   * 
   * @param text
   * @param baseclass
   * @param owner
   * @param context
   * @return
   */
  public Script parseScript(String text, Class baseclass) {
    Script script;

    CompilerConfiguration compilerConf = new CompilerConfiguration()
    //set the base class for the script. This will be loaded with a new classloader, so the
    //resulting script cannot be cast back to an instance, or invoked with new types.
    compilerConf.setScriptBaseClass(baseclass.name)

    //instead params are passed down via the binding
    Binding binding = new Binding()
    binding.setVariable("conf", getConf())

    GroovyShell shell = new GroovyShell(classLoader, binding, compilerConf)
    script = shell.parse(text)
    if (!script) {
      throw new IOException("Null script")
    }
    script.setProperty('configuration', compilerConf)

    //script.init()
    script
  }

  /**
   * 
   * @param scriptText text of script
   * @param compilerConf the compiler configuration
   * @param owner owning MR
   * @param context Map or Reduce context
   * @return the parsed script
   */
  public Script parse(String scriptText, 
                                  def owner, 
                                  def context) {
    
    Script script = parseScript(scriptText, ScriptOperation)
    //set the properties of the owner class
    script.setProperty('owner', owner)
    script.setProperty('context', context)
    script
  }
  


}
