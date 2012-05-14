/*
* *
*  * Licensed to the Apache Software Foundation (ASF) under one
*  * or more contributor license agreements.  See the NOTICE file
*  * distributed with this work for additional information
*  * regarding copyright ownership.  The ASF licenses this file
*  * to you under the Apache License, Version 2.0 (the
*  * "License"); you may not use this file except in compliance
*  * with the License.  You may obtain a copy of the License at
*  *
*  *     http://www.apache.org/licenses/LICENSE-2.0
*  *
*  * Unless required by applicable law or agreed to in writing, software
*  * distributed under the License is distributed on an "AS IS" BASIS,
*  * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
*  * See the License for the specific language governing permissions and
*  * limitations under the License.
*  
*/

package org.apache.hadoop.grumpy

import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configured
import org.codehaus.groovy.control.CompilerConfiguration

@Commons
class ScriptCompiler extends Configured {

  private Script loadScript(File scriptFile, Class baseclass) {
    if (!scriptFile.exists()) {
      throw new IOException("No script file \"$scriptFile\"")
    }
    String text = scriptFile.getText()
    parseScript(text, baseclass)
  }

  public Script parseScript(String text, Class baseclass) {
    Script script;

    CompilerConfiguration cconf = new CompilerConfiguration()
    //set the base class for the script. This will be loaded with a new classloader, so the
    //resulting script cannot be cast back to an instance, or invoked with new types.
    cconf.setScriptBaseClass(baseclass.name)

    //instead params are passed down via the binding
    Binding binding = new Binding()
    binding.setVariable("configuration", getConf())
    GroovyShell shell = new GroovyShell(this.getClass().getClassLoader(), binding, cconf)
    script = shell.parse(text)
    if (!script) {
      throw new IOException("Null script")
    }
    //script.init()
    script;
  }

}
