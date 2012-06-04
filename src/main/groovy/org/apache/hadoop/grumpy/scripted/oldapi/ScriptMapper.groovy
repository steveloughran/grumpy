

/*
 Licensed to the Apache Software Foundation (ASF) under one
 or more contributor license agreements.  See the NOTICE file
 distributed with this work for additional information
 regarding copyright ownership.  The ASF licenses this file
 to you under the Apache License, Version 2.0 (the
 "License"); you may not use this file except in compliance
 with the License.  You may obtain a copy of the License at
      http://www.apache.org/licenses/LICENSE-2.0
 
 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.
 */

package org.apache.hadoop.grumpy.scripted.oldapi

import org.apache.hadoop.mapred.Mapper
import org.apache.hadoop.mapred.OutputCollector
import org.apache.hadoop.mapred.Reporter

import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.MapReduceBase
import org.apache.hadoop.grumpy.scripted.ScriptCompiler
import org.apache.hadoop.grumpy.scripted.ScriptKeys

class ScriptMapper extends MapReduceBase implements Mapper {

  private JobConf conf
  private Script operation

  @Override
  void configure(JobConf job) {
    conf = job 
    //load in the file
    ScriptCompiler scriptCompiler = new ScriptCompiler(conf)
    operation = scriptCompiler.parse(conf[ScriptKeys.MAPSCRIPT].toString(),
                                                           this,
                                                           this)
  }
  
  @Override
  void map(Object key, Object value, OutputCollector output, Reporter reporter) {
    operation.setProperty("key",key)
    operation.setProperty("value",value)
    operation.setProperty("output",output)
    operation.setProperty("reporter", reporter)
    operation.run()
  }

  @Override
  void close() {

  }
}

