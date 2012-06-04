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

package org.apache.hadoop.grumpy.scripted.test

import org.apache.hadoop.grumpy.projects.bluemine.mr.MapToHour
import org.apache.hadoop.grumpy.projects.bluemine.mr.testtools.BluemineTestBase
import org.apache.hadoop.grumpy.scripted.ScriptedMapper
import org.apache.hadoop.grumpy.scripted.ScriptKeys
import org.apache.hadoop.grumpy.output.NewAPIExtTextOutputFormat
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.grumpy.projects.bluemine.reducers.CountReducer
import org.apache.hadoop.grumpy.GrumpyJob

class ScriptedMRTest extends BluemineTestBase {

  String mapScript = """
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
    
emit(new Text("lines"), new IntWritable(1))
  """
  
  
  String reduceScript = """
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
    
def (k, values) = getKV()
def sum = values.collect() {it.get() }.sum()

emit(k, new IntWritable(1))
  """
  
  
  
  void testScriptedMap() {
    runCountJob([:],
                "mapScript", 
                ScriptedMapper, 
                [[ScriptKeys.MAPSCRIPT,mapScript]])
  }

  void testScriptedReduce() {
    GrumpyJob job
    File outDir
    (job, outDir) = createMRJob([:],
                                "mapScript",
                                ScriptedMapper,
                                reduceScript)
    job.applyOptionList([[ScriptKeys.MAPSCRIPT,mapScript]])
    job.applyOptionList([[ScriptKeys.REDSCRIPT,reduceScript]])
    job.mapOutputValueClass = IntWritable
    job.outputFormatClass = NewAPIExtTextOutputFormat
    runJob(job)
    dumpDir(LOG, outDir)
    outDir
  }

}
