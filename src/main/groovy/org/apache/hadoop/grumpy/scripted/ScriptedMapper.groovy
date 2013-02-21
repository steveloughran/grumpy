

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

package org.apache.hadoop.grumpy.scripted

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.Writable
import org.apache.hadoop.mapreduce.Mapper

class ScriptedMapper extends Mapper<Writable, Writable, Writable, Writable> {

  private Mapper.Context ctx;

  private Script map;
  private Configuration conf

  protected void setup(Mapper.Context ctx) {
    this.ctx = ctx
    this.conf = ctx.configuration
    ScriptCompiler comp = new ScriptCompiler(conf)
    String scriptText = conf['mapscript']
    map = comp.parse(scriptText, this, ctx)
  }

  protected void map(Writable key, Writable value, Mapper.Context ctx) {
    map.setProperty(ScriptOperation.KEY, key)
    map.setProperty(ScriptOperation.VALUE, value)
    map.setProperty(ScriptOperation.CONTEXT, ctx)

    map.run()
  }

}
