

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
import groovy.util.logging.Commons

/**
 * This is the base class for scripted map or reducers
 */
@Commons
class ScriptOperation extends Script {

  public static final String CONTEXT = "context"
  public static final String KEY = "key"
  public static final String VALUE = "value"
  def owner;
 
  def info(def text) {
    log.info(text)
  }  

  def emit(def key, def value) {
    def ctx = getContext()
    ctx.write(key, value);
  }

  def getContext() {
    getProperty(CONTEXT)
  }

  def getKV() {
    def key = getProperty(KEY)
    def value = getProperty(VALUE)
    [key, value]
  }
  
  def increment(def group, def key, int value) {
    context.getCounter(group, key).increment(value)
  }

  @Override
  Object run() {
    def (k, v) = getKV()
    info("in the script key=$k value=$v")
    increment("ScriptOperation","lines",1)
    
  }
}
