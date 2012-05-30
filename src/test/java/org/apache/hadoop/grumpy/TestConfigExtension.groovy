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

package org.apache.hadoop.grumpy

import org.apache.hadoop.conf.Configuration


class TestConfigExtension extends GroovyTestCase {


  public void testInit() {

    new GrumpyInit()

  }


  public void testConfigSet() {
    new GrumpyInit()
    Configuration conf = new Configuration()
    conf['t'] = 'tv'
    assert 'tv' == conf.get('t')
  }

  public void testConfigGet() {
    new GrumpyInit()
    Configuration conf = new Configuration()
    conf.set('t', 'tv')
    assert 'tv' == conf['t']
  }


  public void testHelperMap() throws Throwable {
    new GrumpyInit()
    Configuration conf = new Configuration();
    conf.add(
        'testget': true,
        'testset': 4,
        'teststr': 'str'
    )
    assert conf.get('testget')
    assert '4' == conf.get('testset')
    assert 'str' == conf.get('teststr')
  }

  public void testAll() throws Throwable {
    new GrumpyInit()
    Configuration conf = new Configuration();
    conf.add(
        'testget': true,
        'testset': 4,
        'teststr': 'str'
    )
    conf['teststr', 'str2']
    assert conf['testget']
    assert '4' == conf['testset']
    assert 'str2' == conf['teststr']
  }

}
