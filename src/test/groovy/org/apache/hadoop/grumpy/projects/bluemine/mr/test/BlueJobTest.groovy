package org.apache.hadoop.grumpy.projects.bluemine.mr.test

import org.junit.Test
import org.apache.hadoop.mapred.JobConf

import org.apache.hadoop.mapreduce.OutputFormat
import org.apache.hadoop.grumpy.projects.bluemine.jobs.BluemineJob
import org.apache.hadoop.grumpy.projects.bluemine.mr.testtools.BluemineTestBase
import org.apache.hadoop.grumpy.projects.bluemine.output.ExtTextOutputFormat

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

/**
 * Tests that Bluejob objects get/set operations propagate to configs, as I have been having doubts
 */
class BlueJobTest extends BluemineTestBase {
    
    private BluemineJob job;

    @Override
    protected void setUp() {
        super.setUp()
    }


    @Test
    public void testConfigConstructor() {
        JobConf conf = new JobConf()
        conf.set("testConfigConstructor","true")
        job = new BluemineJob(conf,"testConfigConstructor()")
        assert "testConfigConstructor()" == job.jobName
        assert job.getConfiguration().is(job.configuration)
        assert "true" == job.getConfiguration().get("testConfigConstructor")
        assert job.configuration.get("mapred.job.name") == job.jobName
    }


    @Test
    public void testConfigSetsPropagateBack() {
        job = new BluemineJob("testConfigSetsPropagateBack")
        job.configuration.set("a","a")
        assert job.get("a")=="a"
        job.configuration.setInt("b",1)
        assert job.get("b")=="1"
        job.configuration.setBoolean("c",true)
        assert job.get("c")=="true"
    }

    @Test
    public void testUnknownPropertyGets() {
        job = new BluemineJob("testUnknownPropertyGets")
        job.configuration.set("testUnknownPropertyGets","a") 
        assert "a" == job.'testUnknownPropertyGets'
    }

    @Test
    public void testUnknownPropertySets() {
        job = new BluemineJob("testUnknownPropertySets")
        job.set 'testUnknownPropertySets', true
        assert "true" == job.configuration.get('testUnknownPropertySets')
        job.set 'testUnknownPropertySets1', "1"
        assert "1" == job.configuration.get('testUnknownPropertySets1')
    }

    @Test
    public void testMapSetting() {
        job = new BluemineJob("testUnknownPropertySets")
        Map map = ['bluemine.filter.year':2006, t:true, a:'a']
        map['i']=1
        job.applyOptions(map)
        assert "2006" == job.configuration.get('bluemine.filter.year')
        job.set 'testUnknownPropertySets1', "1"
        assert "1" == job.configuration.get('i')
        assert "true" == job.t
        assert 'a' == job.get('a')
    }

    @Test
    public void testOutputFilterPropagation() {
        job = new BluemineJob("testOutputFilterPropagation")
        assert null == job.configuration.get(MAPREDUCE_OUTPUTFORMAT_CLASS)
        job.setOutputFormatClass(ExtTextOutputFormat)
        assert job.configuration.get("mapreduce.outputformat.class") == ExtTextOutputFormat.name
    }

    @Test
    public void testClassSetter() {
        job = new BluemineJob("testClassSetter")
        job.configuration.setClass(MAPREDUCE_OUTPUTFORMAT_CLASS, ExtTextOutputFormat, OutputFormat.class)
        assert job.configuration.get(MAPREDUCE_OUTPUTFORMAT_CLASS) == ExtTextOutputFormat.name
    }

    @Test
    public void testOutputFormatSetter() {
        job = new BluemineJob("testOutputFormatSetter")
        assert null == job.configuration.get(MAPREDUCE_OUTPUTFORMAT_CLASS) 
        job.setOutputFormatClass(ExtTextOutputFormat)
        assert job.configuration.get(MAPREDUCE_OUTPUTFORMAT_CLASS) == ExtTextOutputFormat.name
    }

}
