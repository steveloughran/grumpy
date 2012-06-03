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

import org.apache.commons.cli.CommandLine
import org.apache.commons.cli.Option
import org.apache.commons.cli.Options
import org.apache.hadoop.grumpy.GrumpyJob
import org.apache.hadoop.grumpy.tools.AbstractRunnableTool
import org.apache.hadoop.grumpy.tools.GrumpyToolRunner
import org.apache.hadoop.grumpy.tools.GrumpyUtils
import org.apache.hadoop.mapred.JobConf

class ScriptMain extends AbstractRunnableTool {

  @Override
  Options createToolSpecificOptions() {
    CliBuilder opts = new CliBuilder(usage: "${toolName} [-j jobtracker] [-fs filesystem] [-v] [-D define=value] [-pf  properties] -s sourcedir -o outdir  ")
    // Create the list of options.

    opts.with {
      'do' longOpt: 'deloutdir', args: 1, argName: 'do', 'output directory -delete first'
//      'ms' longOpt: 'mapscript', args: 1, argName: 'scriptmapper', 'Mapper Script'
//      'mr' longOpt: 'reducescript', args: 1, argName: 'scriptreducer', 'Reducer script'
      'o' longOpt: 'outdir', args: 1, argName: 'out', 'directory for destination files'
      'ou' longOpt: 'outurl', args: 1, argName: 'out', 'URL for destination files'
      'of' longOpt: 'opfiles', args: 1, argName: 'optional properties', 'Property file'
      'pf' longOpt: 'pfile', args: 1, argName: 'properties', 'Property file'
      't' longOpt: 'terminate', args: 1, argName: 'terminate', 'terminate job if client program is interrupted '
      'v' longOpt: 'verbose', 'verbose job output'
      'k' longOpt: 'killable', 'kill the job if the client process is killed'
    }

    def mapOpt = new Option('ms', 'mapscript', true, 'Mapper Script')
    mapOpt.required = true
    opts << mapOpt
    def redOpt = new Option('res', 'reducescript', true, 'Reducer Script')
    redOpt.required = true
    opts << redOpt

  }

  @Override
  boolean run(CommandLine commandLine, String[] args) {
    OptionAccessor opts = new OptionAccessor(commandLine)

    File mapscriptF = GrumpyUtils.requiredFile(opts.'ms', "Mapper Script")
    conf[ScriptKeys.MAPSCRIPT] = mapscriptF.text

    File redscriptF = GrumpyUtils.requiredFile(opts.'rs', "Reducer Script")
    conf[ScriptKeys.REDSCRIPT] = redscriptF.text

    GrumpyJob job = GrumpyJob.createBasicJob("scriptedmain",
                                             new JobConf(conf),
                                             ScriptedMapper,
                                             ScriptedReducer)
    return job.submitAndWait(commandLine.hasOption('v'),
                             commandLine.hasOption('k'))
  }


  static void main(String[] args) {
    GrumpyToolRunner.executeAndExit(new ScriptMain(), args)
  }

}
