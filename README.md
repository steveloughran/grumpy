# grumpy

Groovy Map Reduce Library

## History

This was a prototype library for performing Map Reduce jobs on Apache Hadoop in the Groovy language. It is obsolete, being designed to work with the 2.0.x-alpha era of Hadoop 2.

Some of the classes were adopted by [Hoya](https://github.com/hortonworks/hoya), in its initial Groovy implementation, though now as we`ve moved back to Java, they are only there in the SCM history.

Features

1. You could define the code in strings in the job configuration, have them retrieved, parsed and executed dynamically in the [mapper](https://github.com/steveloughran/grumpy/blob/master/src/main/groovy/org/apache/hadoop/grumpy/scripted/ScriptedMapper.groovy) and [reducer](https://github.com/steveloughran/grumpy/blob/master/src/main/groovy/org/apache/hadoop/grumpy/scripted/ScriptedReducer.groovy).
1. A [successor to Hadoop`s own Runnable class](https://github.com/steveloughran/grumpy/blob/master/src/main/groovy/org/apache/hadoop/grumpy/tools/RunnableTool.groovy), with a new [GrumpyToolRunner](https://github.com/steveloughran/grumpy/blob/master/src/main/groovy/org/apache/hadoop/grumpy/tools/GrumpyToolRunner.groovy). This would collaborate with the tool instance to parse the command line [and display usage messages] together, rather than force tools to replicate this work.
1. An [extended `JobConfiguration`](https://github.com/steveloughran/grumpy/blob/master/src/main/groovy/org/apache/hadoop/grumpy/GrumpyJob.groovy) which not only added various helper methods, it let you use array syntax `conf["timeout"]=45`, and map assignment.
1. An example of an MR job that goes against all the rules of statelessness, [Sliding window debouncing of input events](https://github.com/steveloughran/grumpy/blob/master/src/main/groovy/org/apache/hadoop/grumpy/projects/bluemine/mr/DebounceMap.groovy). Yes it is wrong, but it is very efficient provided the input data is in time sequence and you don`t care about the odd extra leftover event at block/map boundaries.

It is dead code and is retained for historical value alone. 

There is a [presentation from Berlin Buzzwords 2012](http://www.slideshare.net/steve_l/hadoop-gets-groovy) on the topic.

As mentioned, some of this was reused in Hoya, HBase on YARN, where we adopted the `CompileStatic` annotation to get speedup and early warnings of trouble. This worked, but had some quirks

* If we marked our AM as static it appeared to think it didn`t implement one of the YARN callback interfaces.
* One callback interface could only be implemented in Java, we had to implement a proxy class to forward to the AM

Given these problems and the fact that groovy isn't used in Hadoop-land except in the test suites of some projects, we moved the production code from Groovy to Java in 48 hours, mostly moving off lists, closures and maps, adding semicolons and replacing debug statements with SLF4J equivalents. We stick with Groovy for its tests -it has fantastic assertion reporting. Even there, `CompileStatic` is unreliable; some tests have to skip it.