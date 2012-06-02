/* (C) Copyright 2010 Hewlett-Packard Development Company, LP

This library is free software; you can redistribute it and/or
modify it under the terms of the GNU Lesser General Public
License as published by the Free Software Foundation; either
version 2.1 of the License, or (at your option) any later version.

This library is distributed in the hope that it will be useful,
but WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
Lesser General Public License for more details.

You should have received a copy of the GNU Lesser General Public
License along with this library; if not, write to the Free Software
Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA

For more information: www.smartfrog.org

*/

package org.apache.hadoop.grumpy

import groovy.util.logging.Commons
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.compress.GzipCodec
import org.apache.hadoop.mapred.SequenceFileOutputFormat
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat
import org.apache.hadoop.grumpy.tools.GrumpyTools

/**
 * This class 
 */
@Commons
class GrumpyJob extends Job {

    GrumpyJob(String jobName) {
        super(new Configuration(), jobName)
    }

    GrumpyJob(Configuration conf) {
        super(conf)
    }

    GrumpyJob(Configuration conf, String jobName) {
        super(conf, jobName)
    }

    void setupOutput(String outputURL) {
        log.info("Output directory is ${outputURL}")
        FileOutputFormat.setOutputPath(this, new Path(outputURL));
    }

    void addInput(String inputURL) {
        log.info("Input Path is ${inputURL}")
        FileInputFormat.addInputPath(this, new Path(inputURL));
    }


    void setupOutput(File output) {
        String outputURL = GrumpyTools.convertToUrl(output)
        setupOutput(outputURL)
    }

    void addInput(File input) {
        String inputURL = GrumpyTools.convertToUrl(input)
        addInput(inputURL)
    }

    void addJarList(List jarlist) {
        String listAsString = GrumpyTools.joinList(jarlist, ",")
        configuration.set(Keys.JOB_KEY_JARS, listAsString)
    }

    void addNoJars() {
        this['tmpjars']=""
    }

    /**
     * Add the groovy jar. if this is groovy-all, you get everything.
     */
    String addGroovyJar() {
        return addJar(GString.class)
    }

    String addJar(Class jarClass) {
        String file = GrumpyTools.findContainingJar(jarClass)
        if (!file) {
            throw new FileNotFoundException("No JAR containing class \"${jarClass}\"")
        }
        log.info("Jar containing class ${jarClass} is ${file}")
        addJar("file://" + file)
        file
    }

    void addJar(String jarFile) {
        append(Keys.JOB_KEY_JARS, jarFile)
    }

    public void append(String key, String value) {
        String attrlist = configuration.get(key, null)
        if (!attrlist) {
            attrlist = value;
        } else {
            attrlist += "," + value;
        }
        configuration.set(key, attrlist)
        attrlist
    }

    /**
     * if you want compressed output, ask for gzip
     * as it is the one that is there
     */
    def compressOutputToGzip() {
        FileOutputFormat.setCompressOutput(this, true)
        FileOutputFormat.setOutputCompressorClass(this, GzipCodec)
    }

    /**
     * Make the output sequenced block format
     */
    def outputSequencedBlocks() {
        conf.setOutputFormat(SequenceFileOutputFormat)
        SequenceFileOutputFormat.setOutputCompressionType(conf, SequenceFile.CompressionType.BLOCK)
    }

    
    String get(String name) {
        conf.get(name)
    }

    void set(String name, Object value) {
        conf.set(name, value.toString())
    }

    @Override
    def getAt(String name) {
        conf.get(name) 
    }

    @Override
    void putAt(String name, Object value) {
        set(name, value)
    }
    
    /**
     * Relay a property set to the configuration
     * @param name property name
     * @param value new value
     */
/*    def propertyMissing(String name, Object value) { set(name, value) }*/

    /**
     * Relay a property query to the configuration
     * @param name
     * @return the value or null
     */
/*
    String propertyMissing(String name) { get(name) }
*/

    /**
     * Apply a map of option pairs to the settings
     * @param options options map, can be null
     */
    void applyOptions(Map options) {
        log.debug("Setting options $options")
        if (options != null) {
            options.each { elt ->
                String key = elt.key.toString()
                String val = elt.value.toString()
                log.debug("settings $key=$val")
                set(key, val)
            }
        }
    }

    /**
     * Try to kill a job
     * @return true iff the operation was successful
     */
    boolean kill() {
        try {
            killJob()
            return true;
        } catch (Exception e) {
            log.debug("Failed to kill job: " + e, e)
            return false
        }
    }
}
