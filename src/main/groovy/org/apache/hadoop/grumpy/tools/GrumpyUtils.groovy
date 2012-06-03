package org.apache.hadoop.grumpy.tools

import groovy.util.logging.Commons
import org.apache.commons.logging.Log
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.grumpy.ExitCodeException

/**
 * Utility methods primarily used in setting up and executing tools
 */
@Commons
class GrumpyUtils {

  static void dumpArguments(String[] args) {
    println("Arguments");
    println(convertArgsToString(args));
  }

  static void dumpConf(Configuration conf) {
    TreeSet<String> keys = sortedConfigKeys(conf);
    keys.each { key ->
      println("$key = ${conf.get(key)}")
    }
  }

  public static String convertArgsToString(String... args) {
    StringBuilder builder = new StringBuilder();
    args.each { arg ->
      builder.append(" \"").append(arg).append("\"");
    }
    builder.toString();
  }

  static long dumpDir(Log dumpLog, File dir, String pattern) {
      if (!dir.exists()) {
          dumpLog.warn("Not found: ${dir}");
          return -1;
      }
      if (!dir.isDirectory()) {
          dumpLog.warn("Not a directory: ${dir}");
          return -1;
      }
      int size = 0;
      dir.eachFile { file ->
          long l = dumpFile(dumpLog, file)
          if (file.name.startsWith(pattern)) {
              size += l
          }
      }
      size;
  }

  static long dumpFile(Log dumpLog, File file) {
      long length = file.length()
      dumpLog.info("File : ${file} of size ${length}")
      length
  }

  static String convertToUrl(File file) {
      return file.toURI().toString();
  }

  static def deleteDirectoryTree(File dir) {
      if (dir.exists()) {
          if (dir.isDirectory()) {
              log.info("Cleaning up $dir")
              //delete the children
              dir.eachFile { file ->
                org.apache.hadoop.grumpy.tools.GrumpyToolRunner.log.info("deleting $file")
                file.delete()
              }
              dir.delete()
          } else {
              throw new IOException("Not a directory: ${dir}")
          }
      } else {
          //not found, do nothing
        GrumpyUtils.log.debug("No output dir yet")
      }
  }

  static String joinList(List elements, String separator) {
      int size = elements.size();
      if (size == 0) return "";
      StringBuilder builder = new StringBuilder()
      elements.eachWithIndex {  elt, index ->
          builder.append(elt.toString());
          if (index < (size - 1)) {
              builder.append(separator)
          }
      }
      return builder.toString()
  }

  public static String findContainingJar(Class my_class) throws IOException {
      ClassLoader loader = my_class.classLoader;
      if (loader == null) {
          throw new IOException("Class $my_class does not have a classloader!")
      }
      assert loader != null
      assert my_class != null
      String class_file = my_class.name.replaceAll("\\.", "/") + ".class";
      Enumeration<URL> urlEnumeration = loader.getResources(class_file)
      assert urlEnumeration != null

      for (Enumeration itr = urlEnumeration;
      itr.hasMoreElements();) {
          URL url = (URL) itr.nextElement();
          if ("jar".equals(url.protocol)) {
              String toReturn = url.path;
              if (toReturn.startsWith("file:")) {
                  toReturn = toReturn.substring("file:".length());
              }
              // URLDecoder is a misnamed class, since it actually decodes
              // x-www-form-urlencoded MIME type rather than actual
              // URL encoding (which the file path has). Therefore it would
              // decode +s to ' 's which is incorrect (spaces are actually
              // either unencoded or encoded as "%20"). Replace +s first, so
              // that they are kept sacred during the decoding process.
              toReturn = toReturn.replaceAll("\\+", "%2B");
              toReturn = URLDecoder.decode(toReturn, "UTF-8");
              return toReturn.replaceAll("!.*\$", "");
          }
      }
      return null;
  }

  public static void checkPort(String hostname, int port, int connectTimeout) {
      InetSocketAddress addr = new InetSocketAddress(hostname, port);
  }

  public static void checkPort(String name, InetSocketAddress address, int connectTimeout) {
      Socket socket = null;
      try {
          socket = new Socket();
          socket.connect(address, connectTimeout);
      } catch (Exception e) {
          throw new IOException("Failed to connect to $name at $address"
                  + " after $connectTimeout millisconds"
                  + ": " + e).initCause(e);
      } finally {
          try {
              socket?.close()
          } catch (IOException ignored) {
          }
      }
  }

  public static void checkURL(String name, String url, int timeout) {
      InetSocketAddress address = NetUtils.createSocketAddr(url)
      checkPort(name, address, timeout)
  }

  public static TreeSet<String> sortedConfigKeys(Configuration conf) {
    TreeSet<String> sorted = new TreeSet<String>();
    conf.each { entry ->
      sorted.add(entry.key, entry.value)
    }
    sorted;
  }

  /**
   * A required file
   * @param role role of the file (for errors)
   * @param filename the filename
   * @throws ExitCodeException if the file is missing
   * @return the file
   */
  public static File requiredFile(String filename, String role) {
    if (!filename) {
      throw new ExitCodeException("$role file not defined");
    }
    File file = new File(filename)
    if (!file.exists()) {
      throw new ExitCodeException("$role file not found: \"${file.canonicalPath}\"");
    }
    file
  }

  protected static File requiredDir(String name) {
    File dir = requiredFile(name, "")
    if (!dir.directory) {
      throw new ExitCodeException("Not a directory: " + dir.canonicalPath)
    }
    dir
  }

  protected static File maybeCreateDir(String name) {
    File dir = new File(name)
    if (!dir.exists()) {
      //this is what we want
      if (!dir.mkdirs()) {
        throw new ExitCodeException("Failed to create directory " + dir.canonicalPath)
      }
    } else {
      if (!dir.directory) {
        throw new ExitCodeException("Not a directory: " + dir.canonicalPath)
      }
    }
    dir
  }
}
