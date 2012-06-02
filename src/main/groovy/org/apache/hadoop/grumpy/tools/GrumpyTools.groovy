package org.apache.hadoop.grumpy.tools

import groovy.util.logging.Commons
import org.apache.commons.logging.Log
import org.apache.hadoop.net.NetUtils

@Commons
class GrumpyTools {

    /**
     * Dump a dir to the log, and add up the total size of all files matching the pattern
     * @param dumpLog
     * @param dir
     * @param pattern to look for when counting
     * @return number of files if
     */
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

    /**
     * Clean up the output directory
     * @param dir
     * @return
     */
    static def deleteDirectoryTree(File dir) {
        if (dir.exists()) {
            if (dir.isDirectory()) {
                log.info("Cleaning up " + dir)
                //delete the children
                dir.eachFile { file ->
                    log.info("deleting " + file)
                    file.delete()
                }
                dir.delete()
            } else {
                throw new IOException("Not a directory: ${dir}")
            }
        } else {
            //not found, do nothing
            log.debug("No output dir yet")
        }
    }

    /**
     * Join a list of anything together into a string, using the defined separator (Which is not appended to the last element)
     * @param elements list elements
     * @param separator string separator
     * @return a combined list
     */
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

    /**
     * Find a jar that contains a class of the same name, if any.
     * It will return a jar file, even if that is not the first thing
     * on the class path that has a class with the same name.
     *
     *
     * This is from Hadoop 0.24 and contains the fix for incomplete uuencode plus some 
     * extra error handling
     * @param my_class the class to find.
     * @return a jar file that contains the class, or null.
     * @throws IOException
     */
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
    
    /**
     * Probe a port for being open
     *
     * @param hostname hostname to check
     * @param port port to probe
     * @param connectTimeout timeout in milliseconds
     * @throws IOException failure to connect, including timeout
     */
    public static void checkPort(String hostname, int port, int connectTimeout) {
        InetSocketAddress addr = new InetSocketAddress(hostname, port);
    }
    
    /**
     * Probe a port for being open
     *
     * @param address        address to check
     * @param connectTimeout timeout in milliseconds
     * @throws IOException failure to connect, including timeout
     */
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
        GrumpyTools.checkPort(name, address, timeout)
    }
}
