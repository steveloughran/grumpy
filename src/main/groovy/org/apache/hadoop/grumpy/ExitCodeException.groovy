package org.apache.hadoop.grumpy

class ExitCodeException extends Exception {

  int exitCode = -1;

  ExitCodeException(final String message) {
    super(message)
  }

  ExitCodeException(final int exitCode, final String message) {
    super(message)
    this.exitCode = exitCode
  }


}
