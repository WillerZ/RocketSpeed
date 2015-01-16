package org.rocketspeed;

import java.io.FileNotFoundException;
import java.io.IOException;

public final class Status extends StatusBase {

  public Status(StatusCode code, String state) {
    super(code, state);
  }

  public void checkExceptions() throws Exception {
    Exception e = getException();
    if (e != null) {
      throw e;
    }
  }

  public Exception getException() {
    switch (getCode()) {
      case OK:
        return null;
      case NOTFOUND:
        return new FileNotFoundException(getState());
      case NOTSUPPORTED:
        return new UnsupportedOperationException(getState());
      case INVALIDARGUMENT:
        return new IllegalArgumentException(getState());
      case IOERROR:
        return new IOException(getState());
      case NOTINITIALIZED:
        return new IllegalStateException(getState());
      case UNAUTHORIZED:
        return new IllegalArgumentException(getState());
      case TIMEDOUT:
      case INTERNAL:
        return new RuntimeException(getState());
      default:
        throw new AssertionError("Missing branch for " + StatusCode.class.getSimpleName());
    }
  }
}
