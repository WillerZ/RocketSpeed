package org.rocketspeed;

import java.io.FileNotFoundException;
import java.io.IOException;

public final class Status extends StatusBase {

  public Status(StatusCode code, String state) {
    super(code, state);
  }

  public void checkExceptions() throws IOException, IllegalArgumentException,
                                       IllegalStateException {
    switch (getCode()) {
      case INTERNAL:
        throw new IllegalStateException(getState());
      case INVALIDARGUMENT:
        throw new IllegalArgumentException(getState());
      case IOERROR:
        throw new IOException(getState());
      case NOTFOUND:
        throw new FileNotFoundException(getState());
      default:
        throw new RuntimeException();
    }
  }
}
