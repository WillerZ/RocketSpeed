// AUTOGENERATED FILE - DO NOT MODIFY!
// This file generated by Djinni from rocketspeed.djinni

package org.rocketspeed;

public final class HostId {


    /*package*/ final String hostname;

    /*package*/ final int port;

    public HostId(
            String hostname,
            int port) {
        this.hostname = hostname;
        this.port = port;
    }

    public String getHostname() {
        return hostname;
    }

    public int getPort() {
        return port;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof HostId)) {
            return false;
        }
        HostId other = (HostId) obj;
        return this.hostname.equals(other.hostname) &&
                this.port == other.port;
    }
}
