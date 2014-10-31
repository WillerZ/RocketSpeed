// AUTOGENERATED FILE - DO NOT MODIFY!
// This file generated by Djinni from rocketspeed.djinni

package org.rocketspeed;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 *
 * Interface to access a received message
 * Created in C++ and used by Java and ObjC
 *
 */
public abstract class MessageReceived {
    public abstract SequenceNumber GetSequenceNumber();

    public abstract Topic GetTopicName();

    public abstract byte[] GetContents();

    public static final class NativeProxy extends MessageReceived
    {
        private final long nativeRef;
        private final AtomicBoolean destroyed = new AtomicBoolean(false);

        private NativeProxy(long nativeRef)
        {
            if (nativeRef == 0) throw new RuntimeException("nativeRef is zero");
            this.nativeRef = nativeRef;
        }

        private native void nativeDestroy(long nativeRef);
        public void destroy()
        {
            boolean destroyed = this.destroyed.getAndSet(true);
            if (!destroyed) nativeDestroy(this.nativeRef);
        }
        protected void finalize() throws java.lang.Throwable
        {
            destroy();
            super.finalize();
        }

        @Override
        public SequenceNumber GetSequenceNumber()
        {
            assert !this.destroyed.get() : "trying to use a destroyed object";
            return native_GetSequenceNumber(this.nativeRef);
        }
        private native SequenceNumber native_GetSequenceNumber(long _nativeRef);

        @Override
        public Topic GetTopicName()
        {
            assert !this.destroyed.get() : "trying to use a destroyed object";
            return native_GetTopicName(this.nativeRef);
        }
        private native Topic native_GetTopicName(long _nativeRef);

        @Override
        public byte[] GetContents()
        {
            assert !this.destroyed.get() : "trying to use a destroyed object";
            return native_GetContents(this.nativeRef);
        }
        private native byte[] native_GetContents(long _nativeRef);
    }
}
