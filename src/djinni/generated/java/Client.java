// AUTOGENERATED FILE - DO NOT MODIFY!
// This file generated by Djinni from rocketspeed.djinni

package org.rocketspeed;

import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The RocketSpeed Client object.
 * Implemented in c++ and called from Java and ObjC
 */
public abstract class Client {
    public abstract void Open(Configuration config, PublishCallback publishCallback, SubscribeCallback subscribeCallback, MessageReceivedCallback receiveCallback);

    public abstract void Publish(Topic topicName, NamespaceID namespaceId, TopicOptions options, byte[] data, MsgId msgid);

    public abstract void ListenTopics(ArrayList<SubscriptionPair> names, TopicOptions options);

    public static final class NativeProxy extends Client
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
        public void Open(Configuration config, PublishCallback publishCallback, SubscribeCallback subscribeCallback, MessageReceivedCallback receiveCallback)
        {
            assert !this.destroyed.get() : "trying to use a destroyed object";
            native_Open(this.nativeRef, config, publishCallback, subscribeCallback, receiveCallback);
        }
        private native void native_Open(long _nativeRef, Configuration config, PublishCallback publishCallback, SubscribeCallback subscribeCallback, MessageReceivedCallback receiveCallback);

        @Override
        public void Publish(Topic topicName, NamespaceID namespaceId, TopicOptions options, byte[] data, MsgId msgid)
        {
            assert !this.destroyed.get() : "trying to use a destroyed object";
            native_Publish(this.nativeRef, topicName, namespaceId, options, data, msgid);
        }
        private native void native_Publish(long _nativeRef, Topic topicName, NamespaceID namespaceId, TopicOptions options, byte[] data, MsgId msgid);

        @Override
        public void ListenTopics(ArrayList<SubscriptionPair> names, TopicOptions options)
        {
            assert !this.destroyed.get() : "trying to use a destroyed object";
            native_ListenTopics(this.nativeRef, names, options);
        }
        private native void native_ListenTopics(long _nativeRef, ArrayList<SubscriptionPair> names, TopicOptions options);
    }
}
