// AUTOGENERATED FILE - DO NOT MODIFY!
// This file generated by Djinni from rocketspeed.djinni

#import <Foundation/Foundation.h>
@class RSNamespaceID;
@class RSSequenceNumber;
@class RSTopic;

/**
 *
 * Indicates whether to subscribe or unsubscribe
 * from the specified topic.
 * Messages after the specified sequence number will be 
 * delivered to the client.
 * A seqno of 0 indicates that the subscriber is interested in
 * receiving whatever data is available via this topic.
 * A seqno of 1 indicates that the subscriber is interested in
 * receiving all the data that was published to this topic.
 *
 */

@interface RSSubscriptionPair : NSObject

@property (nonatomic, readonly) RSSequenceNumber *seqno;

@property (nonatomic, readonly) RSTopic *topicName;

@property (nonatomic, readonly) RSNamespaceID *namespaceId;

@end
