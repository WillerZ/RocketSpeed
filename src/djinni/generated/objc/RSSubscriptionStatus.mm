// AUTOGENERATED FILE - DO NOT MODIFY!
// This file generated by Djinni from rocketspeed.djinni

#import "RSSubscriptionStatus+Private.h"
#import "RSSequenceNumber+Private.h"
#import "RSStatus+Private.h"
#import <Foundation/Foundation.h>
#include <utility>

static_assert(__has_feature(objc_arc), "Djinni requires ARC to be enabled for this file");

@implementation RSSubscriptionStatus

- (id)initWithSubscriptionStatus:(RSSubscriptionStatus *)SubscriptionStatus
{
    if (self = [super init]) {
        _status = [[RSStatus alloc] initWithStatus:SubscriptionStatus.status];
        _seqno = [[RSSequenceNumber alloc] initWithSequenceNumber:SubscriptionStatus.seqno];
        _subscribed = SubscriptionStatus.subscribed;
    }
    return self;
}

- (id)initWithStatus:(RSStatus *)status seqno:(RSSequenceNumber *)seqno subscribed:(BOOL)subscribed
{
    if (self = [super init]) {
        _status = status;
        _seqno = seqno;
        _subscribed = subscribed;
    }
    return self;
}

- (id)initWithCppSubscriptionStatus:(const ::rocketglue::SubscriptionStatus &)SubscriptionStatus
{
    if (self = [super init]) {
        _status = [[RSStatus alloc] initWithCppStatus:SubscriptionStatus.status];
        _seqno = [[RSSequenceNumber alloc] initWithCppSequenceNumber:SubscriptionStatus.seqno];
        _subscribed = (SubscriptionStatus.subscribed) ? YES : NO;
    }
    return self;
}

- (::rocketglue::SubscriptionStatus)cppSubscriptionStatus
{
    ::rocketglue::Status status = std::move([_status cppStatus]);
    ::rocketglue::SequenceNumber seqno = std::move([_seqno cppSequenceNumber]);
    bool subscribed = _subscribed;
    return ::rocketglue::SubscriptionStatus(
            std::move(status),
            std::move(seqno),
            std::move(subscribed));
}

@end
