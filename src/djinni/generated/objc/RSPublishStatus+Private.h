// AUTOGENERATED FILE - DO NOT MODIFY!
// This file generated by Djinni from rocketspeed.djinni

#import "RSPublishStatus.h"
#include "PublishStatus.hpp"
#import <Foundation/Foundation.h>

@interface RSPublishStatus ()

- (id)initWithPublishStatus:(RSPublishStatus *)PublishStatus;
- (id)initWithStatus:(RSStatus *)status msgid:(RSMsgId *)msgid;
- (id)initWithCppPublishStatus:(const ::rocketglue::PublishStatus &)PublishStatus;
- (::rocketglue::PublishStatus)cppPublishStatus;

@end
