// AUTOGENERATED FILE - DO NOT MODIFY!
// This file generated by Djinni from rocketspeed.djinni

#import "RSTopicOptions.h"
#include "TopicOptions.hpp"
#import <Foundation/Foundation.h>

@interface RSTopicOptions ()

- (id)initWithTopicOptions:(RSTopicOptions *)TopicOptions;
- (id)initWithRetention:(RSRetention)retention;
- (id)initWithCppTopicOptions:(const ::rocketglue::TopicOptions &)TopicOptions;
- (::rocketglue::TopicOptions)cppTopicOptions;

@end
