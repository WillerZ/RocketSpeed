// AUTOGENERATED FILE - DO NOT MODIFY!
// This file generated by Djinni from rocketspeed.djinni

#import "RSStatusCodeTranslator+Private.h"
#import <Foundation/Foundation.h>

static_assert(__has_feature(objc_arc), "Djinni requires ARC to be enabled for this file");

@implementation RSStatusCodeTranslator

+ (RSStatusCode)cppStatusCodeToObjcStatusCode:(::rocketglue::StatusCode)StatusCode
{
    return static_cast<RSStatusCode>(StatusCode);
}

+ (::rocketglue::StatusCode)objcStatusCodeToCppStatusCode:(RSStatusCode)StatusCode
{
    return static_cast<enum ::rocketglue::StatusCode>(StatusCode);
}

@end
