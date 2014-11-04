// AUTOGENERATED FILE - DO NOT MODIFY!
// This file generated by Djinni from rocketspeed.djinni

#import "RSSubscribeCallback.h"
#include "SubscribeCallback.hpp"
#import "DJIObjcWrapperCache+Private.h"
#import <Foundation/Foundation.h>
#include <memory>

namespace rocketglue
{
    class SubscribeCallbackObjcProxy final : public ::rocketglue::SubscribeCallback
    {
        public:
        id <RSSubscribeCallback> objcRef;
        SubscribeCallbackObjcProxy (id objcRef);
        virtual ~SubscribeCallbackObjcProxy () override;
        static std::shared_ptr<::rocketglue::SubscribeCallback> SubscribeCallback_with_objc (id objcRef);
        virtual void Call (const ::rocketglue::SubscriptionStatus & subscription_status) override;

        private:
        SubscribeCallbackObjcProxy () {};
    };
}