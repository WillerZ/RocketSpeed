//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//
#define __STDC_FORMAT_MACROS

#include <atomic>
#include <map>
#include <memory>
#include <string>
#include <unordered_set>
#include <vector>

#include "include/Env.h"
#include "include/Logger.h"
#include "src/messages/event_loop.h"
#include "src/messages/scheduled_executor.h"
#include "src/messages/stream.h"
#include "src/port/port.h"
#include "src/util/random.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class EventLoopTest : public ::testing::Test {
 public:
  EventLoopTest()
  : positive_timeout(1000), negative_timeout(100), env(Env::Default()) {
    EXPECT_OK(test::CreateLogger(env, "EventLoopTest", &info_log));
    options.info_log = info_log;
  }

  const std::chrono::milliseconds positive_timeout;
  const std::chrono::milliseconds negative_timeout;
  Env* const env;
  std::shared_ptr<Logger> info_log;
  EventLoop::Options options;
  StreamAllocator stream_allocator;

  void Run(std::function<void()> callback, EventLoop* loop) {
    std::unique_ptr<Command> command(MakeExecuteCommand(std::move(callback)));
    ASSERT_OK(loop->SendCommand(command));
  }

  void Wait(std::function<void()> callback, EventLoop* loop) {
    port::Semaphore done;
    Run([&]() {
      callback();
      done.Post();
    }, loop);
    ASSERT_TRUE(done.TimedWait(positive_timeout));
  }
};

TEST_F(EventLoopTest, AddTask) {
  EventLoop loop(options, std::move(stream_allocator));
  EventLoop::Runner runner(&loop);

  port::Semaphore task_sem;
  // Will only be modified from a thread that used the thread check below.
  // All reads from other threads are properly guarded with a semaphore.
  bool done = false;
  ThreadCheck thread_check;
  Run([&]() {
    thread_check.Check();
    loop.AddTask([&]() {
      thread_check.Check();
      done = true;
      task_sem.Post();
    });
    // The task cannot be invoked inline.
    ASSERT_TRUE(!done);
  }, &loop);
  ASSERT_TRUE(task_sem.TimedWait(positive_timeout));
  ASSERT_TRUE(done);
}

TEST_F(EventLoopTest, TriggerableEvent) {
  EventLoop loop(options, std::move(stream_allocator));
  EventLoop::Runner runner(&loop);

  // Create a trigger for events, this call is thread-safe, so that other
  // threads can create triggers.
  EventTrigger trigger = loop.CreateEventTrigger();

  port::Semaphore task_sem;
  const int kExpected = 2;
  // Will only be modified from a thread that used the thread check below.
  // All reads from other threads are properly guarded with a semaphore.
  int done = 0;
  std::vector<std::unique_ptr<EventCallback>> events;
  ThreadCheck thread_check;
  Wait([&]() {
    thread_check.Check();

    // The code to be executed in the callback.
    const auto cb = [&] {
      thread_check.Check();
      RS_ASSERT(done < kExpected);
      ++done;
      if (done == kExpected) {
        loop.Unnotify(trigger);
      }
      task_sem.Post();
    };
    // Create two events that will be invoked.
    events.emplace_back(loop.CreateEventCallback(cb, trigger));
    events.back()->Enable();
    events.emplace_back(loop.CreateEventCallback(cb, trigger));
    events.back()->Enable();
    // One event that is disabled.
    events.emplace_back(
        loop.CreateEventCallback([]() { std::abort(); }, trigger));
    // And one event that will be destroyed before being invoked.
    auto event = loop.CreateEventCallback([]() { std::abort(); }, trigger);
    event->Enable();

    // Notify all registered events.
    loop.Notify(trigger);
    // The callbacks cannot be invoked inline.
    ASSERT_EQ(0, done);
  }, &loop);
  ASSERT_TRUE(task_sem.TimedWait(positive_timeout));
  ASSERT_TRUE(task_sem.TimedWait(positive_timeout));
  ASSERT_EQ(kExpected, done);

  port::Semaphore destroyed;
  std::unique_ptr<Command> command1(MakeExecuteCommand([&]() {
    // EventCallbacks must be destroyed on the EventLoop thread.
    events.clear();
    destroyed.Post();
  }));
  ASSERT_OK(loop.SendCommand(command1));
  ASSERT_TRUE(destroyed.TimedWait(positive_timeout));
}

template <typename T>
class TestSink : public Sink<T> {
 public:
  explicit TestSink(int initial_capacity)
  : write_ready_fd_(true, true), capacity_(initial_capacity) {
    EXPECT_EQ(0, write_ready_fd_.status());
  }

  ~TestSink() { write_ready_fd_.closefd(); }

  bool Write(T& value) override {
    --capacity_;
    return FlushPending();
  };

  bool FlushPending() override {
    return FlushPendingImpl(true);
  }

  bool FlushPendingImpl(bool check_thread) {
    if (check_thread) {
      write_thread_check_.Check();
    }
    if (capacity_.load() > 0) {
      write_ready_fd_.write_event(1);
      return true;
    } else {
      eventfd_t value;
      write_ready_fd_.read_event(&value);
      return false;
    }
  };

  std::unique_ptr<EventCallback> CreateWriteCallback(
      EventLoop* event_loop, std::function<void()> callback) override {
    return EventCallback::CreateFdReadCallback(
        event_loop, write_ready_fd_.readfd(), std::move(callback));
  };

  void DrainOne() {
    ++capacity_;
    FlushPendingImpl(false);
  }

 private:
  ThreadCheck write_thread_check_;
  port::Eventfd write_ready_fd_;
  std::atomic<int> capacity_;
};

TEST_F(EventLoopTest, StreamsFlowControl) {
// This test is disabled on OSX.
// On OSX we cannot control size of TCP send and receive buffers and they are
// rather large. Consequently, we would have to write plenty of data to the
// socket in order for TCP flow control to kick in and block the writer. Writing
// large amount of data to the socket is undesirable, as it would make the test
// flaky.
#ifndef OS_MACOSX
  MessagePing ping(Tenant::GuestTenant, MessagePing::PingType::Response);
  // Create the sink that will allow us to block the stream.
  TestSink<int> test_sink(0);

  // We pipe all messages received on a stream to the test sink.
  port::Semaphore delivered;
  options.heartbeat_period = std::chrono::milliseconds(0); // disable
  options.event_callback =
      [&](Flow* flow, std::unique_ptr<Message> msg, StreamID stream) {
        int value;
        flow->Write(&test_sink, value);
        delivered.Post();
      };
  options.listener_port = 0;     // listen on auto-allocated port
  options.send_queue_limit = 4;  // small limit on send queue size
  // Set very small TCP buffer sizes.
  options.env_options.tcp_send_buffer_size = 2048;
  options.env_options.tcp_recv_buffer_size = 256;
  EventLoop loop(options, std::move(stream_allocator));
  EventLoop::Runner runner(&loop);

  // An event that signals writeability of the stream.
  port::Semaphore writable;
  std::unique_ptr<EventCallback> write_ev;
  // Create a stream to itself.
  std::unique_ptr<Stream> stream;
  Wait([&] {
    stream = loop.OpenStream(loop.GetHostId());
    write_ev = stream->CreateWriteCallback(&loop,
                                           [&]() {
                                             write_ev->Disable();
                                             writable.Post();
                                           });
    write_ev->Enable();
  }, &loop);

  // Write a few messages.
  Wait([&] {
    // Initially, the stream is writable.
    ASSERT_TRUE(writable.TimedWait(positive_timeout));
    // The send queue can fit a single message only.
    std::unique_ptr<Message> p = Message::Copy(ping);
    ASSERT_TRUE(stream->Write(p));
    p = Message::Copy(ping);
    ASSERT_TRUE(!stream->Write(p));
    p = Message::Copy(ping);
    ASSERT_TRUE(!stream->Write(p));
  }, &loop);

  // Test that flow control on delivery path works.
  ASSERT_TRUE(delivered.TimedWait(positive_timeout));
  ASSERT_TRUE(!delivered.TimedWait(negative_timeout));
  test_sink.DrainOne();
  test_sink.DrainOne();
  ASSERT_TRUE(delivered.TimedWait(positive_timeout));
  ASSERT_TRUE(!delivered.TimedWait(negative_timeout));
  test_sink.DrainOne();
  ASSERT_TRUE(delivered.TimedWait(positive_timeout));
  ASSERT_TRUE(!delivered.TimedWait(negative_timeout));

  const int kManyMessages = 5000;
  // Now fill up the socket buffers and send queue.
  Wait([&]() {
    for (int i = 0; i < kManyMessages; ++i) {
      std::unique_ptr<Message> p = Message::Copy(ping);
      stream->Write(p);
    }
    // Enable the write-enabled event so we get notification when the stream is
    // writable.
    write_ev->Enable();
  }, &loop);

  // We've enabled the write-enabled event, but no notification should happen,
  // as the stream is not writable until all messages are sent out, which should
  // take some time.
  ASSERT_TRUE(!writable.TimedWait(negative_timeout));
  // Now we take all the messages.
  for (int i = 0; i < kManyMessages; ++i) {
    test_sink.DrainOne();
  }
  // We should receive a notification that the stream is writable again.
  ASSERT_TRUE(writable.TimedWait(10 * positive_timeout));

  // Streams must be closed on the EventLoop thread.
  Wait([&]() {
    write_ev.reset();
    stream.reset();
  }, &loop);
#endif  // OS_MACOSX
}

TEST_F(EventLoopTest, ExceptionCircuitBreaker) {
  // Tests that throwing an exception within the EventLoop thread does not
  // crash the process. We should be able to still use the EventLoop, but it
  // will be in a bad state and not respond or recover.
  EventLoop loop(options, std::move(stream_allocator));
  EventLoop::Runner runner(&loop);
  ASSERT_EVENTUALLY_TRUE(loop.IsRunning());

  // Throw an exception.
  Run([]() { throw std::runtime_error("test"); }, &loop);

  // Loop should stop.
  ASSERT_EVENTUALLY_TRUE(!loop.IsRunning());

  // Should be able to call functions on the loop.
  // We test a few here. We don't expect them to return anything valid.
  (void)loop.CreateEventTrigger();
  MessagePing ping(Tenant::GuestTenant, MessagePing::PingType::Response);
  (void)loop.SendResponse(ping, 1234);
  (void)loop.IsOutboundStream(1234);
}

TEST_F(EventLoopTest, ScheduledExecutorTest) {
  // Test for Scheduled Executor which schedules some events and checks
  //    - expected time taken based on max_timeout
  //    - expected order of events based on timeout of each event
  EventLoop loop(options, std::move(stream_allocator));
  EventLoop::Runner runner(&loop);
  ASSERT_EVENTUALLY_TRUE(loop.IsRunning());

  const auto tick_time = std::chrono::milliseconds(5);
  const size_t kNumEvents = 1000;
  const size_t max_timeout = 900;  // Less than positive_timeout

  // Initialize the scheduler
  std::unique_ptr<ScheduledExecutor> scheduler;
  Wait([&]() { scheduler.reset(new ScheduledExecutor(&loop, tick_time)); },
       &loop);

  Random random(static_cast<uint32_t>(time(nullptr)));
  port::Semaphore all_done;
  std::multimap<size_t, size_t> event_timeouts;
  std::vector<size_t> execution_order;

  // Schedule events
  uint64_t start = env->NowMicros();
  for (size_t i = 0; i < kNumEvents; i++) {
    Run([&, event = i ]() {
      auto cb = [&, event]() {
        execution_order.emplace_back(event);
        if (execution_order.size() == kNumEvents) {
          all_done.Post();
        }
      };
      // Schedule one event with maximum timeout to get more precise
      // run time of the test
      auto timeout =
          (event < kNumEvents - 1) ? random.Uniform(max_timeout) : max_timeout;
      event_timeouts.insert({timeout, event});
      scheduler->Schedule(std::move(cb), std::chrono::milliseconds(timeout));
    },
        &loop);
  }

  // Check expected time
  ASSERT_TRUE(all_done.TimedWait(positive_timeout));
  uint64_t taken = env->NowMicros() - start;
  ASSERT_GE(taken / 1000, max_timeout);

  // Check expected order of events
  ASSERT_EQ(kNumEvents, event_timeouts.size());
  ASSERT_EQ(kNumEvents, execution_order.size());

  auto event_timeout_it = event_timeouts.begin();
  auto execution_order_it = execution_order.begin();
  while (event_timeout_it != event_timeouts.end()) {
    ASSERT_EQ(event_timeout_it->second, *execution_order_it);
    ++event_timeout_it;
    ++execution_order_it;
  }
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests(argc, argv);
}
