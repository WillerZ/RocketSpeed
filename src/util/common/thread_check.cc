#include "src/util/common/thread_check.h"

#if defined(OS_MACOSX)
#include <mach/mach_init.h>
#endif

namespace rocketspeed {
uint64_t ThreadCheck::GetCurrentThreadId() {
#if defined(OS_MACOSX)
  return static_cast<uint64_t>(mach_thread_self());
#else
  return static_cast<uint64_t>(pthread_self());
#endif /* OS_MACOSX */
}
}  // namespace rocketspeed
