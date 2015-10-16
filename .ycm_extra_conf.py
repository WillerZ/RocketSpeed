import os

flags = [
'-xc++',
'-std=c++11',
'-Wall',
'-Werror',
'-Wshadow',
'-Wconversion',
'-Wno-sign-conversion',
'-Wnon-virtual-dtor',
'-Woverloaded-virtual',
'-I.',
'-isystem./external',
'-isystem./external/libevent/include',
'-isystem./external/djinni/support-lib/jni',
'-isystem/mnt/gvfs/third-party/53dc1fe83f84e9145b9ffb81b81aa7f6a49c87cc/gcc-4.8.1-glibc-2.17/gflags/gflags-1.6/c3f970a/include',
'-isystem/mnt/gvfs/third-party/53dc1fe83f84e9145b9ffb81b81aa7f6a49c87cc/gcc-4.8.1-glibc-2.17/libgcc/libgcc-4.8.1/8aac7fc/include',
'-isystem/mnt/gvfs/third-party/53dc1fe83f84e9145b9ffb81b81aa7f6a49c87cc/gcc-4.8.1-glibc-2.17/glibc/glibc-2.17/99df8fc/include',
'-DGFLAGS=google',
'-DOS_LINUX',
'-DROCKETSPEED_PLATFORM_POSIX',
'-DROCKETSPEED_ATOMIC_PRESENT',
'-DROCKETSPEED_FALLOCATE_PRESENT',
]


def DirectoryOfThisScript():
  return os.path.dirname( os.path.abspath( __file__ ) )


def MakeRelativePathsInFlagsAbsolute( flags, working_directory ):
  if not working_directory:
    return list( flags )
  new_flags = []
  make_next_absolute = False
  path_flags = [ '-isystem', '-I', '-iquote', '--sysroot=' ]
  for flag in flags:
    new_flag = flag

    if make_next_absolute:
      make_next_absolute = False
      if not flag.startswith( '/' ):
        new_flag = os.path.join( working_directory, flag )

    for path_flag in path_flags:
      if flag == path_flag:
        make_next_absolute = True
        break

      if flag.startswith( path_flag ):
        path = flag[ len( path_flag ): ]
        new_flag = path_flag + os.path.join( working_directory, path )
        break

    if new_flag:
      new_flags.append( new_flag )
  return new_flags


def FlagsForFile( filename, **kwargs ):
  relative_to = DirectoryOfThisScript()
  final_flags = MakeRelativePathsInFlagsAbsolute( flags, relative_to )

  return {
    'flags': final_flags,
    'do_cache': True
  }
