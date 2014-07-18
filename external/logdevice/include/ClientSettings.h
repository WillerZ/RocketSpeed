/* -*- Mode: C++; tab-width: 2; c-basic-offset: 2; indent-tabs-mode: nil -*- */
#pragma once

/**
 * @file Allows applications to configure the LogDevice client, before and
 * after constructing the Client instance.
 */

namespace facebook { namespace logdevice {

class ClientSettingsImpl;       // private implementation

class ClientSettings {
 public:
  /**
   * Creates an instance with default settings.
   */
  static ClientSettings* create();

  virtual ~ClientSettings() {}

  /**
   * Changes a setting.
   *
   * @param name   setting name, as would be provided on the server command line
   *               (see Settings::addOptions() in logdevice/common/Settings.cpp)
   * @param value  string representation of value, as would be provided on the
   *               server command line
   *
   * @returns On success, returns 0. On failure, returns -1 and sets
   *          logdevice::err to:
   *
   *            UNKNOWN_SETTING        name parameter was not recognized
   *            INVALID_SETTING_VALUE  value was invalid (e.g. not numeric for a
   *                                   numeric setting)
   *            INVALID_PARAM          any other error
   */
  int set(const char *name, const char *value);

  // Overload for settings with integral settings, for convenience
  int set(const char *name, int64_t value);

 private:
  ClientSettings() { }

  friend class ClientSettingsImpl;
  ClientSettingsImpl* impl();   // downcasts (this)
};

}}
