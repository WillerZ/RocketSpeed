// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#pragma once
#include "include/RocketSpeed.h"
#include <random>
#include <cmath>
#include <string>


namespace rocketspeed {

class RandomDistributionBase {
 public:
  virtual uint64_t generateRandomInt() = 0;
  virtual ~RandomDistributionBase() {}

 protected:
  explicit RandomDistributionBase(uint64_t seed) : rng(seed) {}
  std::mt19937_64 rng;
};
//Uniform distribution as per first version
class UniformDistribution : public RandomDistributionBase
{
 public:
  explicit UniformDistribution(uint64_t a, uint64_t b, uint64_t seed)
  : RandomDistributionBase(seed)
  , distr(a, b) {}

  virtual uint64_t generateRandomInt() {
    return distr(RandomDistributionBase::rng);
  }

 private:
  std::uniform_int_distribution<uint64_t> distr;
};
//Normal, or Gaussian, distribution
class NormalDistribution : public RandomDistributionBase
{
 public:
  explicit NormalDistribution(double mean,
                              double stddev,
                              uint64_t seed)
  : RandomDistributionBase(seed)
  , distr(mean, stddev) {}

  virtual uint64_t generateRandomInt() {
    return static_cast<uint64_t>(round(distr(RandomDistributionBase::rng)));
  }

 private:
  std::normal_distribution<double> distr;
};
//Poisson distribution
class PoissonDistribution : public RandomDistributionBase
{
 public:
  explicit PoissonDistribution(double mean, uint64_t seed)
  : RandomDistributionBase(seed)
  , distr(mean) {}

  virtual uint64_t generateRandomInt() {
    return distr(RandomDistributionBase::rng);
  }

 private:
  std::poisson_distribution<uint64_t> distr;
};

//Calculate the standard deviation of the sequence of numbers, given the mean
double StandardDeviation(uint64_t a, uint64_t b, double mean);
//get a pointer to the distribution instance based on the distribution name
RandomDistributionBase* GetDistributionByName(
                        const std::string& dist_name,
                        uint64_t a,
                        uint64_t b,
                        double amean,
                        double stdd,
                        uint64_t seed);

};
