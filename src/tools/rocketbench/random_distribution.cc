// Copyright (c) 2014, Facebook, Inc.  All rights reserved.
// This source code is licensed under the BSD-style license found in the
// LICENSE file in the root directory of this source tree. An additional grant
// of patent rights can be found in the PATENTS file in the same directory.
//

#include "src/tools/rocketbench/random_distribution.h"

namespace rocketspeed {


//Calculate the standard deviation of the sequence of numbers, given the mean
double StandardDeviation(uint64_t a, uint64_t b, double mean) {

    double sumsqrs(0.0);
    //substract the mean from each value in the range [a,b],
    //square the results and add them
    for (uint64_t i = a; i <= b; i++)
    {
        sumsqrs += pow(static_cast<double>(i) - mean, 2.0);
    }
    //finally return the sqrt of the sum of squares
    //divided by the total number of numbers
    return sqrt(sumsqrs / static_cast<double>(b-a+1));

}
//get a pointer to the distribution instance based on the distribution name
// Note that if mean and/or stddev are set to 0,
//these will be generated assuming a consecutive
//range of numbers in the range [a,b]
RandomDistributionBase* GetDistributionByName(
                        const std::string& dist_name, uint64_t a,
                        uint64_t b, double amean, double stdd) {

    RandomDistributionBase* pDistribution = nullptr;
    //mean: if not provided, this is close enough as the range
    //of number [a,b] is in ascending sequential and consecutive order
    double mean = (amean > 0.0 && amean > a && amean < b) ?
                   amean : static_cast<double>(a+b) / 2;

    if (dist_name.compare("normal") == 0) {
        double stddev = (stdd > 0.0) ? stdd : StandardDeviation(a, b, mean);
        pDistribution = new NormalDistribution(mean, stddev);
    }
    else if (dist_name.compare("poisson") == 0) {
        pDistribution = new PoissonDistribution(mean);
    }
    else if (dist_name.compare("uniform") == 0) {
        pDistribution = new UniformDistribution(a, b);
    } else if (dist_name.compare("fixed") == 0) {
        pDistribution = nullptr;
    }
    return pDistribution;
}
}
