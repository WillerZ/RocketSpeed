//  Copyright (c) 2014, Facebook, Inc.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.
//

#include <algorithm>
#include <map>
#include <set>
#include <string>

#include "src/util/common/statistics.h"
#include "src/util/testharness.h"
#include "src/util/testutil.h"

namespace rocketspeed {

class StatisticsTest { };

TEST(StatisticsTest, Basic) {
  Statistics stats1;
  Counter* count_a1 = stats1.AddCounter("count1");
  Counter* count_b1 = stats1.AddCounter("count2");
  Counter* count_c1 = stats1.AddCounter("count3");
  // Buckets: [0, 10)[10, 100)
  Histogram* histo_a1 = stats1.AddHistogram("histo1", 0, 100, 10, 10);
  // Buckets: [10, 11)[11, 13)[13, 17)[17, 20)
  Histogram* histo_b1 = stats1.AddHistogram("histo2", 10, 20, 1, 2);

  Statistics stats2;
  Counter* count_b2 = stats2.AddCounter("count2");
  Counter* count_a2 = stats2.AddCounter("count1");
  // Buckets: [10, 11)[11, 13)[13, 17)[17, 20)
  Histogram* histo_b2 = stats2.AddHistogram("histo2", 10, 20, 1, 2);
  // Buckets: [0, 10)[10, 100)
  Histogram* histo_a2 = stats2.AddHistogram("histo1", 0, 100, 10, 10);
  // Buckets: [0, 10)[10, 100)
  Histogram* histo_c2 = stats2.AddHistogram("histo3", 0, 100, 10, 10);

  ASSERT_EQ(count_a1->Get(), 0);

  count_a1->Add(10);
  count_b1->Add(20);
  count_c1->Add(30);
  ASSERT_EQ(count_a1->Get(), 10);
  ASSERT_EQ(count_b1->Get(), 20);
  ASSERT_EQ(count_c1->Get(), 30);

  // All in 2nd bucket.
  histo_a1->Record(50.0);
  histo_a1->Record(11.0);
  histo_a1->Record(99.0);
  ASSERT_GT(histo_a1->Percentile(0.5), 10);
  ASSERT_LT(histo_a1->Percentile(0.5), 100);

  // All in 3rd bucket.
  histo_b1->Record(14.0);
  histo_b1->Record(15.0);
  histo_b1->Record(16.0);
  ASSERT_GT(histo_b1->Percentile(0.5), 13);
  ASSERT_LT(histo_b1->Percentile(0.5), 17);

  count_a2->Add(100);
  count_b2->Add(200);
  histo_a2->Record(1.0);
  histo_a2->Record(101.0);
  histo_b2->Record(10.0);
  histo_b2->Record(20.0);
  histo_c2->Record(50.0);
  histo_c2->Record(11.0);
  histo_c2->Record(99.0);

  ASSERT_EQ(stats1.Report(),
    "count1:                                   10\n"
    "count2:                                   20\n"
    "count3:                                   30\n"
    "histo1:                                   p50: 55.0      p90: 77.5      p99: 77.5      p99.9: 77.5      (3 samples)\n"
    "histo2:                                   p50: 16.0      p90: 17.0      p99: 17.0      p99.9: 17.0      (3 samples)\n");

  // Aggregate stats1 into stats2
  stats2.Aggregate(stats1);
  ASSERT_EQ(count_a2->Get(), 110);
  ASSERT_EQ(count_b2->Get(), 220);
  ASSERT_GT(histo_a2->Percentile(0.5), 10);
  ASSERT_LT(histo_a2->Percentile(0.5), 100);
  ASSERT_GT(histo_b2->Percentile(0.5), 13);
  ASSERT_LT(histo_b2->Percentile(0.5), 17);
  ASSERT_GT(histo_c2->Percentile(0.5), 10);
  ASSERT_LT(histo_c2->Percentile(0.5), 100);

  ASSERT_EQ(stats2.Report(),
    "count1:                                   110\n"
    "count2:                                   220\n"
    "count3:                                   30\n"
    "histo1:                                   p50: 55.0      p90: 100.0     p99: 100.0     p99.9: 100.0     (5 samples)\n"
    "histo2:                                   p50: 16.0      p90: 18.0      p99: 18.0      p99.9: 18.0      (5 samples)\n"
    "histo3:                                   p50: 55.0      p90: 77.5      p99: 77.5      p99.9: 77.5      (3 samples)\n");
}

TEST(StatisticsTest, HistogramPercentiles) {
  std::vector<double> ratios = { 1.1, 1.2, 1.3, 2.0, 10.0 };
  std::vector<double> thresholds = { 0.1, 1.0, 10.0, 100.0 };

  std::random_device rd;
  std::mt19937 engine(rd());

  for (double ratio : ratios) {
    for (double threshold : thresholds) {
      // Create the histogram object.
      Histogram histogram(0.0, 1234567.0, threshold, ratio);

      // Generate some samples.
      const size_t N = 1000;
      double samples[N];
      std::uniform_real_distribution<> dis(threshold * 1.01, threshold * 1000);
      for (size_t i = 0; i < N; ++i) {
        samples[i] = dis(engine);
      }

      // Add the samples.
      for (double sample : samples) {
        histogram.Record(sample);
      }

      // Check the percentiles
      std::sort(samples, samples + N);
      for (size_t i = 1; i < N - 1; ++i) {
        double p = static_cast<double>(i) / N;
        double lower = samples[i-1] / (ratio * 1.01);
        double upper = samples[i+1] * (ratio * 1.01);
        double test = histogram.Percentile(p);
        ASSERT_GT(test, lower);
        ASSERT_LT(test, upper);
      }
    }
  }
}

TEST(StatisticsTest, HistogramPercentilesOneBucket) {
  // Test percentile sampling within a bucket
  Histogram histogram(0.0, 1000.0, 1.0, 10.0);

  // Add sample to the 100-1000 bucket.
  histogram.Record(150.0);

  // With a ratio of 10, the interface only guarantees between 100-1000.
  ASSERT_GE(histogram.Percentile(0.0), 100.0);
  ASSERT_LT(histogram.Percentile(0.0), 1000.0);
  ASSERT_GT(histogram.Percentile(0.5), 100.0);
  ASSERT_LT(histogram.Percentile(0.5), 1000.0);
  ASSERT_GT(histogram.Percentile(1.0), 100.0);
  ASSERT_LE(histogram.Percentile(1.0), 1000.0);

  // But it should also interpolate for different percentiles.
  histogram.Record(250.0);
  ASSERT_LT(histogram.Percentile(0.1), histogram.Percentile(0.9));
}

TEST(StatisticsTest, StatisticsWindowAggregator) {
  Statistics s0, s1, s2, s3;
  s1.AddCounter("a")->Add(1);
  s2.AddCounter("a")->Add(2);
  s3.AddCounter("a")->Add(3);

  s1.AddLatency("b")->Record(1.0);
  s2.AddLatency("b")->Record(2.0);
  s3.AddLatency("b")->Record(4.0);

  StatisticsWindowAggregator window(4);
  auto GetA = [&] () {
    return window.GetAggregate().GetCounterValue("a");
  };
  auto GetB = [&] () {
    return window.GetAggregate().GetHistograms().find("b")->second.get();
  };

  // Fill up window.
  window.AddSample(s1);
  ASSERT_EQ(GetA(), 1);
  window.AddSample(s2);
  ASSERT_EQ(GetA(), 1+2);
  window.AddSample(s3);
  ASSERT_EQ(GetA(), 1+2+3);
  window.AddSample(s1);
  ASSERT_EQ(GetA(), 1+2+3+1);

  // Old values should start dropping off now.
  window.AddSample(s2);
  ASSERT_EQ(GetA(), 2+3+1+2);
  window.AddSample(s3);
  ASSERT_EQ(GetA(), 3+1+2+3);
  window.AddSample(s1);
  ASSERT_EQ(GetA(), 1+2+3+1);

  // Add empty statistics and rest will clear out.
  window.AddSample(s0);
  ASSERT_EQ(GetA(), 2+3+1);
  window.AddSample(s0);
  ASSERT_EQ(GetA(), 3+1);
  window.AddSample(s0);
  ASSERT_EQ(GetA(), 1);
  window.AddSample(s0);
  ASSERT_EQ(GetA(), 0);

  // Now test histogram similarly.
  window.AddSample(s1);
  ASSERT_GE(GetB()->Percentile(0.5), 0.5);
  ASSERT_LE(GetB()->Percentile(0.5), 1.5);
  window.AddSample(s2);
  window.AddSample(s2);
  window.AddSample(s2);
  window.AddSample(s2);
  ASSERT_GE(GetB()->Percentile(0.5), 1.5);
  ASSERT_LE(GetB()->Percentile(0.5), 2.5);
  window.AddSample(s3);
  window.AddSample(s3);
  window.AddSample(s3);
  window.AddSample(s3);
  ASSERT_GE(GetB()->Percentile(0.5), 3.5);
  ASSERT_LE(GetB()->Percentile(0.5), 4.5);
}

}  // namespace rocketspeed

int main(int argc, char** argv) {
  return rocketspeed::test::RunAllTests();
}
