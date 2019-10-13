// Copyright 2019 Mikhail Kazhamiaka
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//        http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#pragma once

#include <cinttypes>
#include <vector>
#include <math.h>

// this is based off of http://www.csee.usf.edu/~kchriste/tools/genzipf.c
// this class caches the entire distribution, so instantiate it only once,
// that is why everything here is a constant method.
class ZipfDistributionCDF {
 public:
  ZipfDistributionCDF(int64_t min, int64_t max, double alpha)
          : min_(min), max_(max), alpha_(alpha), translation_(min), numElems_(0), cummulativeDist_() {
      numElems_ = (max_ - min_) + 1;
      cummulativeDist_.reserve(numElems_);
  }

  void init() {
      double runningSum = 0.0;

      for (int64_t pos = 0; pos < numElems_; pos++) {
          double prob = invertProb(pos);
          runningSum += prob;
          cummulativeDist_.push_back(runningSum);
      }

      for (int64_t pos = 0; pos < numElems_; pos++) {
          double cumProb = cummulativeDist_.at(pos);
          double prob = cumProb / runningSum;
          cummulativeDist_.at(pos) = prob;
      }
  }

  std::vector<int64_t> getRange() const {
      return std::vector<int64_t>({min_, max_});
  }

  double getAlpha() const {
      return alpha_;
  }

  int64_t getValue(double p) const {
      int64_t pos = binarySearch(p);
      int64_t v = pos + translation_;

      return v;
  }

 private:
  double invertProb(int64_t pos) const {
      // don't divide by 0
      // note that if alpha_ = 0, then this is a uniform distribution. The higher
      // the alpha_ the greater the skew
      double p = pow((double)(pos + 1), alpha_);
      double invP = (1.0 / p);
      return invP;
  }

  int64_t binarySearch(double needle) const {
      int64_t minPos = 0;
      int64_t maxPos = numElems_ - 1;
      int64_t medPos = 0 /*(minPos + maxPos) / 2*/;
      double elem = 0;

      while (minPos <= maxPos) {
          medPos = (minPos + maxPos) / 2;

          elem = cummulativeDist_.at(medPos);
          // found
          if (elem == needle) {
              // breakout
              break;
          } else if (elem < needle) {
              minPos = medPos + 1;
          } else {  // >
              if (minPos == maxPos) {
                  // breakout
                  break;
              }
              maxPos = medPos - 1;
          }
      }
      if (elem < needle) {
          int64_t oldPos = medPos;
          medPos = std::min(oldPos + 1, numElems_ - 1);
      }
      return medPos;
  }

  int64_t min_;
  int64_t max_;
  double alpha_;
  int64_t translation_;
  int64_t numElems_;

  std::vector<double> cummulativeDist_;  // this fills everything in
};
