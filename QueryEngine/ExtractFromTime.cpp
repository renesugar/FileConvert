/*
 * Copyright 2017 MapD Technologies, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "ExtractFromTime.h"

#ifndef __CUDACC__
#include <glog/logging.h>
#endif

extern "C" __attribute__((noinline))
#ifdef __CUDACC__
__device__
#endif
    int
    extract_hour(const time_t* tim_p) {
  long days, rem;
  const time_t lcltime = *tim_p;
  days = ((long)lcltime) / SECSPERDAY - EPOCH_ADJUSTMENT_DAYS;
  rem = ((long)lcltime) % SECSPERDAY;
  if (rem < 0) {
    rem += SECSPERDAY;
    --days;
  }
  return (int)(rem / SECSPERHOUR);
}

#ifdef __CUDACC__
__device__
#endif
    int
    extract_minute(const time_t* tim_p) {
  long days, rem;
  const time_t lcltime = *tim_p;
  days = ((long)lcltime) / SECSPERDAY - EPOCH_ADJUSTMENT_DAYS;
  rem = ((long)lcltime) % SECSPERDAY;
  if (rem < 0) {
    rem += SECSPERDAY;
    --days;
  }
  rem %= SECSPERHOUR;
  return (int)(rem / SECSPERMIN);
}

#ifdef __CUDACC__
__device__
#endif
    int
    extract_second(const time_t* tim_p) {
  const time_t lcltime = *tim_p;
  return (int)((long)lcltime % SECSPERMIN);
}

#ifdef __CUDACC__
__device__
#endif
    int
    extract_dow(const time_t* tim_p) {
  long days, rem;
  int weekday;
  const time_t lcltime = *tim_p;
  days = ((long)lcltime) / SECSPERDAY - EPOCH_ADJUSTMENT_DAYS;
  rem = ((long)lcltime) % SECSPERDAY;
  if (rem < 0) {
    rem += SECSPERDAY;
    --days;
  }

  if ((weekday = ((ADJUSTED_EPOCH_WDAY + days) % DAYSPERWEEK)) < 0)
    weekday += DAYSPERWEEK;
  return weekday;
}

#ifdef __CUDACC__
__device__
#endif
    int
    extract_quarterday(const time_t* tim_p) {
  long quarterdays;
  const time_t lcltime = *tim_p;
  quarterdays = ((long)lcltime) / SECSPERQUARTERDAY;
  return (int)(quarterdays % 4) + 1;
}

#ifdef __CUDACC__
__device__
#endif
    tm*
    gmtime_r_newlib(const time_t* tim_p, tm* res) {
  const int month_lengths[2][MONSPERYEAR] = {{31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31},
                                             {31, 29, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31}};
  long days, rem;
  const time_t lcltime = *tim_p;
  int year, month, yearday, weekday;
  int years400, years100, years4, remainingyears;
  int yearleap;
  const int* ip;

  days = ((long)lcltime) / SECSPERDAY - EPOCH_ADJUSTMENT_DAYS;
  rem = ((long)lcltime) % SECSPERDAY;
  if (rem < 0) {
    rem += SECSPERDAY;
    --days;
  }

  /* compute hour, min, and sec */
  res->tm_hour = (int)(rem / SECSPERHOUR);
  rem %= SECSPERHOUR;
  res->tm_min = (int)(rem / SECSPERMIN);
  res->tm_sec = (int)(rem % SECSPERMIN);

  /* compute day of week */
  if ((weekday = ((ADJUSTED_EPOCH_WDAY + days) % DAYSPERWEEK)) < 0)
    weekday += DAYSPERWEEK;
  res->tm_wday = weekday;

  /* compute year & day of year */
  years400 = days / DAYS_PER_400_YEARS;
  days -= years400 * DAYS_PER_400_YEARS;
  /* simplify by making the values positive */
  if (days < 0) {
    days += DAYS_PER_400_YEARS;
    --years400;
  }

  years100 = days / DAYS_PER_100_YEARS;
  if (years100 == 4) /* required for proper day of year calculation */
    --years100;
  days -= years100 * DAYS_PER_100_YEARS;
  years4 = days / DAYS_PER_4_YEARS;
  days -= years4 * DAYS_PER_4_YEARS;
  remainingyears = days / DAYS_PER_YEAR;
  if (remainingyears == 4) /* required for proper day of year calculation */
    --remainingyears;
  days -= remainingyears * DAYS_PER_YEAR;

  year = ADJUSTED_EPOCH_YEAR + years400 * 400 + years100 * 100 + years4 * 4 + remainingyears;

  /* If remainingyears is zero, it means that the years were completely
   * "consumed" by modulo calculations by 400, 100 and 4, so the year is:
   * 1. a multiple of 4, but not a multiple of 100 or 400 - it's a leap year,
   * 2. a multiple of 4 and 100, but not a multiple of 400 - it's not a leap
   * year,
   * 3. a multiple of 4, 100 and 400 - it's a leap year.
   * If years4 is non-zero, it means that the year is not a multiple of 100 or
   * 400 (case 1), so it's a leap year. If years100 is zero (and years4 is zero
   * - due to short-circuiting), it means that the year is a multiple of 400
   * (case 3), so it's also a leap year. */
  yearleap = remainingyears == 0 && (years4 != 0 || years100 == 0);

  /* adjust back to 1st January */
  yearday = days + DAYS_IN_JANUARY + DAYS_IN_FEBRUARY + yearleap;
  if (yearday >= DAYS_PER_YEAR + yearleap) {
    yearday -= DAYS_PER_YEAR + yearleap;
    ++year;
  }
  res->tm_yday = yearday;
  res->tm_year = year - YEAR_BASE;

  /* Because "days" is the number of days since 1st March, the additional leap
   * day (29th of February) is the last possible day, so it doesn't matter much
   * whether the year is actually leap or not. */
  ip = month_lengths[1];
  month = 2;
  while (days >= ip[month]) {
    days -= ip[month];
    if (++month >= MONSPERYEAR)
      month = 0;
  }
  res->tm_mon = month;
  res->tm_mday = days + 1;

  res->tm_isdst = 0;

  return (res);
}

/*
 * @brief support the SQL EXTRACT function
 */
extern "C" __attribute__((noinline))
#ifdef __CUDACC__
__device__
#endif
    int64_t
    ExtractFromTime(ExtractField field, time_t timeval) {

  // We have fast paths for the 5 fields below - do not need to do full gmtime
  switch (field) {
    case kEPOCH:
      return timeval;
    case kQUARTERDAY:
      return extract_quarterday(&timeval);
    case kHOUR:
      return extract_hour(&timeval);
    case kMINUTE:
      return extract_minute(&timeval);
    case kSECOND:
      return extract_second(&timeval);
    case kDOW:
      return extract_dow(&timeval);
    case kISODOW: {
      int64_t dow = extract_dow(&timeval);
      return (dow == 0 ? 7 : dow);
    }
    default:
      break;
  }

  tm tm_struct;
  gmtime_r_newlib(&timeval, &tm_struct);
  switch (field) {
    case kYEAR:
      return 1900 + tm_struct.tm_year;
    case kQUARTER:
      return (tm_struct.tm_mon) / 3 + 1;
    case kMONTH:
      return tm_struct.tm_mon + 1;
    case kDAY:
      return tm_struct.tm_mday;
    case kDOY:
      return tm_struct.tm_yday + 1;
    case kWEEK: {
      int64_t doy = tm_struct.tm_yday;          // numbered from 0
      int64_t dow = extract_dow(&timeval) + 1;  // use Sunday 1 - Saturday 7
      int64_t week = (doy / 7) + 1;
      // now adjust for offset at start of year
      //      S M T W T F S
      // doy      0 1 2 3 4
      // doy  5 6
      // mod  5 6 0 1 2 3 4
      // dow  1 2 3 4 5 6 7
      // week 2 2 1 1 1 1 1
      if (dow > (doy % 7)) {
        return week;
      }
      return week + 1;
    }
    default:
#ifdef __CUDACC__
      return -1;
#else
      abort();
#endif
  }
}

extern "C"
#ifdef __CUDACC__
    __device__
#endif
        int64_t
        ExtractFromTimeNullable(ExtractField field, time_t timeval, const int64_t null_val) {
  if (timeval == null_val) {
    return null_val;
  }
  return ExtractFromTime(field, timeval);
}
