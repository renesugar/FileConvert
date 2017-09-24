/*
Copyright (c) 2005-2017, NumPy Developers.
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:

    * Redistributions of source code must retain the above copyright
       notice, this list of conditions and the following disclaimer.

    * Redistributions in binary form must reproduce the above
       copyright notice, this list of conditions and the following
       disclaimer in the documentation and/or other materials provided
       with the distribution.

    * Neither the name of the NumPy Developers nor the names of any
       contributors may be used to endorse or promote products derived
       from this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
*/

#include "HalfFloat.h"

uint32_t halfbits_to_floatbits(uint16_t h) {
  uint16_t h_exp, h_sig;
  uint32_t f_sgn, f_exp, f_sig;

  h_exp = (h&0x7c00u);
  f_sgn = ((uint32_t)h&0x8000u) << 16;
  switch (h_exp) {
    case 0x0000u: /* 0 or subnormal */
      h_sig = (h&0x03ffu);
      /* Signed zero */
      if (h_sig == 0) {
          return f_sgn;
      }
      /* Subnormal */
      h_sig <<= 1;
      while ((h_sig&0x0400u) == 0) {
          h_sig <<= 1;
          h_exp++;
      }
      f_exp = ((uint32_t)(127 - 15 - h_exp)) << 23;
      f_sig = ((uint32_t)(h_sig&0x03ffu)) << 13;
      return f_sgn + f_exp + f_sig;
    case 0x7c00u: /* inf or NaN */
      /* All-ones exponent and a copy of the significand */
      return f_sgn + 0x7f800000u + (((uint32_t)(h&0x03ffu)) << 13);
    default: /* normalized */
      /* Just need to adjust the exponent and shift */
      return f_sgn + (((uint32_t)(h&0x7fffu) + 0x1c000u) << 13);
  }
}

