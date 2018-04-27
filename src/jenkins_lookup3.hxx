/*
 * jenkins_lookup3.hxx
 *
 *  Created on: 27 avr. 2018
 *      Author: benoit.gschwind
 */

#ifndef SRC_JENKINS_LOOKUP3_HXX_
#define SRC_JENKINS_LOOKUP3_HXX_

#include <cstdint>
#include <cassert>

namespace h5ng {

/*
-------------------------------------------------------------------------------
H5_lookup3_mix -- mix 3 32-bit values reversibly.

This is reversible, so any information in (a,b,c) before mix() is
still in (a,b,c) after mix().

If four pairs of (a,b,c) inputs are run through mix(), or through
mix() in reverse, there are at least 32 bits of the output that
are sometimes the same for one pair and different for another pair.
This was tested for:
* pairs that differed by one bit, by two bits, in any combination
  of top bits of (a,b,c), or in any combination of bottom bits of
  (a,b,c).
* "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
  the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
  is commonly produced by subtraction) look like a single 1-bit
  difference.
* the base values were pseudorandom, all zero but one bit set, or
  all zero plus a counter that starts at zero.

Some k values for my "a-=c; a^=rot(c,k); c+=b;" arrangement that
satisfy this are
    4  6  8 16 19  4
    9 15  3 18 27 15
   14  9  3  7 17  3
Well, "9 15 3 18 27 15" didn't quite get 32 bits diffing
for "differ" defined as + with a one-bit base and a two-bit delta.  I
used http://burtleburtle.net/bob/hash/avalanche.html to choose
the operations, constants, and arrangements of the variables.

This does not achieve avalanche.  There are input bits of (a,b,c)
that fail to affect some output bits of (a,b,c), especially of a.  The
most thoroughly mixed value is c, but it doesn't really even achieve
avalanche in c.

This allows some parallelism.  Read-after-writes are good at doubling
the number of bits affected, so the goal of mixing pulls in the opposite
direction as the goal of parallelism.  I did what I could.  Rotates
seem to cost as much as shifts on every machine I could lay my hands
on, and rotates are much kinder to the top and bottom bits, so I used
rotates.
-------------------------------------------------------------------------------
*/

template<uint32_t const k>
static inline uint32_t lookup3_rot(uint32_t const & x) { return (((x)<<(k)) ^ ((x)>>(32-(k)))); }

static inline void lookup3_mix(uint32_t & a,uint32_t & b,uint32_t & c)
{
  a -= c;  a ^= lookup3_rot< 4>(c);  c += b;
  b -= a;  b ^= lookup3_rot< 6>(a);  a += c;
  c -= b;  c ^= lookup3_rot< 8>(b);  b += a;
  a -= c;  a ^= lookup3_rot<16>(c);  c += b;
  b -= a;  b ^= lookup3_rot<19>(a);  a += c;
  c -= b;  c ^= lookup3_rot< 4>(b);  b += a;
}

/*
-------------------------------------------------------------------------------
lookup3_final -- final mixing of 3 32-bit values (a,b,c) into c

Pairs of (a,b,c) values differing in only a few bits will usually
produce values of c that look totally different.  This was tested for
* pairs that differed by one bit, by two bits, in any combination
  of top bits of (a,b,c), or in any combination of bottom bits of
  (a,b,c).
* "differ" is defined as +, -, ^, or ~^.  For + and -, I transformed
  the output delta to a Gray code (a^(a>>1)) so a string of 1's (as
  is commonly produced by subtraction) look like a single 1-bit
  difference.
* the base values were pseudorandom, all zero but one bit set, or
  all zero plus a counter that starts at zero.

These constants passed:
 14 11 25 16 4 14 24
 12 14 25 16 4 14 24
and these came close:
  4  8 15 26 3 22 24
 10  8 15 26 3 22 24
 11  8 15 26 3 22 24
-------------------------------------------------------------------------------
*/
void lookup3_final(uint32_t & a,uint32_t & b, uint32_t & c)
{
  c ^= b; c -= lookup3_rot<14>(b);
  a ^= c; a -= lookup3_rot<11>(c);
  b ^= a; b -= lookup3_rot<25>(a);
  c ^= b; c -= lookup3_rot<16>(b);
  a ^= c; a -= lookup3_rot< 4>(c);
  b ^= a; b -= lookup3_rot<14>(a);
  c ^= b; c -= lookup3_rot<24>(b);
}


/*
 * By Bob Jenkins, 2006.  bob_jenkins@burtleburtle.net.  You may use this
 * code any way you wish, private, educational, or commercial.  It's free.
 * Source : HDF5 library
 */
uint32_t jenkins_lookup3(uint8_t const * data, uint64_t length, uint32_t initval = 0) noexcept
{
    uint32_t a, b, c;           /* internal state */

    /* Set up the internal state */
    a = b = c = 0xdeadbeef + static_cast<uint32_t>(length) + initval;

    /*--------------- all but the last block: affect some 32 bits of (a,b,c) */
    while (length > 12)
    {
      a += data[0];
      a += static_cast<uint32_t>(data[1])<<8;
      a += static_cast<uint32_t>(data[2])<<16;
      a += static_cast<uint32_t>(data[3])<<24;
      b += data[4];
      b += static_cast<uint32_t>(data[5])<<8;
      b += static_cast<uint32_t>(data[6])<<16;
      b += static_cast<uint32_t>(data[7])<<24;
      c += data[8];
      c += static_cast<uint32_t>(data[9])<<8;
      c += static_cast<uint32_t>(data[10])<<16;
      c += static_cast<uint32_t>(data[11])<<24;
      lookup3_mix(a, b, c);
      length -= 12;
      data += 12;
    }

    /*-------------------------------- last block: affect all 32 bits of (c) */
    switch(length)                   /* all the case statements fall through */
    {
        case 12: c+=static_cast<uint32_t>(data[11])<<24;// @suppress("No break at end of case")
        case 11: c+=static_cast<uint32_t>(data[10])<<16;// @suppress("No break at end of case")
        case 10: c+=static_cast<uint32_t>(data[9])<<8;  // @suppress("No break at end of case")
        case 9 : c+=data[8];                            // @suppress("No break at end of case")
        case 8 : b+=static_cast<uint32_t>(data[7])<<24; // @suppress("No break at end of case")
        case 7 : b+=static_cast<uint32_t>(data[6])<<16; // @suppress("No break at end of case")
        case 6 : b+=static_cast<uint32_t>(data[5])<<8;  // @suppress("No break at end of case")
        case 5 : b+=data[4];                            // @suppress("No break at end of case")
        case 4 : a+=static_cast<uint32_t>(data[3])<<24; // @suppress("No break at end of case")
        case 3 : a+=static_cast<uint32_t>(data[2])<<16; // @suppress("No break at end of case")
        case 2 : a+=static_cast<uint32_t>(data[1])<<8;  // @suppress("No break at end of case")
        case 1 : a+=data[0];
                 break;
        case 0 : return c;
        default:
            assert(false);
    }

    lookup3_final(a, b, c);
    return c;
}

}



#endif /* SRC_JENKINS_LOOKUP3_HXX_ */
