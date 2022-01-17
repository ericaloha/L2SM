// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include <sys/types.h>
#include <stdio.h>
#include <stdlib.h>
#include "db/db_impl.h"
#include "db/version_set.h"
#include "leveldb/cache.h"
#include "leveldb/db.h"
#include "leveldb/env.h"
#include "leveldb/write_batch.h"
#include "port/port.h"
#include "util/crc32c.h"
#include "util/histogram.h"
#include "util/mutexlock.h"
#include "util/random.h"
#include "util/testutil.h"
#include <cmath>
#include <vector>
#include <string>
#include <time.h>
#include <iostream>
#include <limits.h>
#include <fstream>
#include <ctime>

#include <unordered_map>
#include <thread>
//#include "generator.h"

int Read_Ratio = 0;
int update = 0;
extern int Stopflag;
float check_ratio = 0.911646;

// using namespace std;
extern int pureRead;
extern int Loadflag;
extern int get_start;
extern int HitLoad;
extern int MissLoad;
FILE *fp;
FILE *fp1;
FILE *check;
FILE *WarmUp;
FILE *debug;
extern int l0;
extern int l4;
extern int l44;
extern int l8;
extern int l12;
extern int get0;
extern int write0;
extern int picktir;
extern int score;
extern int seek;
extern int nothing;

extern int readtime;
extern int seekHit;
extern int seekHitInLSM;
extern int seekHitInLog;

extern int SeekMissLevel0;
extern int seekHitInLevel0;

extern int seektime;
extern int SeekMissFIlter;
extern int SeekMissLSM;
extern std::vector<leveldb::SSTBloom_Opt_2 *> STBs;
extern leveldb::SSTBloom_Opt_2 *Recorder;

extern int FilterSize;
extern int FilterSize_;
extern int FiltersLevel;

extern int seekInLog;

extern int FilterHit;
extern int FilterMiss;
extern int FilterMultiMiss;
extern int level0Hit;
extern int level0Miss;
extern int Level0MultiMiss;

extern int total_density_count[7];
extern float total_density[7];
// 2.avg hotness per level
extern int total_hotness_count[7];
extern long total_hotness[7];

extern int weakHit;
extern int NullFilter;
extern int weakMiss;

extern std::set<uint64_t> KeyLogNames;
extern int oldData;
struct cmp_key
{
  bool operator()(const uint64_t &k1, const uint64_t &k2) const
  {
    return k1 > k2;
  }
};
extern std::map<uint64_t, leveldb::FileMetaData *, cmp_key> SSTKeyLog[7];

extern int WarmUpFlag;

namespace generator
{

  /*
       Generating yscb-style sorted hashkey
       Usage:generating string for UniformGenerator(I guess)
  */
  static inline void shuffle(uint64_t a[], uint64_t n)
  {
    uint64_t index, tmp, i;
    srand(315);

    for (int i = n - 1; i > 0; i--)
    {
      index = rand() % i;
      tmp = a[i];
      a[i] = a[index];
      a[index] = tmp;
    }
  }
  static inline long long YCSBKey_hash(long long val)
  {
    long long FNV_offset_basis_64 = 0xCBF29CE484222325LL;
    long long FNV_prime_64 = 1099511628211LL;
    long long hashval = FNV_offset_basis_64;
    for (int i = 0; i < 8; i++)
    {
      long long octet = val & 0x00ff;
      val = val >> 8;
      hashval = hashval ^ octet;
      hashval = hashval * FNV_prime_64;
    }
    return llabs(hashval);
  }
  class YCSBKeyGenerator
  {
  private:
    long long *keypool;
    int index;
    int max;

  public:
    YCSBKeyGenerator(int startnum, int filenum, int keysize) : index(0)
    {
      keypool = new long long[keysize * filenum];
      for (long long i = 0; i < keysize * filenum; i++)
      {
        keypool[i] = YCSBKey_hash(i + startnum);
      }
      sort(keypool, 0, keysize * filenum);
    }
    ~YCSBKeyGenerator()
    {
      delete keypool;
    }
    void sort(long long *num, int top, int bottom)
    {
      int middle;
      if (top < bottom)
      {
        middle = partition(num, top, bottom);
        sort(num, top, middle);        // sort first section
        sort(num, middle + 1, bottom); // sort second section
      }
      return;
    }
    int partition(long long *array, int top, int bottom)
    {
      long long x = array[top];
      int i = top - 1;
      int j = bottom + 1;
      long long temp;
      do
      {
        do
        {
          j--;
        } while (x > array[j]);

        do
        {
          i++;
        } while (x < array[i]);

        if (i < j)
        {
          temp = array[i];
          array[i] = array[j];
          array[j] = temp;
        }
      } while (i < j);
      return j; // returns middle subscript
    }

    long long nextKey()
    {
      return keypool[index++];
    }
  };

  class Utils
  {
  public:
    /**
     * Hash an integer value.
     */
    static long hash(long val)
    {
      return FNVhash64(val);
    }

    static const int FNV_offset_basis_32 = 0x811c9dc5;
    static const int FNV_prime_32 = 16777619;

    /**
     * 32 bit FNV hash. Produces more "random" hashes than (say) std::string.hashCode().
     *
     * @param val The value to hash.
     * @return The hash value
     */
    static int FNVhash32(int val)
    {
      // from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
      int hashval = FNV_offset_basis_32;

      for (int i = 0; i < 4; i++)
      {
        int octet = val & 0x00ff;
        val = val >> 8;

        hashval = hashval ^ octet;
        hashval = hashval * FNV_prime_32;
        // hashval = hashval ^ octet;
      }
      return labs(hashval);
    }

    static const long FNV_offset_basis_64 = 0xCBF29CE484222325L;
    static const long FNV_prime_64 = 1099511628211L;

    /**
     * 64 bit FNV hash. Produces more "random" hashes than (say) std::string.hashCode().
     *
     * @param val The value to hash.
     * @return The hash value
     */
    static long FNVhash64(long val)
    {
      // from http://en.wikipedia.org/wiki/Fowler_Noll_Vo_hash
      long hashval = FNV_offset_basis_64;

      for (int i = 0; i < 8; i++)
      {
        long octet = val & 0x00ff;
        val = val >> 8;

        hashval = hashval ^ octet;
        hashval = hashval * FNV_prime_64;
        // hashval = hashval ^ octet;
      }
      return labs(hashval);
    }
  };

  class Generator
  {
    /**
     * Generate the next string in the distribution.
     */
  public:
    virtual std::string nextString() = 0;

    /**
     * Return the previous string generated by the distribution; e.g., returned from the last nextString() call.
     * Calling lastString() should not advance the distribution or have any side effects. If nextString() has not yet
     * been called, lastString() should return something reasonable.
     */
    virtual std::string lastString() = 0;
    virtual ~Generator(){};
  };

  /*
      2.2 IntegerGenerator
          do noothing but define methods of nextint(string) and lastint(string)
      */
  class IntegerGenerator : public Generator
  {
    int lastint;

    /**
     * Set the last value generated. IntegerGenerator subclasses must use this call
     * to properly set the last string value, or the lastString() and lastInt() calls won't work.
     */
  public:
    void setLastInt(int last)
    {
      lastint = last;
    }

    /**
     * Return the next value as an int. When overriding this method, be sure to call setLastString() properly, or the lastString() call won't work.
     */
    virtual int nextInt() = 0;

    /**
     * Generate the next string in the distribution.
     */
    std::string nextString()
    {
      char buf[100];
      sprintf(buf, "%d", nextInt()); //����buf��
      std::string s = buf;
      return s;
    }
    /**
     * Return the previous int generated by the distribution. This call is unique to IntegerGenerator subclasses, and assumes
     * IntegerGenerator subclasses always return ints for nextInt() (e.g. not arbitrary strings).
     */
    int lastInt()
    {
      return lastint;
    }
    /**
     * Return the previous string generated by the distribution; e.g., returned from the last nextString() call.
     * Calling lastString() should not advance the distribution or have any side effects. If nextString() has not yet
     * been called, lastString() should return something reasonable.
     */
    std::string lastString()
    {
      char buf[100];
      sprintf(buf, "%d", lastint);
      std::string s = buf;
      return s;
    }

    /**
     * Return the expected value (mean) of the values this generator will return.
     */
    virtual double mean() = 0;
    virtual ~IntegerGenerator(){};
  };
  /*
      2.3 CounterGenerator :used by other generators as a counter
          input:countstart
          output:nextInt() to generate a next int & lastInt() to generate a last int

      */
  class CounterGenerator : public IntegerGenerator
  {
    int counter;
    int max;
    /**
     * Create a counter that starts at countstart
     */
  public:
    CounterGenerator(int countstart, int countend = INT_MAX) : counter(countstart), max(countend) // countstart��ֵ��counter
    {
      counter = countstart > 0 ? countstart : 0;
      IntegerGenerator::setLastInt(counter - 1);
      max = countend > counter ? countend : INT_MAX;
    }

    /**
     * If the generator returns numeric (integer) values, return the next value as an int.
     * Default is to return -1, which
     * is appropriate for generators that do not return numeric values.
     */
    int nextInt()
    {
      int ret = (counter++) % max;
      IntegerGenerator::setLastInt(ret);
      // std::cout<<"nextint:"<<ret<<std::endl;
      return ret;
    }
    int lastInt()
    {
      return counter - 1;
    }
    double mean()
    {
      return 0;
      // throw new UnsupportedOperationException("Can't compute mean of non-stationary distribution!");
    }
  };

  /*
      3.Special generator: will be used as key_generating method
    */

  /*
      3.1 ZipfianGenerator
          input:  can be modified
                  default: _items The number of items in the distribution
          output： (1)nextLong() or nextInt()
                      @param itemcount The number of items in the distribution.
                      @return The next item in the sequence.

  */
  class ZipfianGenerator : public IntegerGenerator
  {
  public:
    static constexpr double ZIPFIAN_CONSTANT = 0.99;

  private:
    /**
     * Number of items.
     */
    long items;

    /**
     * Min item to generate.
     */
    long base;

    /**
     * The zipfian constant to use.
     */
    double zipfianconstant;

    /**
     * Computed parameters for generating the distribution.
     */
    double alpha, zetan, eta, theta, zeta2theta;

    /**
     * The number of items used to compute zetan the last time.
     */
    long countforzeta;

    /**
     * Flag to prevent problems. If you increase the number of items the zipfian generator is allowed to choose from, this code will incrementally compute a new zeta
     * value for the larger itemcount. However, if you decrease the number of items, the code computes zeta from scratch; this is expensive for large itemsets.
     * Usually this is not intentional; e.g. one thread thinks the number of items is 1001 and calls "nextLong()" with that item count; then another thread who thinks the
     * number of items is 1000 calls nextLong() with itemcount=1000 triggering the expensive recomputation. (It is expensive for 100 million items, not really for 1000 items.) Why
     * did the second thread think there were only 1000 items? maybe it read the item count before the first thread incremented it. So this flag allows you to say if you really do
     * want that recomputation. If true, then the code will recompute zeta if the itemcount goes down. If false, the code will assume itemcount only goes up, and never recompute.
     */
    bool allowitemcountdecrease;

    /******************************* Constructors **************************************/

    /**
     * Create a zipfian generator for the specified number of items.
     * @param _items The number of items in the distribution.
     */
  public:
    ZipfianGenerator(long _items)
    {
      new (this) ZipfianGenerator(0, _items - 1);
    }

    /**
     * Create a zipfian generator for items between min and max.
     * @param _min The smallest integer to generate in the sequence.
     * @param _max The largest integer to generate in the sequence.
     */
    ZipfianGenerator(long _min, long _max)
    {
      new (this) ZipfianGenerator(_min, _max, ZIPFIAN_CONSTANT);
    }

    /**
     * Create a zipfian generator for the specified number of items using the specified zipfian constant.
     *
     * @param _items The number of items in the distribution.
     * @param _zipfianconstant The zipfian constant to use.
     */
    ZipfianGenerator(long _items, double _zipfianconstant)
    {
      new (this) ZipfianGenerator(0, _items - 1, _zipfianconstant);
    }

    /**
     * Create a zipfian generator for items between min and max (inclusive) for the specified zipfian constant.
     * @param min The smallest integer to generate in the sequence.
     * @param max The largest integer to generate in the sequence.
     * @param _zipfianconstant The zipfian constant to use.
     */
    ZipfianGenerator(long min, long max, double _zipfianconstant)
    {
      new (this) ZipfianGenerator(min, max, _zipfianconstant, zetastatic(max - min + 1, _zipfianconstant));
    }
    /**
     * Create a zipfian generator for items between min and max (inclusive) for the specified zipfian constant, using the precomputed value of zeta.
     *
     * @param min The smallest integer to generate in the sequence.
     * @param max The largest integer to generate in the sequence.
     * @param _zipfianconstant The zipfian constant to use.
     * @param _zetan The precomputed zeta constant.
     */
    ZipfianGenerator(long min, long max, double _zipfianconstant, double _zetan)
    {
      // std::cout<<"terry is good"<<std::endl;
      allowitemcountdecrease = false;
      items = max - min + 1;
      base = min;
      zipfianconstant = _zipfianconstant;

      theta = zipfianconstant;

      zeta2theta = zeta(2, theta);

      alpha = 1.0 / (1.0 - theta);
      // zetan=zeta(items,theta);
      zetan = _zetan;
      countforzeta = items;
      eta = (1 - pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zetan);
      srand(time(NULL));
      nextInt();
      // System.out.println("XXXX 4 XXXX");
    }

    /**************************************************************************/

    /**
     * Compute the zeta constant needed for the distribution. Do this from scratch for a distribution with n items, using the
     * zipfian constant theta. Remember the value of n, so if we change the itemcount, we can recompute zeta.
     *
     * @param n The number of items to compute zeta over.
     * @param theta The zipfian constant.
     */
    double zeta(long n, double theta)
    {
      countforzeta = n;
      return zetastatic(n, theta);
    }

    /**
     * Compute the zeta constant needed for the distribution. Do this from scratch for a distribution with n items, using the
     * zipfian constant theta. This is a static version of the function which will not remember n.
     * @param n The number of items to compute zeta over.
     * @param theta The zipfian constant.
     */
    static double zetastatic(long n, double theta)
    {
      return zetastatic(0, n, theta, 0);
    }

    /**
     * Compute the zeta constant needed for the distribution. Do this incrementally for a distribution that
     * has n items now but used to have st items. Use the zipfian constant theta. Remember the new value of
     * n so that if we change the itemcount, we'll know to recompute zeta.
     *
     * @param st The number of items used to compute the last initialsum
     * @param n The number of items to compute zeta over.
     * @param theta The zipfian constant.
     * @param initialsum The value of zeta we are computing incrementally from.
     */
    double zeta(long st, long n, double theta, double initialsum)
    {
      countforzeta = n;
      return zetastatic(st, n, theta, initialsum);
    }

    /**
     * Compute the zeta constant needed for the distribution. Do this incrementally for a distribution that
     * has n items now but used to have st items. Use the zipfian constant theta. Remember the new value of
     * n so that if we change the itemcount, we'll know to recompute zeta.
     * @param st The number of items used to compute the last initialsum
     * @param n The number of items to compute zeta over.
     * @param theta The zipfian constant.
     * @param initialsum The value of zeta we are computing incrementally from.
     */
    static double zetastatic(long st, long n, double theta, double initialsum)
    {
      double sum = initialsum;
      // std::cout<<n<<std::endl;
      //  std::cout<<st<<" ";
      // std::cout<<n<<" ";
      // std::cout<<theta<<std::endl;
      for (long i = st; i < n; i++)
      {
        sum += 1 / (pow(i + 1, theta));
      }

      // std::cout<<"sum="<<sum<<std::endl;

      return sum;
    }

    /****************************************************************************************/
    /**
     * Generate the next item as a long.
     *
     * @param itemcount The number of items in the distribution.
     * @return The next item in the sequence.
     */
    long nextLong(long itemcount)
    {
      // from "Quickly Generating Billion-Record Synthetic Databases", Jim Gray et al, SIGMOD 1994
      //  std::cout<<"got here"<<std::endl;
      if (itemcount != countforzeta)
      {

        // have to recompute zetan and eta, since they depend on itemcount
        if (itemcount > countforzeta)
        {
          // System.err.println("WARNING: Incrementally recomputing Zipfian distribtion. (itemcount="+itemcount+" countforzeta="+countforzeta+")");

          // we have added more items. can compute zetan incrementally, which is cheaper
          zetan = zeta(countforzeta, itemcount, theta, zetan);
          eta = (1 - pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zetan);
        }
        else if ((itemcount < countforzeta) && (allowitemcountdecrease))
        {
          // have to start over with zetan
          // note : for large itemsets, this is very slow. so don't do it!

          // TODO: can also have a negative incremental computation, e.g. if you decrease the number of items, then just subtract
          // the zeta sequence terms for the items that went away. This would be faster than recomputing from scratch when the number of items
          // decreases

          // std::cout<<"WARNING: Recomputing Zipfian distribtion. This is slow and should be avoided. (itemcount="+itemcount+" countforzeta="+countforzeta+")";

          zetan = zeta(itemcount, theta);
          eta = (1 - pow(2.0 / items, 1 - theta)) / (1 - zeta2theta / zetan);
        }
      }

      double u = (double)(rand() / (double)RAND_MAX);
      // std::cout<<"random u="<<u<<std::endl;
      double uz = u * zetan;

      if (uz < 1.0)
      {
        return 0;
      }

      if (uz < 1.0 + pow(0.5, theta))
      {
        return 1;
      }

      long ret = base + (long)((itemcount)*pow(eta * u - eta + 1, alpha));
      setLastInt((int)ret);
      return ret;
    }
    /**
     * Generate the next item. this distribution will be skewed����б�� toward lower integers; e.g. 0 will
     * be the most popular, 1 the next most popular, etc.
     * @param itemcount The number of items in the distribution.
     * @return The next item in the sequence.
     */
    int nextInt(int itemcount)
    {
      return (int)nextLong(itemcount);
    }

    /**
     * Return the next value, skewed by the Zipfian distribution. The 0th item will be the most popular, followed by the 1st, followed
     * by the 2nd, etc. (Or, if min != 0, the min-th item is the most popular, the min+1th item the next most popular, etc.) If you want the
     * popular items scattered throughout the item space, use ScrambledZipfianGenerator instead.
     */
    int nextInt()
    {
      return (int)nextLong(items);
    }

    /**
     * Return the next value, skewed by the Zipfian distribution. The 0th item will be the most popular, followed by the 1st, followed
     * by the 2nd, etc. (Or, if min != 0, the min-th item is the most popular, the min+1th item the next most popular, etc.) If you want the
     * popular items scattered throughout the item space, use ScrambledZipfianGenerator instead.
     */
    long nextLong()
    {
      return nextLong(items);
    }

    /**
     * @todo Implement ZipfianGenerator.mean()
     */
    double mean()
    {
      return 0;
      // throw new UnsupportedOperationException("@todo implement ZipfianGenerator.mean()");
    }
  };

  /*
      3.2 SkewedLatestGenerator
          input: a counterGenerator as a counter
          output： (1)nextInt() : return a int

  */

  class SkewedLatestGenerator : public IntegerGenerator
  {
    CounterGenerator _basis;
    ZipfianGenerator *_zipfian;

  public:
    SkewedLatestGenerator(CounterGenerator basis) : _basis(basis)
    {
      /**
       * Create a zipfian generator for the specified number of items.
       * @param _items The number of items in the distribution.
       */
      _zipfian = new ZipfianGenerator(_basis.lastInt());

      nextInt();
    }

    /**
     * Generate the next string in the distribution,
     * skewed Zipfian favoring the items most recently returned by the basis generator.
     */
    int nextInt()
    {
      int max = _basis.lastInt();
      int nextint = max - _zipfian->nextInt(max);
      setLastInt(nextint);
      return nextint;
    }

    double mean()
    {
      return 0;
      // throw new UnsupportedOperationException("Can't compute mean of non-stationary distribution!");
    }
  };

  /*
      3.3 ScrambledZipfianGenerator
          input:  can  be modified
                  default: @param _items The number of items in the distribution.
          output： (1) nextLong() or nextInt()
                   (2) mean(): I will try it

  */
  class ScrambledZipfianGenerator : public IntegerGenerator
  {

    ZipfianGenerator *gen;
    long _min, _max, _itemcount;

  public:
    static constexpr double ZETAN = 26.46902820178302;
    static constexpr double USED_ZIPFIAN_CONSTANT = 0.99;
    static const long ITEM_COUNT = 10000000000L;

    /******************************* Constructors **************************************/

    /**
     * Create a zipfian generator for the specified number of items.
     * @param _items The number of items in the distribution.
     */
    ScrambledZipfianGenerator(long _items)
    {
      new (this) ScrambledZipfianGenerator(0, _items - 1);
    }

    /**
     * Create a zipfian generator for items between min and max.
     * @param _min The smallest integer to generate in the sequence.
     * @param _max The largest integer to generate in the sequence.
     */
    ScrambledZipfianGenerator(long _min, long _max)
    {
      new (this) ScrambledZipfianGenerator(_min, _max, ZipfianGenerator::ZIPFIAN_CONSTANT);
    }

    /**
     * Create a zipfian generator for the specified number of items using the specified zipfian constant.
     *
     * @param _items The number of items in the distribution.
     * @param _zipfianconstant The zipfian constant to use.
     */
    /*
  // not supported, as the value of zeta depends on the zipfian constant, and we have only precomputed zeta for one zipfian constant
    public ScrambledZipfianGenerator(long _items, double _zipfianconstant)
    {
      this(0,_items-1,_zipfianconstant);
    }
  */

    /**
     * Create a zipfian generator for items between min and max (inclusive) for the specified zipfian constant. If you
     * use a zipfian constant other than 0.99, this will take a long time to complete because we need to recompute zeta.
     * @param min The smallest integer to generate in the sequence.
     * @param max The largest integer to generate in the sequence.
     * @param _zipfianconstant The zipfian constant to use.
     */
    //���ҵ�
    ScrambledZipfianGenerator(long min, long max, double _zipfianconstant)
    {
      _min = min;
      _max = max;
      _itemcount = _max - _min + 1;
      if (_zipfianconstant == USED_ZIPFIAN_CONSTANT)
      {
        gen = new ZipfianGenerator(0, ITEM_COUNT, _zipfianconstant, ZETAN);
      }
      else
      {
        gen = new ZipfianGenerator(0, ITEM_COUNT, _zipfianconstant);
      }
    }

    /**************************************************************************************************/
    /**
     * Return the next long in the sequence.
     */
    long nextLong()
    {
      long ret = gen->nextLong();
      ret = _min + Utils::FNVhash64(ret) % _itemcount;
      setLastInt((int)ret);
      return ret;
    }
    /**
     * Return the next int in the sequence.
     */
    int nextInt()
    {
      return (int)nextLong();
    }

    /**
     * since the values are scrambled (hopefully uniformly), the mean is simply the middle of the range.
     */
    double mean()
    {
      return ((double)(((long)_min) + (long)_max)) / 2.0;
    }
  };

  /*
      3.4 UniformIntegerGenerator
          input:  lb and ub : the range of the generating key
          output： (1) nextInt()
                   (2) mean(): middle point of the range

  */

  class UniformIntegerGenerator : public IntegerGenerator
  {
    int _lb, _ub, _interval;

  public:
    /**
     * Creates a generator that will return integers uniformly randomly from the interval [lb,ub] inclusive (that is, lb and ub are possible values)
     *
     * @param lb the lower bound (inclusive) of generated values
     * @param ub the upper bound (inclusive) of generated values
     */
    UniformIntegerGenerator(int lb, int ub)
    {
      _lb = lb;
      _ub = ub;
      _interval = _ub - _lb + 1;
      srand((unsigned)time(NULL));
    }

    int nextInt()
    {
      double r = (double)(rand() / (double)RAND_MAX);
      int ret = r * _interval + _lb;
      IntegerGenerator::setLastInt(ret);
      return ret;
    }

    double mean()
    {
      return ((double)((long)(_lb + (long)_ub))) / 2.0;
    }
  };

  /*
      3.5 UniformGenerator  : return strings
          input:  values
          output： (1) nextString and laststring

  */

  class UniformGenerator : public Generator
  {

    std::vector<std::string> _values;
    std::string _laststring;
    UniformIntegerGenerator *_gen;

  public:
    /**
     * Creates a generator that will return strings from the specified set uniformly randomly
     */
    UniformGenerator(std::vector<std::string> values)
    {
      for (int i = 0; i < values.size(); i++)
      {
        _values.push_back(values.at(i));
      }
      //_values=(std::vector<std::string>)values.clone();
      _laststring.clear();
      _gen = new UniformIntegerGenerator(0, values.size() - 1);
    }

    /**
     * Generate the next string in the distribution.
     */
    std::string nextString()
    {
      _laststring = _values.at(_gen->nextInt());
      return _laststring;
    }

    /**
     * Return the previous string generated by the distribution; e.g., returned from the last nextString() call.
     * Calling lastString() should not advance the distribution or have any side effects. If nextString() has not yet
     * been called, lastString() should return something reasonable.
     */
    std::string lastString()
    {
      if (_laststring.empty())
      {
        nextString();
      }
      return _laststring;
    }
  };

} // namespace generator

/////////////////////////////////////////////////////////////////////////////////

// Comma-separated list of operations to run in the specified order
//   Actual benchmarks:
//      fillseq       -- write N values in sequential key order in async mode
//      fillrandom    -- write N values in random key order in async mode
//      overwrite     -- overwrite N values in random key order in async mode
//      fillsync      -- write N/100 values in random key order in sync mode
//      fill100K      -- write N/1000 100K values in random order in async mode

//      fill_Zipsk         -- fill_skewed
//      fill_ZipScr        --fill_Scramble
//      fill_Uniform       --fill_Uniform
//      fill_YCSBKey
//      readwhilezipK      --readwhilezipskewed
//      readwhilezipS      --readwhilezipScramble
//      readwhileUni      --readwhilezipuniform

//      deleteseq     -- delete N keys in sequential order
//      deleterandom  -- delete N keys in random order
//      readseq       -- read N times sequentially
//      readreverse   -- read N times in reverse order
//      readrandom    -- read N times in random order
//      readmissing   -- read N missing keys in random order
//      readhot       -- read N times in random order from 1% section of DB
//      seekrandom    -- N random seeks
//      open          -- cost of opening a DB
//      crc32c        -- repeated crc32c of 4K of data
//      acquireload   -- load N*1000 times
//   Meta operations:
//      compact     -- Compact the entire DB
//      stats       -- Print DB stats
//      sstables    -- Print sstable info
//      heapprofile -- Dump a heap profile (if supported by this port)
static const char *FLAGS_benchmarks =

    //"fill_YCSBKey,"       //newly created
    "fillseq,"
    "fillrandom,"
    "fill_Zipsk,"   // newly created
    "fill_ZipScr,"  // newly created
    "fill_Uniform," // newly created
    "readAndzipK,"  // newly created
    "readAndzipS,"  // newly created
    "readAndUni,"   // newly created
    "LoadAndRead,"
    "fillseqC,"
    "fillrandomC,"
    "fillsync,"
    "overwrite,"
    "readrandom,"
    "readrandom," // Extra run to allow previous compactions to quiesce ��Ĭ
    "readseq,"
    "readreverse,"
    "compact,"
    "readrandom,"
    "readseq,"
    "readreverse,"
    "fill100K,"
    "crc32c,"
    "snappycomp,"
    "snappyuncomp,"
    "acquireload,";

// Number of key/values to place in database
static int FLAGS_num = 1000000;

static int FLAG_OPS = 1000000;

static const char *FLAGS_workloads_distribution = "uniform";

// Number of read operations to do.  If negative, do FLAGS_num reads.
static int FLAGS_reads = -1;

// Number of concurrent threads to run.
static int FLAGS_threads = 1;

// Size of each value
static int FLAGS_value_size = 100;

// Arrange to generate values that shrink to this fraction of
// their original size after compression
static double FLAGS_compression_ratio = 0.5;

// Print histogram(ֱ��ͼ) of operation timings
static bool FLAGS_histogram = false;

// Number of bytes to buffer in memtable before compacting
// (initialized to default value by "main")
static int FLAGS_write_buffer_size = 0;

// Number of bytes written to each file.
// (initialized to default value by "main")
static int FLAGS_max_file_size = 0;

// Approximate size of user data packed per block (before compression.
// (initialized to default value by "main")
static int FLAGS_block_size = 0;

// Number of bytes to use as a cache of uncompressed data.
// Negative means use default settings.
static int FLAGS_cache_size = -1;

// Maximum number of files to keep open at the same time (use default if == 0)
static int FLAGS_open_files = 0;

// Bloom filter bits per key.
// Negative means use default settings.
static int FLAGS_bloom_bits = -1;

// If true, do not destroy the existing database.  If you set this
// flag and also specify a benchmark that wants a fresh database, that
// benchmark will fail.
static bool FLAGS_use_existing_db = false;

// If true, reuse existing log/MANIFEST files when re-opening a database.
static bool FLAGS_reuse_logs = false;

// Use the db with the following name.
static const char *FLAGS_db = NULL;

namespace leveldb
{

  namespace
  {
    leveldb::Env *g_env = NULL;

    // Helper for quickly generating random data（value, not key）.
    class RandomGenerator
    {
    private:
      std::string data_; //Ҫ������Ϊָ������
      int pos_;          //��ʼ��Ϊ0

    public:
      RandomGenerator()
      {
        // We use a limited amount of data over and over again and ensure
        // that it is larger than the compression window (32KB), and also
        // large enough to serve all typical value sizes we want to write.
        Random rnd(301);
        std::string piece;
        while (data_.size() < 1048576)
        {
          // Add a short fragment that is as compressible as specified
          // by FLAGS_compression_ratio(0.5).
          test::CompressibleString(&rnd, FLAGS_compression_ratio, 100, &piece);
          data_.append(piece);
        }
        pos_ = 0;
      }

      Slice Generate(size_t len)
      {
        if (pos_ + len > data_.size())
        {
          pos_ = 0;
          assert(len < data_.size()); //Ϊ������ֹ��posΪ����ʱΪ�棩
        }
        pos_ += len;
        return Slice(data_.data() + pos_ - len, len);
        // data_.data() + pos_ - lenΪ��ʼλ�ã�ָ�룬��ַ����lenΪƫ�ƣ�����slice
      }
    };

#if defined(__linux)
    static Slice TrimSpace(Slice s)
    {
      size_t start = 0;
      while (start < s.size() && isspace(s[start]))
      {
        start++;
      }
      size_t limit = s.size();
      while (limit > start && isspace(s[limit - 1]))
      {
        limit--;
      }
      return Slice(s.data() + start, limit - start);
    }
#endif

    static void AppendWithSpace(std::string *str, Slice msg)
    {
      if (msg.empty())
        return;
      if (!str->empty())
      {
        str->push_back(' ');
      }
      str->append(msg.data(), msg.size());
    }

    class Stats
    {
    private:
      double start_;
      double finish_;
      double seconds_;
      int done_;
      int next_report_;
      int64_t bytes_;
      double last_op_finish_;
      Histogram hist_;
      std::string message_;

    public:
      Stats() { Start(); }

      void Start()
      {
        next_report_ = 100;
        last_op_finish_ = start_;
        hist_.Clear();
        done_ = 0;
        bytes_ = 0;
        seconds_ = 0;
        start_ = g_env->NowMicros();
        finish_ = start_;
        message_.clear();
      }

      void Merge(const Stats &other)
      {
        hist_.Merge(other.hist_);
        done_ += other.done_;
        bytes_ += other.bytes_;
        seconds_ += other.seconds_;
        if (other.start_ < start_)
          start_ = other.start_;
        if (other.finish_ > finish_)
          finish_ = other.finish_;

        // Just keep the messages from one thread
        if (message_.empty())
          message_ = other.message_;
      }

      void Stop()
      {
        finish_ = g_env->NowMicros();
        seconds_ = (finish_ - start_) * 1e-6;
      }

      void AddMessage(Slice msg)
      {
        AppendWithSpace(&message_, msg);
      }

      void FinishedSingleOp()
      {
        if (FLAGS_histogram)
        {
          double now = g_env->NowMicros();
          double micros = now - last_op_finish_;
          hist_.Add(micros);
          if (micros > 20000)
          {
            fprintf(stderr, "long op: %.1f micros%30s\r", micros, "");
            fflush(stderr);
          }
          last_op_finish_ = now;
        }

        done_++;
        if (done_ >= next_report_)
        {
          if (next_report_ < 1000)
            next_report_ += 100;
          else if (next_report_ < 5000)
            next_report_ += 500;
          else if (next_report_ < 10000)
            next_report_ += 1000;
          else if (next_report_ < 50000)
            next_report_ += 5000;
          else if (next_report_ < 100000)
            next_report_ += 10000;
          else if (next_report_ < 500000)
            next_report_ += 50000;
          else
            next_report_ += 100000;
          fprintf(stderr, "... finished %d ops%30s\r", done_, "");
          fflush(stderr);
        }
      }

      void AddBytes(int64_t n)
      {
        bytes_ += n;
      }

      void Report(const Slice &name)
      {
        // Pretend at least one op was done in case we are running a benchmark
        // that does not call FinishedSingleOp().
        if (done_ < 1)
          done_ = 1;

        std::string extra;
        if (bytes_ > 0)
        {
          // Rate is computed on actual elapsed time, not the sum of per-thread
          // elapsed times.
          double elapsed = (finish_ - start_) * 1e-6;
          char rate[100];
          snprintf(rate, sizeof(rate), "%6.1f MB/s",
                   (bytes_ / 1048576.0) / elapsed);
          extra = rate;
        }
        AppendWithSpace(&extra, message_);

        fprintf(stdout, "%-12s : %11.3f micros/op;%s%s\n",
                name.ToString().c_str(),
                seconds_ * 1e6 / done_,
                (extra.empty() ? "" : " "),
                extra.c_str());
        if (FLAGS_histogram)
        {
          fprintf(stdout, "Microseconds per op:\n%s\n", hist_.ToString().c_str());
        }
        fflush(stdout);
      }
    };

    // State shared by all concurrent executions of the same benchmark.
    struct SharedState
    {
      port::Mutex mu;
      port::CondVar cv;
      int total;

      // Each thread goes through the following states:
      //    (1) initializing
      //    (2) waiting for others to be initialized
      //    (3) running
      //    (4) done

      int num_initialized;
      int num_done;
      bool start;

      SharedState() : cv(&mu) {}
    };

    // Per-thread state for concurrent executions of the same benchmark.
    struct ThreadState
    {
      int tid;     // 0..n-1 when running in n threads
      Random rand; // Has different seeds for different threads
      Stats stats;
      SharedState *shared;

      ThreadState(int index)
          : tid(index),
            rand(1000 + index)
      {
      }
    };

  } // namespace

  class Benchmark
  {
  private:
    Cache *cache_;
    const FilterPolicy *filter_policy_;
    DB *db_;
    int num_;
    int value_size_;
    int entries_per_batch_;
    WriteOptions write_options_;
    int reads_;
    int heap_counter_;

    // 输出对应的基本信息
    void PrintHeader()
    {
      const int kKeySize = 16;
      PrintEnvironment();
      fprintf(stdout, "Keys:        %d bytes each\n", kKeySize);
      fprintf(stdout, "Values:      %d bytes each (%d bytes after compression)\n",
              FLAGS_value_size,
              static_cast<int>(FLAGS_value_size * FLAGS_compression_ratio + 0.5));
      fprintf(stdout, "Entries:     %d\n", num_);
      fprintf(stdout, "RawSize:     %.1f MB (estimated)\n",
              ((static_cast<int64_t>(kKeySize + FLAGS_value_size) * num_) / 1048576.0));
      fprintf(stdout, "FileSize:    %.1f MB (estimated)\n",
              (((kKeySize + FLAGS_value_size * FLAGS_compression_ratio) * num_) / 1048576.0));
      PrintWarnings();
      fprintf(stdout, "------------------------------------------------\n");
    }

    void PrintWarnings()
    {

#if defined(__GNUC__) && !defined(__OPTIMIZE__)
      fprintf(stdout,
              "WARNING: Optimization is disabled: benchmarks unnecessarily slow\n");
#endif
#ifndef NDEBUG
      fprintf(stdout,
              "WARNING: Assertions are enabled; benchmarks unnecessarily slow\n");
#endif

      // See if snappy is working by attempting to compress a compressible string
      const char text[] = "yyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyyy";
      std::string compressed;
      if (!port::Snappy_Compress(text, sizeof(text), &compressed))
      {
        fprintf(stdout, "WARNING: Snappy compression is not enabled\n");
      }
      else if (compressed.size() >= sizeof(text))
      {
        fprintf(stdout, "WARNING: Snappy compression is not effective\n");
      }
    }

    void PrintEnvironment()
    {
      fprintf(stderr, "LevelDB:     version %d.%d\n",
              kMajorVersion, kMinorVersion);

#if defined(__linux)
      time_t now = time(NULL);
      fprintf(stderr, "Date:        %s", ctime(&now)); // ctime() adds newline

      FILE *cpuinfo = fopen("/proc/cpuinfo", "r");
      if (cpuinfo != NULL)
      {
        char line[1000];
        int num_cpus = 0;
        std::string cpu_type;
        std::string cache_size;
        while (fgets(line, sizeof(line), cpuinfo) != NULL)
        {
          const char *sep = strchr(line, ':');
          if (sep == NULL)
          {
            continue;
          }
          Slice key = TrimSpace(Slice(line, sep - 1 - line));
          Slice val = TrimSpace(Slice(sep + 1));
          if (key == "model name")
          {
            ++num_cpus;
            cpu_type = val.ToString();
          }
          else if (key == "cache size")
          {
            cache_size = val.ToString();
          }
        }
        fclose(cpuinfo);
        fprintf(stderr, "CPU:         %d * %s\n", num_cpus, cpu_type.c_str());
        fprintf(stderr, "CPUCache:    %s\n", cache_size.c_str());
      }
#endif
    }

  public:
    Benchmark()
        : cache_(FLAGS_cache_size >= 0 ? NewLRUCache(FLAGS_cache_size) : NULL),
          filter_policy_(FLAGS_bloom_bits >= 0
                             ? NewBloomFilterPolicy(FLAGS_bloom_bits)
                             : NULL),
          db_(NULL),
          num_(FLAGS_num),
          value_size_(FLAGS_value_size),
          entries_per_batch_(1),
          reads_(FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads),
          heap_counter_(0)
    {
      std::vector<std::string> files;
      g_env->GetChildren(FLAGS_db, &files);
      for (size_t i = 0; i < files.size(); i++)
      {
        if (Slice(files[i]).starts_with("heap-"))
        {
          g_env->DeleteFile(std::string(FLAGS_db) + "/" + files[i]);
        }
      }
      if (!FLAGS_use_existing_db)
      {
        DestroyDB(FLAGS_db, Options());
      }
    }

    ~Benchmark()
    {
      delete db_;
      delete cache_;
      delete filter_policy_;
    }

    void Run()
    {
      // 输出基础信息
      PrintHeader();
      // 打开数据库
      Open();

      // 获取对应的 benchmark 类型
      const char *benchmarks = FLAGS_benchmarks;
      // 可以执行多个 benchmark
      while (benchmarks != NULL)
      {
        // 按逗号分隔解析 benchmark
        const char *sep = strchr(benchmarks, ',');
        Slice name;
        if (sep == NULL)
        {
          name = benchmarks;
          benchmarks = NULL;
        }
        else
        {
          name = Slice(benchmarks, sep - benchmarks);
          benchmarks = sep + 1;
        }

        // 设置对应的 benchmarks 参数
        // Reset parameters that may be overridden below
        // 操作数
        num_ = FLAGS_num;
        // 读操作数，如果设置的小于 0，则为 FLAGS_num，否则为 FLAGS_reads
        reads_ = (FLAGS_reads < 0 ? FLAGS_num : FLAGS_reads);
        // value 大小
        value_size_ = FLAGS_value_size;
        // 每个 batch 的大小
        entries_per_batch_ = 1;
        write_options_ = WriteOptions();

        void (Benchmark::*method)(ThreadState *) = NULL;
        bool fresh_db = false;
        // 设置线程数
        int num_threads = FLAGS_threads;

        // 数据库 open 测试
        if (name == Slice("open"))
        {
          method = &Benchmark::OpenBench;
          num_ /= 10000;
          if (num_ < 1)
            num_ = 1;
        }

        else if (name == Slice("fillseq"))
        {
          fresh_db = true;
          method = &Benchmark::WriteSeq;
        }
        else if (name == Slice("fillseqC"))
        {
          // fresh_db = true;
          method = &Benchmark::WriteSeq;
        }
        else if (name == Slice("fillbatch"))
        {
          fresh_db = true;
          entries_per_batch_ = 1000;
          method = &Benchmark::WriteSeq;
        }
        else if (name == Slice("fillrandomC"))
        {
          // fresh_db = true;
          method = &Benchmark::WriteRandom;
        }
        else if (name == Slice("fillrandom"))
        {
          fresh_db = true;
          method = &Benchmark::WriteRandom;
        }

        else if (name == Slice("fill_Zipsk"))
        {
          // TODO 新添加的负载 fill_Zipsk
          // 对应的就是 Latest 写入
          // fresh_db = false;
          method = &Benchmark::Write_skewed;
        }
        else if (name == Slice("fill_ZipScr"))
        {

          // TODO 新添加的负载 fill_ZipScr
          // 对应的就是 zipfian 写入
          // fresh_db = false;
          method = &Benchmark::Write_scramble;
        }

        else if (name == Slice("fill_Uniform"))
        {
          // TODO 新添加的负载 fill_Uniform
          // 对应的就是 uniform 写入
          // fresh_db = false;
          method = &Benchmark::Write_Uniform;
        }
        /*
        else if (name == Slice("fill_YCSBKey")) {
              //fresh_db = false;
              method = &Benchmark::WriteYCSB;
        }
          */
        else if (name == Slice("readAndzipK"))
        {
          // TODO 新添加的负载 readAndzipK
          // 添加一个专门用于写操作的线程
          num_threads++; // Add extra thread for writing
          method = &Benchmark::ReadWhileWritingZipK;
        }
        else if (name == Slice("readAndzipS"))
        {
          // TODO 新添加的负载 readAndzipS
          // 添加一个专门用于写操作的线程
          num_threads++; // Add extra thread for writing
          method = &Benchmark::ReadWhileWritingZipS;
        }
        else if (name == Slice("readAndUni"))
        {
          // TODO 新添加的负载 readAndUni
          // 添加一个专门用于写操作的线程
          num_threads++; // Add extra thread for writing
          method = &Benchmark::ReadWhileWritingUni;
        }
        else if (name == Slice("LoadAndRead"))
        {
          // TODO 新添加的负载 LoadAndRead
          // num_threads++; // Add extra thread for writing
          method = &Benchmark::LoadThenRead;
        }
        else if (name == Slice("overwrite"))
        {
          fresh_db = false;
          method = &Benchmark::WriteRandom;
        }
        else if (name == Slice("fillsync"))
        {
          fresh_db = true;
          num_ /= 1000;
          write_options_.sync = true;
          method = &Benchmark::WriteRandom;
        }
        else if (name == Slice("fill100K"))
        {
          fresh_db = true;
          num_ /= 1000;
          value_size_ = 100 * 1000;
          method = &Benchmark::WriteRandom;
        }
        else if (name == Slice("readseq"))
        {
          method = &Benchmark::ReadSequential;
        }
        else if (name == Slice("readreverse"))
        {
          method = &Benchmark::ReadReverse;
        }
        else if (name == Slice("readrandom"))
        {
          method = &Benchmark::ReadRandom;
        }
        else if (name == Slice("readmissing"))
        {
          method = &Benchmark::ReadMissing;
        }
        else if (name == Slice("seekrandom"))
        {
          method = &Benchmark::SeekRandom;
        }
        else if (name == Slice("readhot"))
        {
          method = &Benchmark::ReadHot;
        }
        else if (name == Slice("readrandomsmall"))
        {
          reads_ /= 1000;
          method = &Benchmark::ReadRandom;
        }
        else if (name == Slice("deleteseq"))
        {
          method = &Benchmark::DeleteSeq;
        }
        else if (name == Slice("deleterandom"))
        {
          method = &Benchmark::DeleteRandom;
        }
        else if (name == Slice("readwhilewriting"))
        {
          // 正在写的时候读
          num_threads++; // Add extra thread for writing
          method = &Benchmark::ReadWhileWriting;
        }
        else if (name == Slice("compact"))
        {
          method = &Benchmark::Compact;
        }
        else if (name == Slice("crc32c"))
        {
          method = &Benchmark::Crc32c;
        }
        else if (name == Slice("acquireload"))
        {
          method = &Benchmark::AcquireLoad;
        }
        else if (name == Slice("snappycomp"))
        {
          method = &Benchmark::SnappyCompress;
        }
        else if (name == Slice("snappyuncomp"))
        {
          method = &Benchmark::SnappyUncompress;
        }
        else if (name == Slice("heapprofile"))
        {
          HeapProfile();
        }
        else if (name == Slice("stats"))
        {
          PrintStats("leveldb.stats");
        }
        else if (name == Slice("sstables"))
        {
          PrintStats("leveldb.sstables");
        }
        else
        {
          if (name != Slice())
          { // No error message for empty name
            fprintf(stderr, "unknown benchmark '%s'\n", name.ToString().c_str());
          }
        }

        if (fresh_db)
        {
          if (FLAGS_use_existing_db)
          {
            fprintf(stdout, "%-12s : skipped (--use_existing_db is true)\n",
                    name.ToString().c_str());
            method = NULL;
          }
          else
          {
            delete db_;
            db_ = NULL;
            DestroyDB(FLAGS_db, Options());
            Open();
          }
        }

        if (method != NULL)
        {
          // 传入对应的函数指针，执行对应的 benchmark
          RunBenchmark(num_threads, name, method);
        }
      }
    }

  private:
    struct ThreadArg
    {
      Benchmark *bm;
      SharedState *shared;
      ThreadState *thread;
      void (Benchmark::*method)(ThreadState *);
    };

    static void ThreadBody(void *v)
    {
      ThreadArg *arg = reinterpret_cast<ThreadArg *>(v);
      SharedState *shared = arg->shared;
      ThreadState *thread = arg->thread;
      {
        MutexLock l(&shared->mu);
        shared->num_initialized++;
        if (shared->num_initialized >= shared->total)
        {
          shared->cv.SignalAll();
        }
        while (!shared->start)
        {
          shared->cv.Wait();
        }
      }

      thread->stats.Start();
      (arg->bm->*(arg->method))(thread);
      thread->stats.Stop();

      {
        MutexLock l(&shared->mu);
        shared->num_done++;
        if (shared->num_done >= shared->total)
        {
          shared->cv.SignalAll();
        }
      }
    }

    void RunBenchmark(int n, Slice name,
                      void (Benchmark::*method)(ThreadState *))
    {
      SharedState shared;
      shared.total = n;
      shared.num_initialized = 0;
      shared.num_done = 0;
      shared.start = false;

      // 创建对应的线程数
      ThreadArg *arg = new ThreadArg[n];
      for (int i = 0; i < n; i++)
      {
        arg[i].bm = this;
        arg[i].method = method;
        arg[i].shared = &shared;
        arg[i].thread = new ThreadState(i);
        arg[i].thread->shared = &shared;
        g_env->StartThread(ThreadBody, &arg[i]);
      }

      shared.mu.Lock();
      while (shared.num_initialized < n)
      {
        shared.cv.Wait();
      }

      shared.start = true;
      shared.cv.SignalAll();
      while (shared.num_done < n)
      {
        shared.cv.Wait();
      }
      shared.mu.Unlock();

      // 合并对应的统计信息
      // 1 号线程以后的线程
      for (int i = 1; i < n; i++)
      {
        arg[0].thread->stats.Merge(arg[i].thread->stats);
      }
      arg[0].thread->stats.Report(name);

      for (int i = 0; i < n; i++)
      {
        delete arg[i].thread;
      }
      delete[] arg;
    }

    void Crc32c(ThreadState *thread)
    {
      // Checksum about 500MB of data total
      const int size = 4096;
      const char *label = "(4K per op)";
      std::string data(size, 'x');
      int64_t bytes = 0;
      uint32_t crc = 0;
      while (bytes < 500 * 1048576)
      {
        crc = crc32c::Value(data.data(), size);
        thread->stats.FinishedSingleOp();
        bytes += size;
      }
      // Print so result is not dead
      fprintf(stderr, "... crc=0x%x\r", static_cast<unsigned int>(crc));

      thread->stats.AddBytes(bytes);
      thread->stats.AddMessage(label);
    }

    void AcquireLoad(ThreadState *thread)
    {
      int dummy;
      port::AtomicPointer ap(&dummy);
      int count = 0;
      void *ptr = NULL;
      thread->stats.AddMessage("(each op is 1000 loads)");
      while (count < 100000)
      {
        for (int i = 0; i < 1000; i++)
        {
          ptr = ap.Acquire_Load();
        }
        count++;
        thread->stats.FinishedSingleOp();
      }
      if (ptr == NULL)
        exit(1); // Disable unused variable warning.
    }

    void SnappyCompress(ThreadState *thread)
    {
      RandomGenerator gen;
      Slice input = gen.Generate(Options().block_size);
      int64_t bytes = 0;
      int64_t produced = 0;
      bool ok = true;
      std::string compressed;
      while (ok && bytes < 1024 * 1048576)
      { // Compress 1G
        ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
        produced += compressed.size();
        bytes += input.size();
        thread->stats.FinishedSingleOp();
      }

      if (!ok)
      {
        thread->stats.AddMessage("(snappy failure)");
      }
      else
      {
        char buf[100];
        snprintf(buf, sizeof(buf), "(output: %.1f%%)",
                 (produced * 100.0) / bytes);
        thread->stats.AddMessage(buf);
        thread->stats.AddBytes(bytes);
      }
    }

    void SnappyUncompress(ThreadState *thread)
    {
      RandomGenerator gen;
      Slice input = gen.Generate(Options().block_size);
      std::string compressed;
      bool ok = port::Snappy_Compress(input.data(), input.size(), &compressed);
      int64_t bytes = 0;
      char *uncompressed = new char[input.size()];
      while (ok && bytes < 1024 * 1048576)
      { // Compress 1G
        ok = port::Snappy_Uncompress(compressed.data(), compressed.size(),
                                     uncompressed);
        bytes += input.size();
        thread->stats.FinishedSingleOp();
      }
      delete[] uncompressed;

      if (!ok)
      {
        thread->stats.AddMessage("(snappy failure)");
      }
      else
      {
        thread->stats.AddBytes(bytes);
      }
    }

    void Open()
    {
      assert(db_ == NULL);
      // 使用到的配置参数
      Options options;
      options.env = g_env;

      // FLAGS_use_existing_db
      options.create_if_missing = !FLAGS_use_existing_db;
      // block_cache, 单位 字节
      options.block_cache = cache_;
      // Memtable 大小，单位字节
      options.write_buffer_size = FLAGS_write_buffer_size;
      // 最大的文件大小
      options.max_file_size = FLAGS_max_file_size;
      // 块大小
      options.block_size = FLAGS_block_size;
      // 最多打开的文件数目
      options.max_open_files = FLAGS_open_files;
      // bloom 过滤器位数
      options.filter_policy = filter_policy_;
      // 是否重用日志，默认配置
      options.reuse_logs = FLAGS_reuse_logs;
      options.compression = leveldb::kNoCompression;
      Status s = DB::Open(options, FLAGS_db, &db_);
      if (!s.ok())
      {
        fprintf(stderr, "open error: %s\n", s.ToString().c_str());
        exit(1);
      }
    }

    void OpenBench(ThreadState *thread)
    {
      for (int i = 0; i < num_; i++)
      {
        delete db_;
        Open();
        thread->stats.FinishedSingleOp();
      }
    }

    void WriteSeq(ThreadState *thread)
    {
      DoWrite(thread, true);
    }
    void WriteRandom(ThreadState *thread)
    {
      DoWrite(thread, false);
    }
    /*
   void WriteYCSB(ThreadState* thread) {
      DoWrite_Y_U(thread, false);
    }
  */

    // LATEST 只是写操作 Load
    void Write_skewed(ThreadState *thread)
    {
      // Skewed 和 scrambled 的 load 都是调用的 zipfian
      // 参数一个为 true 一个为 false

      // true - skewed
      DoWrite_zipfian(thread, true);
    }

    // Zipfian 只是写操作 Load
    void Write_scramble(ThreadState *thread)
    {
      // zipfian 0.99 
      // false - scramble
      DoWrite_zipfian(thread, false);
    }

    // Uniform
    void Write_Uniform(ThreadState *thread)
    {
      // Uniform 就是随机加载
      DoWrite_Y_U(thread, true);
    }

    void DoWrite_zipfian(ThreadState *thread, bool seq)
    {
     
      if (num_ != FLAGS_num)
      { // Number of key/values to place in database
        char msg[100];
        snprintf(msg, sizeof(msg), "(%d ops)", num_);
        thread->stats.AddMessage(msg);
      }
      RandomGenerator gen;
      // generator::CounterGenerator cg(1);
      generator::SkewedLatestGenerator slg(10000000);     // can be modified by need 10 billion
      generator::ScrambledZipfianGenerator szg(10000000); // can be modified by need 10 billion
      WriteBatch batch;
      Status s;
      int64_t bytes = 0;

      // 总操作数，zipfian 写
      for (int i = 0; i < num_; i += entries_per_batch_)
      {
        batch.Clear();
        // batch 批量执行
        for (int j = 0; j < entries_per_batch_; j++)
        {
          // const int k = seq ? i+j : (thread->rand.Next() % FLAGS_num);  //seq 0(���д);1��˳��д
          // 如果 seq 为 true，对应即为 skewed 负载，使用 slg SkewedLatestGenerator
          // 如果为 false，对应即为 scramble 负载，使用 ScrambledZipfianGenerator。 
          const int k = seq ? slg.nextInt() % FLAGS_num : szg.nextInt() % FLAGS_num;

          char key[100];
          snprintf(key, sizeof(key), "%016d", k);
          batch.Put(key, gen.Generate(value_size_));
          bytes += value_size_ + strlen(key);
          thread->stats.FinishedSingleOp();
        }
        // clock_gettime(CLOCK_MONOTONIC, &tpstart);
        // 执行真正的写入
        s = db_->Write(write_options_, &batch);
        if (!s.ok())
        {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
        // clock_gettime(CLOCK_MONOTONIC, &tpend);
        // timedif = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + (tpend.tv_nsec - tpstart.tv_nsec) / 1000;
        // what we want : lantency as microseconds
        // dif = 1000000*(tpend.tv_sec-tpstart.tv_sec)+(tpend.tv_nsec-tpstart.tv_nsec)/1000;
        // write into LantencyLogfile by appending
        // openfile << timedif << " ";
      }
      thread->stats.AddBytes(bytes);
      // openfile.close();
    }
    void DoWrite_Y_U(ThreadState *thread, bool seq)
    {
      if (num_ != FLAGS_num)
      { // Number of key/values to place in database
        char msg[100];
        snprintf(msg, sizeof(msg), "(%d ops)", num_);
        thread->stats.AddMessage(msg);
      }
      RandomGenerator gen;
      // 随机加载
      generator::UniformIntegerGenerator uig(1, 99999999); // 1 billion
      // generator::YCSBKeyGenerator ycsb(1,100,5);//startnum, filenum, keysize can be modified by need

      WriteBatch batch;
      Status s;
      int64_t bytes = 0;
      for (int i = 0; i < num_; i += entries_per_batch_)
      { // entries_per_batch_ was defined as 1 by default!!!!!! you can change depend on your need (fillbatch operation has changed which to 1000)
        batch.Clear();
        for (int j = 0; j < entries_per_batch_; j++)
        {
          // seq - true - uniform 负载
          // false 其实也为 uniform 负载
          // const int k = seq ? (uig.nextInt()% FLAGS_num) : ((int)(ycsb.nextKey() % 1000000));
          const int k = seq ? (uig.nextInt() % FLAGS_num) : (uig.nextInt() % FLAGS_num);
          char key[100];
          snprintf(key, sizeof(key), "%016d", k);
          batch.Put(key, gen.Generate(value_size_));
          bytes += value_size_ + strlen(key);
          thread->stats.FinishedSingleOp();
        }
        // clock_gettime(CLOCK_MONOTONIC, &tpstart);

        // 执行真正的写操作
        s = db_->Write(write_options_, &batch);
        if (!s.ok())
        {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
        // clock_gettime(CLOCK_MONOTONIC, &tpend);
        // timedif = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + (tpend.tv_nsec - tpstart.tv_nsec) / 1000;
        // what we want : lantency as microseconds
        // dif = 1000000*(tpend.tv_sec-tpstart.tv_sec)+(tpend.tv_nsec-tpstart.tv_nsec)/1000;
        // write into LantencyLogfile by appending
        // openfile << timedif << " ";
      }
      thread->stats.AddBytes(bytes);
      // openfile.close();
    }

    void DoWrite(ThreadState *thread, bool seq)
    {
      // extern FILE *fp;
      /*struct timespec tpstart;
      struct timespec tpend;
      long timedif;
      std::ofstream openfile("/home/shunzi/lantency_standard.txt", std::ios::app);
  */
      if (num_ != FLAGS_num)
      { // Number of key/values to place in database
        char msg[100];
        snprintf(msg, sizeof(msg), "(%d ops)", num_);
        thread->stats.AddMessage(msg);
      }

      RandomGenerator gen;

      WriteBatch batch;
      Status s;
      int64_t bytes = 0;
      for (int i = 0; i < num_; i += entries_per_batch_)
      {
        batch.Clear();
        for (int j = 0; j < entries_per_batch_; j++)
        {
          const int k = seq ? i + j : (thread->rand.Next() % FLAGS_num);
          char key[100];
          snprintf(key, sizeof(key), "%016d", k);
          batch.Put(key, gen.Generate(value_size_));

          bytes += value_size_ + strlen(key);
          thread->stats.FinishedSingleOp();
        }
        // clock_gettime(CLOCK_MONOTONIC, &tpstart);
        s = db_->Write(write_options_, &batch);

        if (!s.ok())
        {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
        // clock_gettime(CLOCK_MONOTONIC, &tpend);
        // timedif = 1000000 * (tpend.tv_sec - tpstart.tv_sec) + (tpend.tv_nsec - tpstart.tv_nsec) / 1000;
        // what we want : lantency as microseconds
        // dif = 1000000*(tpend.tv_sec-tpstart.tv_sec)+(tpend.tv_nsec-tpstart.tv_nsec)/1000;
        // write into LantencyLogfile by appending
        // openfile << timedif << " ";
      }
      thread->stats.AddBytes(bytes);
      // openfile.close();
    }

    void ReadSequential(ThreadState *thread)
    {
      Iterator *iter = db_->NewIterator(ReadOptions());
      int i = 0;
      int64_t bytes = 0;
      for (iter->SeekToFirst(); i < reads_ && iter->Valid(); iter->Next())
      {
        bytes += iter->key().size() + iter->value().size();
        thread->stats.FinishedSingleOp();
        ++i;
      }
      delete iter;
      thread->stats.AddBytes(bytes);
    }

    void ReadReverse(ThreadState *thread)
    {
      Iterator *iter = db_->NewIterator(ReadOptions());
      int i = 0;
      int64_t bytes = 0;
      for (iter->SeekToLast(); i < reads_ && iter->Valid(); iter->Prev())
      {
        bytes += iter->key().size() + iter->value().size();
        thread->stats.FinishedSingleOp();
        ++i;
      }
      delete iter;
      thread->stats.AddBytes(bytes);
    }


    // LATEST 读
    void ReadRandom(ThreadState *thread)
    {
      ReadOptions options;
      generator::SkewedLatestGenerator slg(2000000);
      std::string value;
      int found = 0;
      // 根据指定的读次数
      for (int i = 0; i < reads_; i++)
      {
        char key[100];
        // const int k = thread->rand.Next() % FLAGS_num;
        const int k = slg.nextInt() % FLAGS_num;
        snprintf(key, sizeof(key), "%016d", k);
        if (db_->Get(options, key, &value).ok())
        {
          found++;
        }
        thread->stats.FinishedSingleOp();
      }
      // 延迟统计
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
      thread->stats.AddMessage(msg);
    }

    void ReadMissing(ThreadState *thread)
    {
      ReadOptions options;
      std::string value;
      for (int i = 0; i < reads_; i++)
      {
        char key[100];
        const int k = thread->rand.Next() % FLAGS_num;
        snprintf(key, sizeof(key), "%016d.", k);
        db_->Get(options, key, &value);
        thread->stats.FinishedSingleOp();
      }
    }

    void ReadHot(ThreadState *thread)
    {
      ReadOptions options;
      std::string value;
      const int range = (FLAGS_num + 99) / 100;
      for (int i = 0; i < reads_; i++)
      {
        char key[100];
        const int k = thread->rand.Next() % range;
        snprintf(key, sizeof(key), "%016d", k);
        db_->Get(options, key, &value);
        thread->stats.FinishedSingleOp();
      }
    }

    void SeekRandom(ThreadState *thread)
    {
      ReadOptions options;
      int found = 0;
      for (int i = 0; i < reads_; i++)
      {
        Iterator *iter = db_->NewIterator(options);
        char key[100];
        const int k = thread->rand.Next() % FLAGS_num;
        snprintf(key, sizeof(key), "%016d", k);
        iter->Seek(key);
        if (iter->Valid() && iter->key() == key)
          found++;
        delete iter;
        thread->stats.FinishedSingleOp();
      }
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
      thread->stats.AddMessage(msg);
    }

    void DoDelete(ThreadState *thread, bool seq)
    {
      RandomGenerator gen;
      WriteBatch batch;
      Status s;
      for (int i = 0; i < num_; i += entries_per_batch_)
      {
        batch.Clear();
        for (int j = 0; j < entries_per_batch_; j++)
        {
          const int k = seq ? i + j : (thread->rand.Next() % FLAGS_num);
          char key[100];
          snprintf(key, sizeof(key), "%016d", k);
          batch.Delete(key);
          thread->stats.FinishedSingleOp();
        }
        s = db_->Write(write_options_, &batch);
        if (!s.ok())
        {
          fprintf(stderr, "del error: %s\n", s.ToString().c_str());
          exit(1);
        }
      }
    }

    void DeleteSeq(ThreadState *thread)
    {
      DoDelete(thread, true);
    }

    void DeleteRandom(ThreadState *thread)
    {
      DoDelete(thread, false);
    }

    // 对应 Zipfian Skewed 负载
    void ReadWhileWritingZipK(ThreadState *thread)
    {
      // 使用 CounterGenerator start 200w
      generator::SkewedLatestGenerator slg(2000000); // can be modified by need
      RandomGenerator gen;

      int time1 = 0;

      // 先执行一半的写操作
      while (time1 < (FLAGS_num / 2))
      {
        // 写操作计数
        time1++;
        const int k = slg.nextInt() % FLAGS_num;
        char key[100];
        snprintf(key, sizeof(key), "%016d", k);
        // 写操作
        Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok())
        {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
      }

      // 线程号大于 0 则随机读
      if (thread->tid > 0)
      {
        // 执行指定数目的读操作
        ReadRandom(thread);
      }
      else
      {
        // 线程号等于 0 则执行如下逻辑

        // 在其他线程完成之前一直在写的特殊线程。
        // Special thread that keeps writing until other threads are done.

        // generator::SkewedLatestGenerator slg(2000000); //can be modified by need
        // RandomGenerator gen;
        while (true)
        {
          {
            MutexLock l(&thread->shared->mu);
            if (thread->shared->num_done + 1 >= thread->shared->num_initialized)
            {
              // Other threads have finished
              break;
            }
          }

          // const int k = thread->rand.Next() % FLAGS_num;
          // LATEST 写入
          const int k = slg.nextInt() % FLAGS_num;
          char key[100];
          snprintf(key, sizeof(key), "%016d", k);
          Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
          if (!s.ok())
          {
            fprintf(stderr, "put error: %s\n", s.ToString().c_str());
            exit(1);
          }
        }

        // Do not count any of the preceding work/delay in stats.
        thread->stats.Start();
      }
    }

    // 对应 Zipfian Scrambled 负载
    void ReadWhileWritingZipS(ThreadState *thread)
    {

      // 线程号 > 0 执行随机读
      if (thread->tid > 0)
      {
        ReadRandom(thread);
      }
      else
      {
        // 否则使用专门的线程
        // 在其他线程完成之前一直在写的特殊线程。
        // Special thread that keeps writing until other threads are done.

        generator::ScrambledZipfianGenerator szg(2000000); // can be modified by need
        RandomGenerator gen;
        while (true)
        {
          // 确保不是所有的线程都已经完成了
          // 终止条件就是其他线程全都完成了
          {
            MutexLock l(&thread->shared->mu);
            if (thread->shared->num_done + 1 >= thread->shared->num_initialized)
            {
              // Other threads have finished
              break;
            }
          }

          // const int k = thread->rand.Next() % FLAGS_num;

          const int k = szg.nextInt() % FLAGS_num;
          char key[100];
          snprintf(key, sizeof(key), "%016d", k);
          Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
          if (!s.ok())
          {
            fprintf(stderr, "put error: %s\n", s.ToString().c_str());
            exit(1);
          }
        }

        // Do not count any of the preceding work/delay in stats.
        thread->stats.Start();
      }
    }

    // 初始化对应的布隆过滤器
    bool InitFilters()
    {
      for (int i = 0; i < FiltersLevel; i++)
      {
        STBs.push_back(new SSTBloom_Opt_2(FilterSize, 0.001));
      }
      // Recorder = new SSTBloom_Opt_2(FilterSize,0.001); //L*
      return true;
    }


    int checkclose()
    {
      for (int i = 1; i < 5; i++)
      {
        if (STBs[i - 1]->getEle() > STBs[i - 1]->get_n() * 0.08 && STBs[i]->getEle() / (double)STBs[i - 1]->getEle() > std::pow(0.8, i))
        {

          fprintf(fp1, "Too close:%d, [%d,%d;%d,%d;%d,%d;%d,%d;%d,%d]  ",
                  i - 1,
                  STBs[0]->getEle(), STBs[0]->get_n(),
                  STBs[1]->getEle(), STBs[1]->get_n(),
                  STBs[2]->getEle(), STBs[2]->get_n(),
                  STBs[3]->getEle(), STBs[3]->get_n(),
                  STBs[4]->getEle(), STBs[4]->get_n());
          // update++;
          fprintf(fp1, "Req:%d  ", update);

          // fprintf(fp1,"%d\n",update);

          return i;
        }
      }
      return -1;
    }


    int IsFilterFull()
    {
      if (STBs[0]->getEle() > STBs[0]->get_n())
      {

        fprintf(fp1, "L1 full, [%d,%d;%d,%d;%d,%d;%d,%d;%d,%d]  ",
                STBs[0]->getEle(), STBs[0]->get_n(),
                STBs[1]->getEle(), STBs[1]->get_n(),
                STBs[2]->getEle(), STBs[2]->get_n(),
                STBs[3]->getEle(), STBs[3]->get_n(),
                STBs[4]->getEle(), STBs[4]->get_n());
        // update++;
        fprintf(fp1, "Req:%d  ", update);

        // fprintf(fp1,"%d\n",update);

        return 100;
      }
      else
      {
        return checkclose();
        // return -1;
      }

    }


    // 重置布隆过滤器
    bool ResetFilter(int flag)
    {
      if (flag == 100)
      { // L1 is full
        if (STBs[1]->getEle() < STBs[1]->get_n() * 0.05)
        {
          // do not enlarge
          FilterSize_ = STBs.back()->get_n();
          fprintf(fp1, " -->Rare repeat--> ");
        }
        else
        {
          FilterSize_ = STBs.back()->get_n() * 1.1;
          fprintf(fp1, " -->Enlarge--> ");
        }
        SSTBloom_Opt_2 *stb0 = STBs.front();
        delete stb0;
        // delete Recorder;
        // Recorder=new SSTBloom_Opt_2(FilterSize,0.001);
        STBs.erase(STBs.begin());
        // stb0->resize(FilterSize*15);
        STBs.push_back(new SSTBloom_Opt_2(FilterSize_, 0.001));
        fprintf(fp1, " ...OK\n");
        return true;
      }
      else
      { // enlarge
        if (flag == -1)
        {
          return false;
        }
        else
        {

          int evict_level = flag - 1;
          FilterSize_ = STBs.back()->get_n();
          delete STBs[evict_level];
          STBs.erase(STBs.begin() + evict_level);
          STBs.push_back(new SSTBloom_Opt_2(FilterSize_, 0.001));
          fprintf(fp1, " -->Too close-->...OK\n");

          return true;
        }
      }
    }

    // 更新布隆过滤器
    bool UpdateByLevel(std::string key, int level)
    {
      if (level == FiltersLevel)
      {
        return true;
      }
      else
      {
        if (STBs[level]->contains(key))
        {
          return UpdateByLevel(key, level + 1);
        }
        else
        {
          STBs[level]->insert(key);
          return false;
        }
      }
    }
    

    void UpdateFilter_Impl(std::string &key)
    {
      // if(Recorder->contains(key)==false){
      //   Recorder->insert(key);
      //}
      for (int i = 0; i < STBs.size(); i++)
      {
        if (STBs[i]->contains(key) == false)
        {
          STBs[i]->insert(key);
          break;
        }
      }
    }


    void UpdateFilter(std::string &usKey)
    {
      if (STBs.empty())
      {
        InitFilters();
      }
      int flag = IsFilterFull();
      if (flag != -1)
      {
        ResetFilter(flag);
      }
      UpdateFilter_Impl(usKey);
    }

    // Load then read
    void LoadThenRead(ThreadState *thread)
    {
      // 1.define the latency marker
      struct timespec tpstart_Write;
      struct timespec tpend_Write;
      long timedif_Write;

      struct timespec tpstart_Read;
      struct timespec tpend_Read;
      long timedif_Read;

      struct timespec tpstart_Warmup;
      struct timespec tpend_Warmup;
      long timedif_Warmup;

      // 设置对应的读比例
      float ReadRatio = (float)Read_Ratio / 10;
      fprintf(debug, "Read Ratio:%d \n", Read_Ratio);
      fprintf(debug, "Read ratio:%f \n", ReadRatio);
      long readTotal = 0;
      long writeTotal = 0;

      srand((int)time(0));                             // 产生随机种子  把0换成NULL也行

      // Latest 负载生成器 FLAGS_num - max
      generator::SkewedLatestGenerator slg(FLAGS_num); // can be modified by need
      // generator::ScrambledZipfianGenerator szgForread(2000000);

      // zipfina 负载生成器 0 - FLAGS_num
      generator::ScrambledZipfianGenerator szg(0, FLAGS_num);
      // generator::ScrambledZipfianGenerator szg(0,20000000);
      // generator::UniformIntegerGenerator uig(1,49999999);
      RandomGenerator gen;
      ReadOptions options;
      std::string value;
      int latcy_warm = 0;
      // 起始时间
      clock_gettime(CLOCK_MONOTONIC, &tpstart_Warmup);
      for (int time1 = 0; time1 < 0; time1++)
      {
        update = time1;
        char key[100];
        // const int k = thread->rand.Next() % FLAGS_num;
        // zipfian 负载
        const int k = szg.nextInt();
        // const int k = slg.nextInt();//
        // const int k=time1;
        // snprintf(key, sizeof(key), "%016d", k);
        std::string s = std::to_string(k);
        // 预热 filter
        UpdateFilter(s);
        // fprintf(fp,"%d ",k);
      }

      // 输出对应的 filter 初始化时间
      clock_gettime(CLOCK_MONOTONIC, &tpend_Warmup);
      timedif_Warmup = 1000000 * (tpend_Warmup.tv_sec - tpstart_Warmup.tv_sec) + (tpend_Warmup.tv_nsec - tpstart_Warmup.tv_nsec) / 1000;
      fprintf(debug, "Init Filter:%ld \n", timedif_Warmup);
      // 输出对应的 update 次数
      fprintf(debug, "update:%d \n", update);

      for (int time1 = 0; time1 < 0; time1++)
      {
        char key[100];
        // zipfian 负载
        const int k = szg.nextInt();
        // const int k =  thread->rand.Next()  % FLAGS_num;
        // const int k = thread->rand.Next() % FLAGS_num;
        snprintf(key, sizeof(key), "%016d", k);
        // UpdateFilter(key);
        clock_gettime(CLOCK_MONOTONIC, &tpstart_Warmup);
        Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok())
        {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
        clock_gettime(CLOCK_MONOTONIC, &tpend_Warmup);
        timedif_Warmup = 1000000 * (tpend_Warmup.tv_sec - tpstart_Warmup.tv_sec) + (tpend_Warmup.tv_nsec - tpstart_Warmup.tv_nsec) / 1000;
        // fprintf(WarmUp, "%ld ", timedif_Warmup);
        latcy_warm += timedif_Warmup;
        // if(time1>99000&& get_start==0){
        //     get_start=1;
        //}
      }

      for (int time1 = 0; time1 < 0; time1++)
      {
        char key[100];
        // 随机 Key
        const int k = thread->rand.Next() % FLAGS_num;
        snprintf(key, sizeof(key), "%016d", k);
        Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
        if (!s.ok())
        {
          fprintf(stderr, "put error: %s\n", s.ToString().c_str());
          exit(1);
        }
        if (Loadflag != 2)
        {
          Loadflag = 10;
        }
      }

      fprintf(debug, "Total Warm latcy:%d msc\n", latcy_warm);
      WarmUpFlag = 0;
      if (!STBs.empty())
      {
        fprintf(debug, "STB elements:[%d,%d,%d,%d,%d]\n", STBs[0]->getEle(),
                STBs[1]->getEle(), STBs[2]->getEle(), STBs[3]->getEle(),
                STBs[4]->getEle());
      }

      int found = 0;
      int r = 0;
      int w = 0;
      // long op_count = 50000000;
      long op_count = FLAG_OPS;
      const char* dist = FLAGS_workloads_distribution;
      Slice dist_slice = dist;


      clock_gettime(CLOCK_MONOTONIC, &tpstart_Warmup);
      // 5000w 次操作
      for (int i = 0; i < op_count; i++)
      {
        int k_int = 0;
        if (dist_slice == "latest") {
          k_int = slg.nextInt();
        } else if (dist_slice == "zipf") {
          k_int = szg.nextInt();
        } else {
          k_int = thread->rand.Next() % op_count;
        }

        // 执行读操作
        if (rand() % 100 > 0 && rand() % 100 < 100 * ReadRatio)
        {
          r++;
          char key[100];
          const int k = k_int;
          snprintf(key, sizeof(key), "%016d", k);
          clock_gettime(CLOCK_MONOTONIC, &tpstart_Read);
          if (db_->Get(options, key, &value).ok())
          {
            found++;
          }
          clock_gettime(CLOCK_MONOTONIC, &tpend_Read);
          timedif_Read = 1000000 * (tpend_Read.tv_sec - tpstart_Read.tv_sec) + (tpend_Read.tv_nsec - tpstart_Read.tv_nsec) / 1000;
          fprintf(check, "%ld ", timedif_Read);
          // 读操作总延迟
          readTotal += timedif_Read;
        }
        else
        {
          // 执行写操作
          w++;
          const int k = k_int;
          char key[100];
          snprintf(key, sizeof(key), "%016d", k);

          clock_gettime(CLOCK_MONOTONIC, &tpstart_Write);
          Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
          if (!s.ok())
          {
            fprintf(stderr, "put error: %s\n", s.ToString().c_str());
            exit(1);
          }
          clock_gettime(CLOCK_MONOTONIC, &tpend_Write);
          timedif_Write = 1000000 * (tpend_Write.tv_sec - tpstart_Write.tv_sec) + (tpend_Write.tv_nsec - tpstart_Write.tv_nsec) / 1000;
          fprintf(fp, "%ld ", timedif_Write);
          // 写操作总延迟
          writeTotal += timedif_Write;
        }
      }
      Stopflag = 1;
      clock_gettime(CLOCK_MONOTONIC, &tpend_Warmup);
      timedif_Warmup = 1000000 * (tpend_Warmup.tv_sec - tpstart_Warmup.tv_sec) + (tpend_Warmup.tv_nsec - tpstart_Warmup.tv_nsec) / 1000;
      fprintf(debug, "Total Latency:%ld\n", timedif_Warmup);
      fprintf(debug, "Read:%ld, Latcy: %f msc/op\n", readTotal, readTotal / (float)r);
      fprintf(debug, "Read Thrpt:%f\n", (r * 120) / (float)readTotal);

      fprintf(debug, "Write:%ld, Latcy:%f msc/op \n", writeTotal, writeTotal / (float)w);
      fprintf(debug, "Write Throughput: %f MB/msc \n", (w * 220) / (float)writeTotal);
      fprintf(debug, "********************************************************************\n");
      fprintf(debug, "Read ops: %lf kops/s\n", (r * 1000) / (double)readTotal);
      fprintf(debug, "Write ops: %lf kops/s\n", (w * 1000) / (double)writeTotal);
      fprintf(debug, "Avg ops: %lf kops/s", (1000 * (r+w)) / ((double)readTotal + double(writeTotal)));
      fprintf(debug, "\n********************************************************************\n");
      char msg[100];
      snprintf(msg, sizeof(msg), "(%d of %d found)", found, num_);
      thread->stats.AddMessage(msg);
    }

    // Uniform 负载
    void ReadWhileWritingUni(ThreadState *thread)
    {

      // 读
      if (thread->tid > 0)
      {
        ReadRandom(thread);
      }
      else
      {
        // 专门的写线程 范围 1，1999999 < 200w
        // Special thread that keeps writing until other threads are done.

        generator::UniformIntegerGenerator uig(1, 1999999); // can be modified by need
        RandomGenerator gen;
        while (true)
        {
          {
            MutexLock l(&thread->shared->mu);
            if (thread->shared->num_done + 1 >= thread->shared->num_initialized)
            {
              // Other threads have finished
              break;
            }
          }

          // const int k = thread->rand.Next() % FLAGS_num;

          const int k = uig.nextInt() % FLAGS_num;
          char key[100];
          snprintf(key, sizeof(key), "%016d", k);
          Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
          if (!s.ok())
          {
            fprintf(stderr, "put error: %s\n", s.ToString().c_str());
            exit(1);
          }
        }

        // Do not count any of the preceding work/delay in stats.
        thread->stats.Start();
      }
    }

    // 正在写的时候读
    void ReadWhileWriting(ThreadState *thread)
    {
      // 读操作
      if (thread->tid > 0)
      {
        ReadRandom(thread);
      }
      else
      {
        // 专门的写线程，没有范围
        // Special thread that keeps writing until other threads are done.
        RandomGenerator gen;
        while (true)
        {
          {
            MutexLock l(&thread->shared->mu);
            if (thread->shared->num_done + 1 >= thread->shared->num_initialized)
            {
              // Other threads have finished
              break;
            }
          }

          // 直接随机生成
          const int k = thread->rand.Next() % FLAGS_num;
          char key[100];
          snprintf(key, sizeof(key), "%016d", k);
          Status s = db_->Put(write_options_, key, gen.Generate(value_size_));
          if (!s.ok())
          {
            fprintf(stderr, "put error: %s\n", s.ToString().c_str());
            exit(1);
          }
        }

        // Do not count any of the preceding work/delay in stats.
        thread->stats.Start();
      }
    }

    void Compact(ThreadState *thread)
    {
      db_->CompactRange(NULL, NULL);
    }

    void PrintStats(const char *key)
    {
      std::string stats;
      if (!db_->GetProperty(key, &stats))
      {
        stats = "(failed)";
      }
      fprintf(stdout, "\n%s\n", stats.c_str());
    }

    static void WriteToFile(void *arg, const char *buf, int n)
    {
      reinterpret_cast<WritableFile *>(arg)->Append(Slice(buf, n));
    }

    void HeapProfile()
    {
      char fname[100];
      snprintf(fname, sizeof(fname), "%s/heap-%04d", FLAGS_db, ++heap_counter_);
      WritableFile *file;
      Status s = g_env->NewWritableFile(fname, &file);
      if (!s.ok())
      {
        fprintf(stderr, "%s\n", s.ToString().c_str());
        return;
      }
      bool ok = port::GetHeapProfile(WriteToFile, file);
      delete file;
      if (!ok)
      {
        fprintf(stderr, "heap profiling not supported\n");
        g_env->DeleteFile(fname);
      }
    }
  };

} // namespace leveldb

int main(int argc, char **argv)
{
  // 对应的几个文件，作者使用的文件记录
  debug = fopen("/home/shunzi/debug.txt", "a");
  check = fopen("/home/shunzi/check.txt", "a");
  WarmUp = fopen("/home/shunzi/WarmUp.txt", "a");
  fp = fopen("/home/shunzi/latency.txt", "a");
  fp1 = fopen("/home/shunzi/filter.txt", "w");

  // 设置一些最基本的参数
  FLAGS_write_buffer_size = leveldb::Options().write_buffer_size;
  FLAGS_max_file_size = leveldb::Options().max_file_size;
  FLAGS_block_size = leveldb::Options().block_size;
  FLAGS_open_files = leveldb::Options().max_open_files;
  std::string default_db_path;

  // 开启一个后台线程，对应 BackGroundLogManager，也就是原文中的每一层的 LogArea 的管理
  std::thread BackGround;

  BackGround = std::thread(leveldb::BackGroundLogManager);
  // BackGround.join();
  BackGround.detach();

  // 参数解析
  for (int i = 1; i < argc; i++)
  {
    double d;
    int n;
    char junk;
    if (leveldb::Slice(argv[i]).starts_with("--benchmarks="))
    {
      // 原有的 benchmark 的基础上增加了
      // fill_Zipsk  Skewed
      // fill_ZipScr Scrambled
      // fill_Uniform Random
      // readAndzipK Skewed
      // readAndzipS Scrambled
      // readAndUni Random
      // 对应原文的几种负载 Skewed/Scrambled/Random
      FLAGS_benchmarks = argv[i] + strlen("--benchmarks=");
    }
    else if (sscanf(argv[i], "--compression_ratio=%lf%c", &d, &junk) == 1)
    {
      // 压缩率
      FLAGS_compression_ratio = d;
    }
    else if (sscanf(argv[i], "--histogram=%d%c", &n, &junk) == 1 &&
             (n == 0 || n == 1))
    {
      // 累积分布
      FLAGS_histogram = n;
    }
    else if (sscanf(argv[i], "--use_existing_db=%d%c", &n, &junk) == 1 &&
             (n == 0 || n == 1))
    {
      // 是否使用已有的数据库
      FLAGS_use_existing_db = n;
    }
    else if (sscanf(argv[i], "--reuse_logs=%d%c", &n, &junk) == 1 &&
             (n == 0 || n == 1))
    {
      // 日志重用 MANIFEST
      FLAGS_reuse_logs = n;
    }
    else if (sscanf(argv[i], "--num=%d%c", &n, &junk) == 1)
    {
      // 记录数
      FLAGS_num = n;
    }
    else if (sscanf(argv[i], "--ops=%d%c", &n, &junk) == 1)
    {
      // 操作数
      FLAG_OPS = n;
    }
    else if (strncmp(argv[i], "--dist=", 7) == 0)
    {
      // 负载分布
      FLAGS_workloads_distribution = argv[i] + 7;;
    }
    else if (sscanf(argv[i], "--reads=%d%c", &n, &junk) == 1)
    {
      // 读操作数
      FLAGS_reads = n;
    }
    else if (sscanf(argv[i], "--threads=%d%c", &n, &junk) == 1)
    {
      // 线程数
      FLAGS_threads = n;
    }
    else if (sscanf(argv[i], "--value_size=%d%c", &n, &junk) == 1)
    {
      // value size
      FLAGS_value_size = n;
    }
    else if (sscanf(argv[i], "--read_ratio=%d%c", &n, &junk) == 1)
    {
      // 读数据 比例，只会在 LoadThenRead 负载中使用
      Read_Ratio = n;
    }
    else if (sscanf(argv[i], "--write_buffer_size=%d%c", &n, &junk) == 1)
    {
      // 写 buffer 大小
      FLAGS_write_buffer_size = n;
    }
    else if (sscanf(argv[i], "--max_file_size=%d%c", &n, &junk) == 1)
    {
      // sst 大小
      FLAGS_max_file_size = n;
    }
    else if (sscanf(argv[i], "--block_size=%d%c", &n, &junk) == 1)
    {
      // block 大小
      FLAGS_block_size = n;
    }
    else if (sscanf(argv[i], "--cache_size=%d%c", &n, &junk) == 1)
    {
      // 块缓存大小
      FLAGS_cache_size = n;
    }
    else if (sscanf(argv[i], "--bloom_bits=%d%c", &n, &junk) == 1)
    {
      // bloom 位数
      FLAGS_bloom_bits = n;
    }
    else if (sscanf(argv[i], "--open_files=%d%c", &n, &junk) == 1)
    {
      // 最多打开的文件数
      FLAGS_open_files = n;
    }
    else if (strncmp(argv[i], "--db=", 5) == 0)
    {
      // DB 路径
      FLAGS_db = argv[i] + 5;
    }
    else
    {
      fprintf(stderr, "Invalid flag '%s'\n", argv[i]);
      exit(1);
    }
  }

  leveldb::g_env = leveldb::Env::Default();

  // 设置默认的 dbpath
  // Choose a location for the test database if none given with --db=<path>
  if (FLAGS_db == NULL)
  {
    // 默认 DB 路径
    leveldb::g_env->GetTestDirectory(&default_db_path);
    default_db_path += "/dbbench";
    FLAGS_db = default_db_path.c_str();
    fprintf(stdout, "db path: %s\n", FLAGS_db);
  }

  // 创建 Benchmark 并运行
  leveldb::Benchmark benchmark;
  benchmark.Run();

  // debug 文件中保存对应的时间统计信息
  fprintf(debug, "MakeRoomForWrite:%d\n", l0);
  fprintf(debug, "L0 Test Time(4):%d\n", l4);
  fprintf(debug, "L0 Compact Time(4):%d\n", l44);
  fprintf(debug, "L0 Sleep Time(8):%d\n", l8);
  fprintf(debug, "L0 Stop Time(12):%d\n", l12);
  fprintf(debug, "Total Write:%d\n", write0);
  fprintf(debug, "Total Get:%d\n", get0);
  fprintf(debug, "Total Disk Get:%d,Average seek time:%f\n", readtime, (double)seektime / readtime);
  fprintf(debug, "Pick happens:%d,Score:%d,Seek:%d,Nothing:%d\n", picktir, score, seek, nothing);
  fprintf(debug, " Total Disk Access:%d,Cache Hit Ratio:%f\n", seektime, (double)seekHit / seektime);

  // 输出每层对应的热度信息和密度信息
  for (int i = 0; i < 7; i++)
  {
    if (total_density_count[i] > 0)
    {
      fprintf(debug, "Avg density in level-%d:%f\n", i, total_density[i] / total_density_count[i]);
    }
    if (total_hotness_count[i] > 0)
    {
      fprintf(debug, "Avg Hotness in level-%d:%ld\n", i, total_hotness[i] / total_hotness_count[i]);
    }
  }

  // 输出对应的 KeyLog 大小，即包含的 Log 数量
  fprintf(debug, "KeyLog Size:%ld\n", KeyLogNames.size());
  // 输出 KeyLog 中对应的文件号
  for (std::set<uint64_t>::iterator it = KeyLogNames.begin(); it != KeyLogNames.end(); it++)
  {
    fprintf(debug, "%ld In Log\n", *it);
  }

  // 输出对应的 SSTKeyLog 和热度信息
  fprintf(debug, "Old Data:%d\n", oldData);
  for (int i = 0; i < 7; i++)
  {
    // 输出每一层的 SST Key Log 信息
    if (!SSTKeyLog[i].empty())
    {
      for (std::map<uint64_t, leveldb::FileMetaData *, cmp_key>::iterator it = SSTKeyLog[i].begin();
           it != SSTKeyLog[i].end(); it++)
      {
        // 输出热度信息到 debug file
        fprintf(debug, "Log[%d]:%ld,Hotness:%d \n", i, (*it).second->number, (*it).second->Hotness);
      }
    }
  }

  // 输出命中情况到 debug file
  fprintf(debug, "Level0  Key:[Hit:%d,Miss:%d],Filter:[Hit:%d,Miss:%d,Multi Miss:%d]\n",
          seekHitInLevel0, SeekMissLevel0,
          level0Hit, level0Miss, Level0MultiMiss);

  fprintf(debug, "Seek Hit In:[Log:%d,LSM:%d]\nSeek Miss in [Log:%d,LSM:%d]\n,Filter [Hit:%d,Miss:%d,MultiMiss:%d]\n,Weak [Hit:%d,Miss:%d]\nNullFilter :%d\n", seekHitInLog, seekHitInLSM,
          SeekMissFIlter, SeekMissLSM,
          FilterHit, FilterMiss, FilterMultiMiss,
          weakHit, weakMiss, NullFilter);

  fprintf(debug, "HitLoad:%d, MissLoad:%d\n", HitLoad, MissLoad);
  fclose(fp);
  fclose(fp1);
  fclose(WarmUp);
  fclose(debug);
  fclose(check);
  // BackGround.~thread();
  system("kill -s 9 `ps -aux | grep db_bench | awk '{print $2}'`");
  return 0;
}
